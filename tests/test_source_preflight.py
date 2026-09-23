"""Version gates reject unknown migration state without touching output or leaking input."""

from importlib.metadata import version
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConfigurationError, OperationFailure, ServerSelectionTimeoutError

from nmdc_lakehouse import source_preflight as preflight
from nmdc_lakehouse import target_validation
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.jobs import collection_to_parquet as collection_module
from nmdc_lakehouse.jobs.collection_to_parquet import CollectionToParquetJob
from nmdc_lakehouse.jobs.direct_mongo_to_parquet import DirectMongoToParquetJob


@pytest.fixture
def mongo(monkeypatch):
    """Supply only the existing migration view, without a network connection."""
    client = MagicMock()
    client.__enter__.return_value = client
    view = client.get_default_database.return_value.__getitem__.return_value
    view.find.return_value.limit.return_value = [{"schema_version": version("nmdc-schema")}]
    factory = MagicMock(return_value=client)
    monkeypatch.setattr(preflight, "MongoClient", factory)
    return factory, client, view


def test_matching_source_reads_only_the_version_view_and_closes(mongo):
    factory, client, view = mongo
    assert preflight.assert_mongodb_source_aligned("mongodb://localhost/nmdc") == version("nmdc-schema")
    factory.assert_called_once()
    client.get_default_database.assert_called_once_with(default="nmdc")
    client.get_default_database.return_value.__getitem__.assert_called_once_with(preflight.MIGRATION_VERSION_VIEW)
    view.find.assert_called_once_with({}, {"_id": 0, "schema_version": 1})
    view.find.return_value.limit.assert_called_once_with(2)
    client.__exit__.assert_called_once()
    client.get_default_database.return_value.create_collection.assert_not_called()


@pytest.mark.parametrize(
    ("uri", "expected_database"),
    [
        ("mongodb://localhost:27017", "nmdc"),
        ("mongodb://localhost:27017/", "nmdc"),
        ("mongodb://localhost:27017/?authSource=admin", "nmdc"),
        ("mongodb://localhost:27017/explicit_db?authSource=admin", "explicit_db"),
    ],
)
def test_preflight_honors_uri_database_and_preserves_pathless_fallback(monkeypatch, uri, expected_database):
    """Exercise PyMongo's actual URI selection without connecting or reading a server."""
    client = MongoClient(uri, connect=False)
    monkeypatch.setattr(preflight, "MongoClient", lambda *args, **kwargs: client)
    databases_read = []

    def find_version(collection, filter, projection):
        databases_read.append(collection.database.name)
        assert collection.name == preflight.MIGRATION_VERSION_VIEW
        assert filter == {}
        assert projection == {"_id": 0, "schema_version": 1}
        cursor = MagicMock()
        cursor.limit.return_value = [{"schema_version": version("nmdc-schema")}]
        return cursor

    monkeypatch.setattr(Collection, "find", find_version)
    assert preflight.assert_mongodb_source_aligned(uri) == version("nmdc-schema")
    assert databases_read == [expected_database]


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [{}],
        [{"schema_version": None}],
        [{"schema_version": []}],
        [{"schema_version": "11.23.0"}, {"schema_version": "11.24.0"}],
    ],
)
def test_unknown_or_incomplete_migration_is_rejected(mongo, rows):
    mongo[2].find.return_value.limit.return_value = rows
    with pytest.raises(preflight.SourceSchemaError, match="no unambiguous completed schema migration"):
        preflight.assert_mongodb_source_aligned("mongodb://localhost/nmdc")


@pytest.mark.parametrize(
    "observed",
    ["", "0.0.0", "private-source-content", "11.23.0" if version("nmdc-schema") == "11.24.0" else "11.24.0"],
)
def test_mismatch_does_not_echo_source_values(mongo, observed):
    mongo[2].find.return_value.limit.return_value = [{"schema_version": observed}]
    with pytest.raises(preflight.SourceSchemaError, match="does not match installed") as error:
        preflight.assert_mongodb_source_aligned("mongodb://localhost/nmdc")
    assert "private-source-content" not in str(error.value)


@pytest.mark.parametrize("error_type", [ConfigurationError, OperationFailure, ServerSelectionTimeoutError])
def test_connection_errors_are_sanitized_and_closed(mongo, error_type):
    mongo[2].find.side_effect = error_type("private-connection-details")
    with pytest.raises(preflight.SourceSchemaError, match="Cannot read MongoDB migration state") as error:
        preflight.assert_mongodb_source_aligned("mongodb://localhost/nmdc")
    assert "private-connection-details" not in str(error.value)
    assert error.value.__suppress_context__
    mongo[1].__exit__.assert_called_once()


def test_requested_profile_drift_fails_before_connecting(monkeypatch, mongo):
    monkeypatch.setenv("NMDC_SCHEMA_VERSION", "0.0.0")
    with pytest.raises(target_validation.TargetValidationError, match="differs from the installed"):
        preflight.assert_mongodb_source_aligned("mongodb://localhost/nmdc")
    mongo[0].assert_not_called()


@pytest.mark.parametrize("installed,requested", [("11.24.0", "11.23.0"), ("11.23.0", "11.24.0")])
def test_requested_supported_source_drift_fails_before_connecting(monkeypatch, mongo, installed, requested):
    monkeypatch.setenv("NMDC_SCHEMA_VERSION", requested)
    monkeypatch.setattr(target_validation, "version", lambda _package: installed)
    with pytest.raises(target_validation.TargetValidationError, match="differs from the installed"):
        preflight.assert_mongodb_source_aligned("mongodb://localhost/nmdc")
    mongo[0].assert_not_called()


@pytest.mark.parametrize("matches", [False, True])
def test_preflight_cli_reports_only_sanitized_version_status(monkeypatch, mongo, matches):
    settings = MagicMock(uri="mongodb://localhost/nmdc")
    monkeypatch.setattr("nmdc_lakehouse.config.MongoSettings", lambda: settings)
    if not matches:
        mongo[2].find.return_value.limit.return_value = [{"schema_version": "private-value"}]
    result = CliRunner().invoke(cli, ["source-preflight"])
    assert result.exit_code == (0 if matches else 1)
    assert "private-value" not in result.output
    assert "mongodb://" not in result.output
    assert ("match nmdc-schema" if matches else "does not match installed") in result.output


@pytest.mark.parametrize("job_class", [CollectionToParquetJob, DirectMongoToParquetJob])
def test_both_jobs_check_source_before_creating_output(tmp_path, monkeypatch, job_class):
    gate = MagicMock(side_effect=preflight.SourceSchemaError("source mismatch"))
    monkeypatch.setattr(preflight, "assert_mongodb_source_aligned", gate)
    monkeypatch.setattr(collection_module, "assert_mongodb_source_aligned", gate)
    output = tmp_path / "not-created"
    job = job_class("study_set", "Study", "mongodb://localhost/nmdc", output)
    with pytest.raises(preflight.SourceSchemaError, match="source mismatch"):
        job.run()
    assert not output.exists()
    gate.assert_called_once()


@pytest.mark.parametrize("job_class", [CollectionToParquetJob, DirectMongoToParquetJob])
@pytest.mark.parametrize("dry_run", [False, True])
def test_source_change_during_read_refuses_promotion(tmp_path, monkeypatch, job_class, dry_run):
    record = {"id": "example:study", "type": "nmdc:Study", "study_category": "research_study"}
    source = MagicMock()
    source.iter_records.return_value = iter([record])
    source.estimated_count.return_value = 1
    monkeypatch.setattr(collection_module, "MongoSource", lambda _uri: source)
    client = MagicMock()
    collection = client.__getitem__.return_value.__getitem__.return_value
    collection.estimated_document_count.return_value = 1
    collection.find.return_value.batch_size.return_value = iter([record])
    monkeypatch.setattr("nmdc_lakehouse.jobs.direct_mongo_to_parquet.pymongo.MongoClient", lambda _uri: client)
    gate = MagicMock(side_effect=[version("nmdc-schema"), preflight.SourceSchemaError("source changed")])
    monkeypatch.setattr(preflight, "assert_mongodb_source_aligned", gate)
    monkeypatch.setattr(collection_module, "assert_mongodb_source_aligned", gate)
    prior = tmp_path / "study_set.parquet"
    prior.write_bytes(b"previous output must survive")
    job = job_class("study_set", "Study", "mongodb://localhost/nmdc", tmp_path)
    with pytest.raises(preflight.SourceSchemaError, match="source changed"):
        job.run(dry_run=dry_run)
    assert gate.call_count == 2
    assert prior.read_bytes() == b"previous output must survive"
    assert not (tmp_path / ".staging").exists()
