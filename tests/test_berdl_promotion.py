"""Exercise combined promotion, metadata writes, evidence binding and partial failure."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from nmdc_lakehouse import berdl_metadata, berdl_staging
from nmdc_lakehouse import berdl_promotion as promotion
from nmdc_lakehouse import publication_staging as staging
from nmdc_lakehouse.cli import cli
from nmdc_lakehouse.publication_prepare import file_digest, save_json
from tests.test_berdl_staging import REVISION
from tests.test_publication_planning import prepared as prepared_fixture
from tests.test_publication_staging import planned as planned_fixture

planned = planned_fixture
prepared = prepared_fixture
PARENT = "sha256:" + "a" * 64
DERIVED = "sha256:" + "b" * 64
CANONICAL = "nmdc.metadata"


def state(rows=2, snapshot_id="101"):
    return promotion.CatalogTable(
        rows=rows,
        snapshot_id=snapshot_id,
        physical_schema=[("id", "string"), ("optional", "string")],
        table_description="Reviewed table description",
        columns={"id": "Stable identifier", "optional": None},
        properties={"nmdc_lakehouse.snapshot_id": PARENT, "nmdc_lakehouse.target_schema_version": "test"},
    )


class FakeFrame:
    def __init__(self, spark, value, source):
        self.source = source
        self.spark, self.value = spark, value.model_copy(deep=True)
        self.schema = SimpleNamespace(
            fields=[
                SimpleNamespace(
                    name=n,
                    metadata={"preserved": "value", "comment": "old"},
                    dataType=SimpleNamespace(simpleString=lambda name=n: spark.column_types.get(name, "string")),
                )
                for n in value.columns
            ]
        )

    def limit(self, n):
        assert n == 0
        self.value.rows = 0
        self.spark.reads.append((self.source, None))
        return self

    def __getitem__(self, name):
        return SimpleNamespace(alias=lambda alias, metadata: (alias, metadata))

    def select(self, *columns):
        assert all(metadata["preserved"] == "value" for _, metadata in columns)
        self.value.columns = {name: metadata.get("comment") for name, metadata in columns}
        self.spark.projections.append(columns)
        return self

    def writeTo(self, target):
        self.target = target
        self.value.properties = {}
        self.value.table_description = None
        return self

    def using(self, kind):
        assert kind == "iceberg"
        return self

    def tableProperty(self, key, value):
        if key == "comment":
            self.value.table_description = value or None
        else:
            self.value.properties[key] = value
        return self

    def create(self):
        assert self.target not in self.spark.tables
        self._write("add")

    def replace(self):
        assert self.target in self.spark.tables
        self._write("replace")

    def _write(self, action):
        self.spark.writes.append((action, self.target))
        if self.spark.failure:
            raise self.spark.failure
        self.value.snapshot_id = str(200 + len(self.spark.writes))
        self.spark.tables[self.target] = self.value.model_copy(deep=True)
        if self.spark.after_write:
            self.spark.after_write(self.target)


class FakeReader:
    def __init__(self, spark):
        self.spark = spark

    def format(self, kind):
        assert kind == "iceberg"
        return self

    def option(self, key, value):
        assert key == "snapshot-id"
        self.snapshot_id = value
        return self

    def load(self, source):
        value = self.spark.tables[source]
        assert value.snapshot_id == self.snapshot_id
        self.spark.reads.append((source, self.snapshot_id))
        return FakeFrame(self.spark, value, source)


class FakeSpark:
    def __init__(self, tables):
        self.tables = tables
        self.column_types = {
            table.removeprefix("biosample_set_"): "array<string>" for table in promotion.OBSOLETE_TEXTVALUE_TABLES
        }
        self.writes, self.reads, self.projections, self.queries = [], [], [], []
        self.failure = self.after_write = None
        self.read = FakeReader(self)
        self.catalog = SimpleNamespace(
            getTable=lambda name: SimpleNamespace(description=self.tables[name].table_description),
            listColumns=lambda name: [
                SimpleNamespace(name=n, description=v, dataType=self.column_types.get(n, "string"))
                for n, v in self.tables[name].columns.items()
            ],
        )

    def table(self, name):
        return FakeFrame(self, self.tables[name], name)

    def sql(self, query):
        self.queries.append(query)
        if query.startswith("SHOW TABLES IN "):
            namespace = query.removeprefix("SHOW TABLES IN ")
            rows = [
                {"tableName": n.rsplit(".", 1)[1], "isTemporary": False}
                for n in self.tables
                if n.rsplit(".", 1)[0] == namespace
            ]
        elif query.startswith("SELECT COUNT(*) FROM "):
            rows = [(self.tables[query.split()[-1]].rows,)]
        elif query.startswith("SELECT snapshot_id FROM "):
            assert query.endswith(".refs WHERE name = 'main'")
            name = query.split()[3].removesuffix(".refs")
            value = self.tables[name].snapshot_id
            rows = [] if value is None else [(value,)]
        elif query.startswith("SHOW TBLPROPERTIES "):
            rows = list(self.tables[query.split()[-1]].properties.items())
        elif query.startswith("DROP TABLE "):
            name = query.split()[-1]
            self.writes.append(("drop", name))
            del self.tables[name]
            rows = []
        else:
            pytest.fail(f"Unexpected SQL: {query}")
        return SimpleNamespace(collect=lambda: rows)


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    roots = [tmp_path / "metadata", tmp_path / "derived"]
    for root in roots:
        (root / "snapshot").mkdir(parents=True)
        (root / "evidence").mkdir()
    fields = [
        pa.field(table.removeprefix("biosample_set_"), pa.list_(pa.string()))
        for table in sorted(promotion.OBSOLETE_TEXTVALUE_TABLES)
    ]
    pq.write_table(pa.Table.from_pylist([], schema=pa.schema(fields)), roots[0] / "snapshot/biosample_set.parquet")
    sources = [
        promotion.PromotionSource(
            root=str(root),
            scope="full-mongodb-metadata-snapshot" if index == 0 else "derived-provenance-snapshot",
            snapshot_id=PARENT if index == 0 else DERIVED,
            parent_snapshot_id=None if index == 0 else PARENT,
            staging_namespace="nmdc.nmdc_metadata_staging_test" if index == 0 else "nmdc.nmdc_provenance_staging_test",
            destination_id="nmdc-production",
            source_version="11.23.0",
            ingest_revision=REVISION,
            evidence={str(root / "evidence/complete.json"): "a" * 64},
            tables={"biosample_set": 2, "empty_set": 0}
            if index == 0
            else {"graph_edges": 2, "biosample_to_workflow_run": 2},
        )
        for index, root in enumerate(roots)
    ]
    descriptions = {name: SimpleNamespace(value="Reviewed table description") for s in sources for name in s.tables}
    columns = {name: [("id", "Stable identifier")] for name in descriptions}
    metadata = SimpleNamespace(target_schema_version="test", snapshot_id=PARENT)
    monkeypatch.setattr(
        promotion,
        "_load_source",
        lambda root: (next(s.model_copy(deep=True) for s in sources if s.root == str(root)), metadata, None),
    )
    monkeypatch.setattr(berdl_metadata, "_description_operations", lambda model: (descriptions, columns, []))
    tables = {
        f"{s.staging_namespace}.{name}": state(rows=count, snapshot_id="101" if count else None)
        for s in sources
        for name, count in s.tables.items()
    }
    tables.update(
        {
            f"{CANONICAL}." + name: state(rows=7, snapshot_id="99")
            for name in ["biosample_set", "graph_edges", *sorted(promotion.OBSOLETE_TEXTVALUE_TABLES)]
        }
    )
    tables[f"{sources[0].staging_namespace}.biosample_set"].columns.update({field.name: None for field in fields})
    spark = FakeSpark(tables)
    monkeypatch.setattr(promotion, "_runtime", lambda *a: spark)
    checkout = tmp_path / "ingest"
    checkout.mkdir()
    plan = promotion.build_promotion_plan(
        *roots,
        ingest_checkout=checkout,
        recovery="Stop writers, inspect saved before state and restore reviewed content manually.",
        spark=spark,
    )
    path = tmp_path / "promotion.json"
    save_json(path, plan.model_dump(mode="json"))
    return SimpleNamespace(roots=roots, sources=sources, spark=spark, checkout=checkout, plan=plan, path=path)


def run(candidate, **changes):
    authorization = dict(
        authorize_plan_sha256=file_digest(candidate.path),
        authorize_canonical_namespace=CANONICAL,
        authorize_destination_id="nmdc-production",
    )
    authorization.update(changes)
    return promotion.execute_promotion(candidate.path, **authorization)


def test_combined_plan_and_copy_preserve_metadata_and_verify_all(candidate):
    c = candidate
    assert c.plan.sources[1].parent_snapshot_id == c.plan.sources[0].snapshot_id
    assert [op.table for op in c.plan.operations if op.action == "drop"] == sorted(promotion.OBSOLETE_TEXTVALUE_TABLES)
    assert c.spark.writes == []
    text = promotion.render_promotion_plan(c.plan)
    assert "Expected result: 4 tables" in text and "No automatic rollback" in text
    result = run(c)
    assert result["status"] == "promotion-verified"
    assert result["snapshot_ids"] == [PARENT, DERIVED]
    assert result["started_at"] <= result["finished_at"]
    assert len(result["tables"]) == 4 and len(result["dropped"]) == 9
    assert [a for a, _ in c.spark.writes[:4]] == ["replace", "add", "add", "replace"]
    assert all(a == "drop" for a, _ in c.spark.writes[4:])
    assert len(c.spark.projections) == 4
    assert len(c.spark.reads) == 4 and any(snapshot is None for _, snapshot in c.spark.reads)
    for op in c.plan.operations:
        if op.expected is not None:
            observed = promotion._catalog_table(c.spark, CANONICAL, op.table)
            assert promotion._same_content_and_metadata(observed, op.expected)
    journal = c.path.with_suffix(".execution")
    assert len(list(journal.glob("*-verified.json"))) == 13
    before = json.loads((journal / "before.json").read_text())["before"]
    for path in sorted(journal.glob("*-attempt.json")):
        operation = json.loads(path.read_text())["operation"]
        previous = before.get(operation["table"])
        assert (previous is None) == (operation["action"] == "add")
        if operation["action"] == "drop":
            assert previous["snapshot_id"] == "99" and previous["rows"] == 7
            assert previous["physical_schema"] == [["id", "string"], ["optional", "string"]]
            verified = json.loads(path.with_name(path.name.replace("-attempt", "-verified")).read_text())
            assert verified["after"] is None and verified["status"] == "verified"
    assert (journal / "outcome.json").is_file()
    after = json.loads((journal / "000-verified.json").read_text())
    assert after["after"]["snapshot_id"] == "201"
    assert after["after"]["columns"]["id"] == "Stable identifier"
    assert after["verified_at"] >= result["started_at"]
    assert journal.stat().st_mode & 0o077 == 0
    with pytest.raises(promotion.PromotionPlanError, match="Automatic replay"):
        run(c)


@pytest.mark.parametrize(
    "field", ["authorize_plan_sha256", "authorize_canonical_namespace", "authorize_destination_id"]
)
def test_wrong_authorization_never_connects_or_writes(candidate, monkeypatch, field):
    monkeypatch.setattr(promotion, "_runtime", lambda *a: pytest.fail("Must not connect"))
    with pytest.raises(promotion.PromotionPlanError, match="exact reviewed"):
        run(candidate, **{field: "wrong"})
    assert not candidate.path.with_suffix(".execution").exists()


@pytest.mark.parametrize(
    "change",
    [
        "parent",
        "scope",
        "version",
        "destination",
        "derived-tables",
        "overlap",
        "revision",
        "namespace",
        "unknown",
        "duplicate",
        "missing",
        "unsafe",
        "action",
        "drop-first",
        "drop-source",
    ],
)
def test_saved_plan_refuses_inconsistent_or_unsupported_shapes(candidate, change):
    data = candidate.plan.model_dump()
    if change in {"parent", "scope", "version", "destination", "revision", "namespace"}:
        field = {
            "parent": "parent_snapshot_id",
            "scope": "scope",
            "version": "source_version",
            "destination": "destination_id",
            "revision": "ingest_revision",
            "namespace": "staging_namespace",
        }[change]
        data["sources"][1][field] = "wrong"
    elif change == "derived-tables":
        data["sources"][1]["tables"].pop("graph_edges")
    elif change == "overlap":
        data["sources"][0]["tables"]["graph_edges"] = 2
    elif change == "unknown":
        data["before"]["unknown"] = state().model_dump()
    elif change == "duplicate":
        data["operations"].append(data["operations"][0])
    elif change == "missing":
        data["operations"].pop()
    elif change == "unsafe":
        data["operations"][0]["table"] = "unsafe;name"
    elif change == "action":
        data["operations"][0]["action"] = "drop"
    elif change == "drop-first":
        data["operations"].reverse()
    else:
        data["operations"][-1]["source_namespace"] = "unexpected"
    with pytest.raises(ValueError):
        promotion.BerdlPromotionPlan.model_validate(data)


@pytest.mark.parametrize(
    "change",
    [
        "rows",
        "description",
        "column",
        "properties",
        "missing-staged",
        "staged-column",
        "staged-type",
        "extra-canonical",
        "missing-column",
        "wrong-column",
    ],
)
def test_preview_refuses_changed_stage_or_missing_projection(candidate, change):
    c = candidate
    staged = c.spark.tables[f"{c.sources[0].staging_namespace}.biosample_set"]
    if change == "rows":
        staged.rows += 1
    elif change == "description":
        staged.table_description = "changed"
    elif change == "column":
        staged.columns["id"] = "changed"
    elif change == "properties":
        staged.properties = {}
    elif change == "staged-column":
        staged.columns.pop("host_diet")
    elif change == "staged-type":
        c.spark.column_types["host_diet"] = "string"
    elif change == "missing-staged":
        del c.spark.tables[f"{c.sources[1].staging_namespace}.graph_edges"]
    elif change == "extra-canonical":
        c.spark.tables[f"{CANONICAL}.unreviewed"] = state()
    else:
        field = [] if change == "missing-column" else [pa.field("agrochem_addition", pa.string())]
        pq.write_table(pa.Table.from_pylist([], schema=pa.schema(field)), c.roots[0] / "snapshot/biosample_set.parquet")
    with pytest.raises(promotion.PromotionPlanError):
        promotion.build_promotion_plan(*c.roots, ingest_checkout=c.checkout, recovery="manual", spark=c.spark)
    assert c.spark.writes == []


@pytest.mark.parametrize("change", ["live", "evidence", "implementation"])
def test_changed_plan_inputs_refuse_before_any_mutation(candidate, change):
    c = candidate
    if change == "live":
        c.spark.tables[f"{CANONICAL}.biosample_set"].snapshot_id = "changed"
    elif change == "evidence":
        c.sources[0].evidence["new-input"] = "changed"
    else:
        data = json.loads(c.path.read_text())
        data["implementation_sha256"] = "changed"
        c.path.write_text(json.dumps(data))
    with pytest.raises(promotion.PromotionPlanError, match="Promotion stopped"):
        run(c)
    assert not c.spark.writes
    assert not (c.path.with_suffix(".execution") / "outcome.json").exists()


@pytest.mark.parametrize("exception", [RuntimeError("private runtime text"), KeyboardInterrupt()])
def test_partial_failure_preserves_journal_and_never_claims_recovery(candidate, capsys, exception):
    c = candidate

    def fail_second(target):
        c.spark.failure = exception

    c.spark.after_write = fail_second
    with pytest.raises(promotion.PromotionPlanError, match="No recovery was attempted"):
        run(c)
    journal = c.path.with_suffix(".execution")
    failure = json.loads((journal / "failure.json").read_text())
    assert failure["attempted"] == "empty_set" and failure["verified"] == ["biosample_set"]
    assert failure["recovery_attempted"] is False
    assert len(c.spark.writes) == 2 and not any(action == "drop" for action, _ in c.spark.writes)
    assert not (journal / "outcome.json").exists()
    assert "private runtime text" not in capsys.readouterr().out
    assert next(journal.glob("runtime-*.log")).stat().st_mode & 0o077 == 0


def test_wrong_copy_metadata_prevents_drops(candidate):
    c = candidate
    c.spark.after_write = lambda target: setattr(c.spark.tables[target], "table_description", "lost")
    with pytest.raises(promotion.PromotionPlanError):
        run(c)
    assert len(c.spark.writes) == 1


def test_late_copy_drift_is_caught_before_obsolete_drops(candidate):
    c = candidate

    def drift(target):
        if target.endswith(".graph_edges"):
            c.spark.tables[f"{CANONICAL}.biosample_set"].snapshot_id = "concurrent-same-count-copy"

    c.spark.after_write = drift
    with pytest.raises(promotion.PromotionPlanError):
        run(c)
    assert len(c.spark.writes) == 4 and not any(action == "drop" for action, _ in c.spark.writes)


def test_per_table_canonical_guard_stops_concurrent_changes(candidate):
    c = candidate

    def drift(target):
        c.spark.tables[f"{CANONICAL}.graph_edges"].snapshot_id = "concurrent-writer"

    c.spark.after_write = drift
    with pytest.raises(promotion.PromotionPlanError):
        run(c)
    assert len(c.spark.writes) == 3


def test_current_snapshot_reference_not_latest_history_and_catalog_count_refusals(candidate, monkeypatch):
    c = candidate
    target = f"{CANONICAL}.biosample_set"
    observed = promotion._catalog_table(c.spark, CANONICAL, "biosample_set")
    assert observed.snapshot_id == "99"
    assert all(".snapshots" not in query for query in c.spark.queries)
    c.spark.tables[target].snapshot_id = None
    with pytest.raises(promotion.PromotionPlanError, match="populated"):
        promotion._catalog_table(c.spark, CANONICAL, "biosample_set")
    c.spark.tables[target].rows = True
    with pytest.raises(promotion.PromotionPlanError, match="Unusable row count"):
        promotion._catalog_table(c.spark, CANONICAL, "biosample_set")
    values = iter(["before", 2, "after"])
    monkeypatch.setattr(promotion, "_scalar", lambda *args: next(values))
    with pytest.raises(promotion.PromotionPlanError, match="while reading"):
        promotion._catalog_table(c.spark, CANONICAL, "biosample_set")


def test_output_locations_and_legacy_plan_refused(candidate):
    c = candidate
    with pytest.raises(promotion.PromotionPlanError, match="outside"):
        promotion.plan_promotion(
            *c.roots, c.roots[0] / "snapshot/new.json", ingest_checkout=c.checkout, recovery="manual"
        )
    with pytest.raises(promotion.PromotionPlanError, match="never overwritten"):
        promotion.plan_promotion(*c.roots, c.path, ingest_checkout=c.checkout, recovery="manual")
    link = c.path.with_name("alias.json")
    link.symlink_to(c.path)
    with pytest.raises(ValueError):
        promotion.load_promotion_plan(link)
    c.path.write_text('{"plan_format_version":2,"derived_rebuilds":[]}')
    with pytest.raises(ValidationError):
        promotion.load_promotion_plan(c.path)


def test_preview_wrapper_private_log_and_cli(candidate, monkeypatch, capsys):
    c = candidate
    # This wrapper reads the official revision before connecting. All other input
    # checks remain exercised through build_promotion_plan above.
    monkeypatch.setattr(
        berdl_staging, "load_berdl_staging_plan", lambda *a: SimpleNamespace(ingest=SimpleNamespace(revision=REVISION))
    )
    output = c.path.with_name("preview.json")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "berdl-promotion-plan",
            *map(str, c.roots),
            str(output),
            "--ingest-checkout",
            str(c.checkout),
            "--recovery",
            "manual",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "plan_sha256=" in result.output and not c.spark.writes
    result = runner.invoke(cli, ["berdl-promote", str(output)])
    assert result.exit_code == 0 and "Preview only" in result.output and not c.spark.writes
    result = runner.invoke(
        cli,
        [
            "berdl-promote",
            str(output),
            "--authorize-plan-sha256",
            file_digest(output),
            "--authorize-canonical-namespace",
            CANONICAL,
            "--authorize-destination-id",
            "nmdc-production",
        ],
    )
    assert result.exit_code == 0 and '"status": "promotion-verified"' in result.output

    def fail(*args):
        raise ValueError("private credential diagnostic")

    monkeypatch.setattr(promotion, "_runtime", fail)
    output2 = c.path.with_name("failed.json")
    with pytest.raises(promotion.PromotionPlanError, match="No canonical writes"):
        promotion.plan_promotion(*c.roots, output2, ingest_checkout=c.checkout, recovery="manual")
    assert "private credential diagnostic" not in capsys.readouterr().out
    assert not output2.exists()


def test_real_completed_stage_evidence_loads_without_old_runtime_revalidation(planned, monkeypatch):
    root, authorization, _, _ = planned
    staging.stage_publication(root, **authorization)
    monkeypatch.setattr(
        berdl_staging,
        "revalidate_berdl_staging_plan",
        lambda *a: pytest.fail("Historical staging must not require the new adapter"),
    )
    source, metadata, manifest = promotion._load_source(root)
    assert source.snapshot_id == manifest.snapshot_id == metadata.snapshot_id
    assert source.parent_snapshot_id == manifest.parent_snapshot_id
    assert set(source.tables) == {"graph_edges", "biosample_to_workflow_run"}
    assert all(file_digest(Path(path)) == digest for path, digest in source.evidence.items())
    (root / "evidence/metadata-bundle.json").write_text("changed")
    with pytest.raises(promotion.PromotionPlanError, match="Changed staging evidence"):
        promotion._load_source(root)


@pytest.mark.parametrize("change", ["missing", "duplicate"])
def test_historical_staging_requires_the_complete_unique_evidence_set(planned, change):
    root, authorization, _, _ = planned
    staging.stage_publication(root, **authorization)
    path = root / "evidence/berdl-staging-plan.json"
    document = json.loads(path.read_text())
    if change == "missing":
        document["evidence"] = [item for item in document["evidence"] if item["name"] != "metadata-bundle.json"]
    else:
        document["evidence"].append(document["evidence"][0])
    path.write_text(json.dumps(document))
    with pytest.raises(berdl_staging.BerdlStagingPlanError, match="complete and unique"):
        promotion._load_source(root)


def test_changed_historical_evidence_path_invalidates_the_verified_outcome(planned):
    root, authorization, _, _ = planned
    staging.stage_publication(root, **authorization)
    path = root / "evidence/berdl-staging-plan.json"
    document = json.loads(path.read_text())
    item = next(item for item in document["evidence"] if item["name"] == "destination-inventory.json")
    other = root.parent / "another-inventory.json"
    other.write_bytes(Path(item["path"]).read_bytes())
    item["path"] = str(other)
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="data outcome differs"):
        promotion._load_source(root)


def test_empty_source_populated_after_refresh_stops_before_its_write(candidate):
    c = candidate

    def populate_empty_source(_target):
        staged = c.spark.tables[f"{c.sources[0].staging_namespace}.empty_set"]
        staged.rows = 1
        staged.snapshot_id = "new-source-snapshot"

    c.spark.after_write = populate_empty_source
    with pytest.raises(promotion.PromotionPlanError, match="Promotion stopped"):
        run(c)
    assert c.spark.writes == [("replace", f"{CANONICAL}.biosample_set")]
    failure = json.loads((c.path.with_suffix(".execution") / "failure.json").read_text())
    assert failure["attempted"] == "empty_set" and failure["verified"] == ["biosample_set"]
