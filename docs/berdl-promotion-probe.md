# Probing BERDL promotion and recovery capability

Canonical promotion is not implemented. Before it can be, the platform has to
answer three questions that no BERDL runbook currently answers: whether a table
can be renamed across two namespaces in the same tenant catalog, whether a
supported recovery operation exists and is permitted, and how long a snapshot
survives so a recovery promise can be bounded. This command establishes those
answers with evidence rather than assumption, and writes a credential-free
report for [#240](https://github.com/microbiomedata/nmdc-lakehouse/issues/240)
and [#234](https://github.com/microbiomedata/nmdc-lakehouse/issues/234).

The probe never touches a canonical, shared, or production object. It refuses
any namespace that names `nmdc_metadata`, `nmdc_results`, or `nmdc_ref_data`,
and it requires both namespaces to use a disposable `<name>_probe_<suffix>`
dataset inside one tenant. It creates its own two synthetic tables of two rows
each and confines every mutation to them.

## Preview the plan

Preview is the default. It is offline: it contacts no service, starts no Spark
session, and creates nothing.

```bash
just berdl-promotion-probe \
  nmdc \
  nmdc.promotion_probe_20260820 \
  nmdc.promotion_probe_20260820_target \
  /absolute/path/to/promotion-probe-outcome.json
```

The command prints the immutable plan on stdout and its SHA-256 on stderr.
Review the two namespaces and the table names before going further.

## Run the probe

Run it in a BERDL JupyterHub pod, where the Spark session and catalog are
local. Start the Spark Connect sidecar with `get_spark_session()` in a notebook
before using the pod terminal. Supply the exact plan digest from the preview as
explicit authorization:

```bash
just berdl-promotion-probe \
  nmdc \
  nmdc.promotion_probe_20260820 \
  nmdc.promotion_probe_20260820_target \
  /absolute/path/to/promotion-probe-outcome.json \
  --execute-probe \
  --authorize-plan-sha256 'FULL_PLAN_SHA256'
```

The outcome path must not already exist; the probe publishes it atomically and
refuses to replace an earlier report.

## What the report records

Each attempted operation carries a verdict:

| Verdict | Meaning |
| --- | --- |
| `supported` | The platform accepted the statement. |
| `unsupported-syntax` | The parser rejected the statement. |
| `insufficient-grants` | The principal is not permitted to run it. |
| `unavailable-capability` | The operation does not exist in this deployment. |
| `failed-as-expected` | The step was designed to fail, and it did. |
| `unclassified-failure` | The failure matched none of the above. |
| `not-attempted` | An earlier result made the attempt unnecessary. |

A failing step also records `error_type`, the exception class name, and
`error_condition`, the provider's stable error identifier such as
`INSUFFICIENT_PRIVILEGES` or `UNSUPPORTED_FEATURE`. Those identifiers are
enumerated codes rather than free text, so they can be recorded without carrying
provider message content into the report.

Classification does not rely on `error_condition` alone. The verdict is matched
against the exception type name, the error condition when the provider supplies
one, and a small set of markers found in the provider's message, all compared
case-folded. The message is used to classify but is never recorded. An
`unclassified-failure` means none of those matched, and the statement has to be
rerun by hand in the pod terminal to learn why.

A step may also carry `independently_verified`, which is `null` when the check
backing it could not be completed. A failed catalog listing is never recorded as
a table being absent.

The report distinguishes three things that are easy to conflate, and does so
everywhere rather than only where it was convenient: a value the platform does
not have, a value the principal may not read, and a value that could not be
parsed. An environment field that could not be read is named in
`unresolved_questions` rather than left indistinguishable from one that is unset,
a retention property that is present but not an integer is `unclassified-failure`
rather than `unavailable-capability`, and a failed `EXPLAIN` is recorded as no
plan having been produced rather than as `EXPLAIN` being unsupported. A call that returns without error is not evidence
that it did anything, so recovery is checked by reading the table back and comparing row counts, and
the injected failure is checked by confirming the destination table it targeted does not exist.

`unclassified-failure` is deliberate. An unrecognized error is not silently
folded into a known cause, and any such result sets the report status to
`probe-incomplete` so it cannot be read as a clean answer.

The environment block records the Spark version, the catalog implementation, and
the value of `spark.sql.extensions`. That last one is a list of extension class
names, not a version, and the field is named `spark_sql_extensions` to say so. It
is still worth recording: on the first live run it showed the deployment loads
both the Delta and the Iceberg extensions. Iceberg and Polaris version numbers
are not currently captured, and the report does not imply otherwise.

The report carries the exact statements the probe constructed, the exception
type name, table snapshot identifiers, row counts, and schema fingerprints
before mutation, after mutation, and after recovery. It does not carry provider
exception text, connection details, credentials, or data rows. Read full
provider errors in the pod terminal when a verdict needs interpretation.

## How recovery and partial promotion are tested

The recovery point is read from the promoted destination table, not from the
source it was copied from. A replacement creates a new table whose snapshot
history starts fresh, so a source snapshot identifier would not exist there and
a rollback would fail for the wrong reason.

The probe then makes a second mutation on the promoted table so rollback has a
real earlier snapshot to return to, rolls back to the recorded point, and reads
the row count back to confirm the data actually returned.

The injected failure is a genuine one. The probe attempts to promote the second
table from a source that does not exist, so the run fails between two table
mutations, then records which destination tables exist. A destination holding
the first table and not the second shows that promotion is not atomic across
tables and that a partial promotion is observable.

That step is expected to fail, so it is recorded as `failed-as-expected` rather
than as a missing platform capability. A missing input table is a data
condition, not evidence that the platform lacks an operation, and the report
would be misleading if the two looked alike. For the same reason a table that is
deliberately absent is not reported as unreadable; only a table that exists and
cannot be read raises an unresolved question.

`unresolved_questions` names what the run could not settle, including a rollback
that reported success without restoring rows, an injected failure that did not
fail, and a retention property that could not be read.

That last distinction matters. A retention property the platform does not have
is `unavailable-capability`. A retention property the principal is not permitted
to read is `insufficient-grants`, and the report says the recovery window is
unknown for a reason unrelated to platform capability. Reporting the second as
the first would blame the platform for a permission problem.

## After the run

Fetch the report to the local candidate workspace and attach its findings to
[#240](https://github.com/microbiomedata/nmdc-lakehouse/issues/240). If no
recovery operation is both available and permitted,
[#234](https://github.com/microbiomedata/nmdc-lakehouse/issues/234) stays
blocked with a specific platform-owner question rather than proceeding on an
assumed mechanism.

Clean up the disposable namespaces once the report is retained.
