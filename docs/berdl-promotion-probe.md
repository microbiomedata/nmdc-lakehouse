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
| `unclassified-failure` | The failure matched none of the above. |
| `not-attempted` | An earlier result made the attempt unnecessary. |

`unclassified-failure` is deliberate. An unrecognized error is not silently
folded into a known cause, and any such result sets the report status to
`probe-incomplete` so it cannot be read as a clean answer.

The report carries the exact statements the probe constructed, the exception
type name, table snapshot identifiers, row counts, and schema fingerprints
before mutation, after mutation, and after recovery. It does not carry provider
exception text, connection details, credentials, or data rows. Read full
provider errors in the pod terminal when a verdict needs interpretation.

`unresolved_questions` names what the run could not settle, including a
destination that held a readable second table while the first was mid-promotion,
which shows a partial promotion is observable rather than atomic.

## After the run

Fetch the report to the local candidate workspace and attach its findings to
[#240](https://github.com/microbiomedata/nmdc-lakehouse/issues/240). If no
recovery operation is both available and permitted,
[#234](https://github.com/microbiomedata/nmdc-lakehouse/issues/234) stays
blocked with a specific platform-owner question rather than proceeding on an
assumed mechanism.

Clean up the disposable namespaces once the report is retained.
