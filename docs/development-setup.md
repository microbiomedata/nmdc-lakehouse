# Development setup

This page is the authoritative requirements and setup guide for ordinary
development. The bootstrap path is credential-free: it does not connect to
MongoDB, NERSC, GCP, or BERDL.

## Host requirements

Install host tools through your operating system's trusted package manager. The
repository does not execute remote installers.

| Tool | Supported version | Installed by | Needed for |
|---|---|---|---|
| Git | A maintained release | User or operating system | Checkout and pre-commit hooks |
| uv | 0.12.3 or newer | User | Python 3.13 and locked Python dependencies |
| just | Exactly 1.58.0 | User | Canonical recipes and justfile formatting |
| Vale | Exactly 3.17.1 | User | `just prose-lint` and `just check` |
| Go | Exactly 1.26.5 | User | Compiling the pinned actionlint hook |

Go is a development-tool prerequisite, not an application runtime dependency.
The bootstrap command installs the pre-commit hook without compiling every hook;
Go is needed when actionlint first runs. CI provisions the exact just, Vale, and
Go versions above. uv reads the project's `required-version` setting at runtime
and exits with an error when the installed release is older than 0.12.3.

## Managed environment

The repository, uv, and pre-commit manage the remaining tools:

| Component | Version source | Installed by |
|---|---|---|
| Python | `.python-version` and `requires-python` select 3.13, excluding 3.14 | uv |
| Runtime, development, and documentation packages | `pyproject.toml` plus `uv.lock` | uv |
| Ruff, `mypy`, pytest, `deptry`, and ShellCheck | Locked `dev` extra | uv |
| MkDocs and documentation plugins | Locked `docs` extra | uv |
| Git hooks, including actionlint | `.pre-commit-config.yaml` | pre-commit |

Do not install these packages individually into the environment. Refreshing
dependency versions is an explicit lock-file change; ordinary setup uses
`uv sync --locked`.

## Bootstrap a checkout

From the repository root, run:

<!-- unverified: no run of this procedure is recorded. Declaring the blocks
     that predate this rule was
     https://github.com/microbiomedata/nmdc-lakehouse/issues/291, now closed;
     nothing tracks running them. -->
```bash
just bootstrap
```

The command:

1. creates or updates `.venv` from the locked development and documentation
   extras;
2. installs the repository's pre-commit hook, or preserves and reports an
   existing custom Git hooks policy;
3. smoke-tests the installed `nmdc-lakehouse` command; and
4. prints useful next commands.

Running it again is safe and must not change tracked files. Common next steps
are:

<!-- unverified: no run of this procedure is recorded. Declaring the blocks
     that predate this rule was
     https://github.com/microbiomedata/nmdc-lakehouse/issues/291, now closed;
     nothing tracks running them. -->
```bash
just doctor
just test
just check
just cli --help
```

Neither bootstrap nor unit tests require `.env`, credentials, a database, or a
tunnel.

If Git already has `core.hooksPath` configured, bootstrap preserves that
hooks-path policy rather than replacing its hooks. In that case it prints the
explicit repository-hook command, `uv run pre-commit run --all-files`. Without
a custom hooks path, bootstrap installs the repository pre-commit hook normally.

## Diagnose an installed checkout

Run the read-only, offline diagnostic after bootstrap:

<!-- unverified: no run of this procedure is recorded. Declaring the blocks
     that predate this rule was
     https://github.com/microbiomedata/nmdc-lakehouse/issues/291, now closed;
     nothing tracks running them. -->
```bash
just doctor
```

Doctor checks the required command versions, Python minor, lock synchronization,
pre-commit hook, optional configuration names, and local path safety. It never
prints configured values, modifies the environment, or contacts remote services.
Required failures return a nonzero status with a remediation. Warnings describe
optional production-data readiness and do not make unit development fail.

The justfile loads `.env` before starting a recipe. If `just doctor` exits while
parsing a malformed `.env`, bypass that initial loading step:

<!-- unverified: no run of this procedure is recorded. Declaring the blocks
     that predate this rule was
     https://github.com/microbiomedata/nmdc-lakehouse/issues/291, now closed;
     nothing tracks running them. -->
```bash
uv run --no-sync nmdc-lakehouse doctor
```

Doctor will inspect `.env` itself and report the problem without printing its
contents.

### Opt in to live-service checks

The default doctor remains offline. Pass one or more explicit service checks
only when you intend to inspect live-service readiness:

<!-- unverified: no run of this procedure is recorded. Declaring the blocks
     that predate this rule was
     https://github.com/microbiomedata/nmdc-lakehouse/issues/291, now closed;
     nothing tracks running them. -->
```bash
uv run --no-sync nmdc-lakehouse doctor --service-check mongo-config
uv run --no-sync nmdc-lakehouse doctor --service-check gcp-tunnel
uv run --no-sync nmdc-lakehouse doctor --service-check mongo-ping
```

If `.env` exists but has an invalid assignment or cannot be read, doctor fails
closed: it reports the configuration problem and skips every requested live
service check. Fix `.env` before retrying so no check uses a partially parsed
set of connection values.

`mongo-config` validates the decomposed `MONGO_*` settings and constructs the
credential-bearing URI only in memory. `gcp-tunnel` also verifies the
owner-only jump key (overridden with `NMDC_JUMP_KEY`), requires a local host
with direct connection, and attempts a one-second TCP connection to the
configured local port.
`mongo-ping` attempts a one-second, read-only PyMongo `ping` and distinguishes
configuration, network, and authentication failures.

No profile prints a configured value or URI. These checks never open or close
an SSH tunnel, change `.env`, or write to MongoDB. The tunnel itself remains a
separately managed process; see [Connecting to MongoDB](mongodb-connection.md).

## Supported systems

| Environment | Support status |
|---|---|
| Ubuntu 24.04 x86-64 | Required CI environment |
| macOS on Apple silicon | Verified native contributor environment |
| Other Linux and macOS systems | Expected to work; not represented in CI |
| Native Windows | Not currently supported or tested; use WSL where practical |
| Docker | Planned in [#170](https://github.com/microbiomedata/nmdc-lakehouse/issues/170) |

Operational recipes may have narrower platform requirements than package and
unit-test development. Their runbooks document those requirements.

## Optional services

These are not bootstrap requirements:

| Work | Optional service or credential | Guide or owner |
|---|---|---|
| Unit development, linting, docs, and builds | None | This page |
| Live NMDC metadata ETL | GCP jump-host key and MongoDB credentials | [MongoDB connection](mongodb-connection.md) and [#155](https://github.com/microbiomedata/nmdc-lakehouse/issues/155) |
| NERSC workflow-result access | NERSC account, filesystem access, and relevant network setup | Result-specific runbooks; package migration is [#130](https://github.com/microbiomedata/nmdc-lakehouse/issues/130) |
| BERDL publication | BERDL account, tokens, object-store access, and cluster tooling | `just berdl-doctor SNAPSHOT_ROOT`, [BERDL upload](berdl-upload.md), and [#51](https://github.com/microbiomedata/nmdc-lakehouse/issues/51) |
| Local MongoDB integration tests | A reachable test MongoDB and explicit opt-in | [#155](https://github.com/microbiomedata/nmdc-lakehouse/issues/155) |

Never commit credentials or copy them as part of repository bootstrap.
