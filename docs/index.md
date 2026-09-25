# NMDC Lakehouse

This documentation covers the architecture and operational runbooks for the
NMDC metadata-to-lakehouse work. Start with the repository
[README](https://github.com/microbiomedata/nmdc-lakehouse#implementation-status)
for the authoritative implementation-status matrix. Local Parquet generation
and publication commands are implemented. The combined pod workflow still needs
live acceptance, and canonical promotion requires separate exact-plan approval.

- [Complete publication lifecycle](publication-lifecycle.md)
- [Architecture](architecture.md)
- [Development setup](development-setup.md)
- [Package versions and releases](releases.md)
- [MongoDB connection](mongodb-connection.md)
- [BERDL staging runbook](berdl-staging-runbook.md)
- [Local provenance and query comparison](local-provenance.md)
- [BERDL command details](berdl-upload.md)
- [Portable publication and replacement contract](publication-contract.md)
- [BERDL metadata shaping](berdl-metadata-shaping.md)
- [How a LinkML description becomes an Iceberg column comment](column-description-path.md)
