# NMDC Lakehouse

This documentation covers the architecture and operational runbooks for the
NMDC metadata-to-lakehouse work. Start with the repository
[README](https://github.com/microbiomedata/nmdc-lakehouse#implementation-status)
for the authoritative implementation-status matrix: local Parquet generation
is implemented, while managed Iceberg publication remains a separate BERDL
operation.

- [Architecture](architecture.md)
- [Development setup](development-setup.md)
- [MongoDB connection](mongodb-connection.md)
- [Uploading Parquet to BERDL](berdl-upload.md)
- [BERDL metadata shaping](berdl-metadata-shaping.md)
