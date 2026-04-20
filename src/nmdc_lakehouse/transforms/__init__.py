"""Transform layer.

Flattens the nested NMDC / LinkML object model into a tabular representation
suitable for Parquet / Iceberg. The LinkML ``SchemaView`` is used to drive
the flattening so that the shape of the output follows the schema.
"""
