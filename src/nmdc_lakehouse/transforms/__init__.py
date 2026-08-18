"""Transform layer.

Flattens the nested NMDC / LinkML object model into a tabular representation
suitable for tabular storage. The LinkML ``SchemaView`` drives projection and
type construction so the output shape follows the schema; this layer does not
perform full record validation.
"""
