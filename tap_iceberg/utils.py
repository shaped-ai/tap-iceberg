import pyarrow as pa
from pyarrow import DataType
from singer_sdk import typing as th


def pyarrow_to_jsonschema_type(arrow_type: DataType) -> th.JSONTypeHelper:
    """Convert a PyArrow data type to a corresponding JSON Schema type."""
    if pa.types.is_boolean(arrow_type):
        return th.BooleanType()
    elif pa.types.is_integer(arrow_type):
        return th.IntegerType()
    elif pa.types.is_floating(arrow_type):
        return th.NumberType()
    elif pa.types.is_string(arrow_type):
        return th.StringType()
    elif pa.types.is_binary(arrow_type):
        return th.BinaryType()
    elif pa.types.is_date(arrow_type):
        return th.DateType()
    elif pa.types.is_time(arrow_type):
        return th.TimeType()
    elif pa.types.is_timestamp(arrow_type):
        return th.DateTimeType()
    elif pa.types.is_duration(arrow_type):
        return th.DurationType()
    elif pa.types.is_decimal(arrow_type):
        return th.NumberType()
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return th.ArrayType(pyarrow_to_jsonschema_type(arrow_type.value_type))
    elif pa.types.is_struct(arrow_type):
        return th.ObjectType(
            properties={
                field.name: pyarrow_to_jsonschema_type(field.type)
                for field in arrow_type
            }
        )
    elif pa.types.is_map(arrow_type):
        return th.ObjectType(
            additional_properties=pyarrow_to_jsonschema_type(arrow_type.item_type)
        )
    elif pa.types.is_dictionary(arrow_type):
        return pyarrow_to_jsonschema_type(arrow_type.value_type)
    else:
        # Default to string for unknown types
        return th.StringType()


def generate_schema_from_pyarrow(arrow_schema: pa.Schema) -> dict:
    """Generate a JSON Schema from a PyArrow schema."""
    properties = {}
    for field in arrow_schema:
        json_type = pyarrow_to_jsonschema_type(field.type)
        properties[field.name] = th.Property(field.name, json_type)

    return th.PropertiesList(*properties.values()).to_dict()
