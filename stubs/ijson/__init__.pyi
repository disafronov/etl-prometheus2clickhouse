"""Type stubs for ijson library.

This stub file provides type annotations for ijson.parse() function
used in the ETL job for streaming JSON parsing.
"""

from decimal import Decimal
from typing import BinaryIO, Iterator, Literal, Union

# Event types that ijson.parse() can yield
EventType = Literal[
    "start_map",
    "end_map",
    "start_array",
    "end_array",
    "string",
    "number",
    "boolean",
    "null",
]

# Value types that can be returned in events
# Note: The actual type depends on the event type:
# - "number" -> int | float | Decimal (Decimal when use_float=False, float when use_float=True)
# - "string" -> str
# - "boolean" -> bool
# - "null" -> None
# - other events -> None
ValueType = Union[str, int, float, Decimal, bool, None]

# Parser event tuple: (prefix, event, value)
# Type narrowing should be used in code when accessing value based on event type
ParserEvent = tuple[str, EventType, ValueType]

def parse(
    file: BinaryIO,
    *,
    multiple_values: bool = False,
    use_float: bool = False,
    buf_size: int = 65536,
) -> Iterator[ParserEvent]:
    """Parse JSON from binary file stream, yielding events.

    Args:
        file: Binary file object to parse JSON from
        multiple_values: If True, allows multiple JSON values in one stream
        use_float: If True, uses float instead of Decimal for numbers
        buf_size: Buffer size for reading from file

    Yields:
        Tuple of (prefix, event, value) where:
        - prefix: JSON path prefix (e.g., "data.result.item")
        - event: Event type ("start_map", "end_map", "string", "number", etc.)
        - value: Event value (string, number, bool, or None depending on event type)
    """
    ...
