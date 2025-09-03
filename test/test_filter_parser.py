import pytest
from typing import List, Any, Tuple

from py_event_sourcing.models import EventFilter, EqualsClause, InClause, LikeClause
from py_event_sourcing.adaptors.sqlite.filter_parser import parse_event_filter_to_sql

# -----------------------------------------------------------------------------
# Unit Tests for the Parsing Logic
# -----------------------------------------------------------------------------

def test_parse_simple_equality():
    """Tests a single EqualsClause."""
    event_filter = EventFilter(clauses=[
        EqualsClause(field="stream_id", value="user-123")
    ])
    sql, params = parse_event_filter_to_sql(event_filter)
    assert sql == "WHERE stream_id = ?"
    assert params == ["user-123"]

def test_parse_in_clause():
    """Tests an InClause with a list of values."""
    event_filter = EventFilter(clauses=[
        InClause(field="event_type", value=["user_created", "user_updated"])
    ])
    sql, params = parse_event_filter_to_sql(event_filter)
    assert sql == "WHERE event_type IN (?, ?)"
    assert params == ["user_created", "user_updated"]

def test_parse_like_clause():
    """Tests a LikeClause."""
    event_filter = EventFilter(clauses=[
        LikeClause(field="stream_id", value="order-%")
    ])
    sql, params = parse_event_filter_to_sql(event_filter)
    assert sql == "WHERE stream_id LIKE ?"
    assert params == ["order-%"]

def test_parse_simple_metadata_clause():
    """Tests a query on a top-level metadata field using EqualsClause."""
    event_filter = EventFilter(clauses=[
        EqualsClause(field="metadata.correlation_id", value="abc-123")
    ])
    sql, params = parse_event_filter_to_sql(event_filter)
    assert sql == "WHERE json_extract(metadata, ?) = ?"
    assert params == ["$.correlation_id", "abc-123"]

def test_parse_nested_metadata_clause():
    """Tests a query on a nested metadata field using EqualsClause."""
    event_filter = EventFilter(clauses=[
        EqualsClause(field="metadata.tags.region", value="EMEA")
    ])
    sql, params = parse_event_filter_to_sql(event_filter)
    assert sql == "WHERE json_extract(metadata, ?) = ?"
    assert params == ["$.tags.region", "EMEA"]

def test_parse_combined_clauses():
    """Tests a combination of different clause types."""
    event_filter = EventFilter(clauses=[
        LikeClause(field="stream_id", value="customer-%"),
        EqualsClause(field="metadata.verified", value=True)
    ])
    sql, params = parse_event_filter_to_sql(event_filter)
    assert sql == "WHERE stream_id LIKE ? AND json_extract(metadata, ?) = ?"
    assert params == ["customer-%", "$.verified", True]

def test_validation_unsupported_field():
    """Ensures that filtering on a non-whitelisted field raises an error."""
    event_filter = EventFilter(clauses=[
        EqualsClause(field="timestamp", value="2023-01-01")
    ])
    with pytest.raises(ValueError, match="Filtering on field 'timestamp' is not supported."):
        parse_event_filter_to_sql(event_filter)

def test_pydantic_validation_for_in_clause():
    """Ensures Pydantic's validation catches incorrect values for InClause."""
    with pytest.raises(ValueError):
        # Pydantic should reject a non-list value
        InClause(field="event_type", value="not-a-list")
    
    with pytest.raises(ValueError):
        # Pydantic should reject an empty list due to Field(..., min_items=1)
        InClause(field="event_type", value=[])
