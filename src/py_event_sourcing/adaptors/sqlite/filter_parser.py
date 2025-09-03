from typing import Any, Callable, List, Tuple, Union

from py_event_sourcing.models import EqualsClause, EventFilter, InClause, LikeClause

# Type alias for handler functions
HandlerFunc = Callable[[Union[EqualsClause, InClause, LikeClause], List[Any]], str]


def _parse_sql_target(field: str, params: List[Any]) -> str:
    """Helper to determine if a field is a standard column or a JSON path."""
    allowed_fields = ["stream_id", "event_type", "version"]
    if field.startswith("metadata."):
        json_path = "$." + field.split(".", 1)[1]
        params.append(json_path)
        return "json_extract(metadata, ?)"
    elif field in allowed_fields:
        return field
    else:
        raise ValueError(f"Filtering on field '{field}' is not supported.")


def _handle_equals_clause(clause: EqualsClause, params: List[Any]) -> str:
    target_sql = _parse_sql_target(clause.field, params)
    params.append(clause.value)
    return f"{target_sql} = ?"


def _handle_in_clause(clause: InClause, params: List[Any]) -> str:
    target_sql = _parse_sql_target(clause.field, params)
    placeholders = ", ".join("?" for _ in clause.value)
    params.extend(clause.value)
    return f"{target_sql} IN ({placeholders})"


def _handle_like_clause(clause: LikeClause, params: List[Any]) -> str:
    target_sql = _parse_sql_target(clause.field, params)
    params.append(clause.value)
    return f"{target_sql} LIKE ?"


def parse_event_filter_to_sql(event_filter: EventFilter) -> Tuple[str, List[Any]]:
    conditions: List[str] = []
    params: List[Any] = []

    handler_map = {
        EqualsClause: _handle_equals_clause,
        InClause: _handle_in_clause,
        LikeClause: _handle_like_clause,
    }

    for clause in event_filter.clauses:
        clause_type = type(clause)
        if clause_type in handler_map:
            handler = handler_map[clause_type]
            conditions.append(handler(clause, params))  # type: ignore[operator]
        else:
            raise TypeError(f"Unsupported clause type: {clause_type}")

    if not conditions:
        return "", []

    return "WHERE " + " AND ".join(conditions), params
