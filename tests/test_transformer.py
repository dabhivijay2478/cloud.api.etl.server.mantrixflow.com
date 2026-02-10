from transformer import safe_exec_transform, validate_transform_script


def test_transform_script_executes():
    script = """
def transform(record):
    return {
        "id": record.get("id"),
        "name_upper": (record.get("name") or "").upper(),
    }
"""
    result = safe_exec_transform([{"id": 1, "name": "alice"}], script)
    assert result["errors"] == []
    assert result["transformed_rows"][0]["name_upper"] == "ALICE"


def test_transform_script_rejects_import():
    script = """
import os
def transform(record):
    return {"id": record.get("id")}
"""
    validation = validate_transform_script(script)
    assert validation["valid"] is False
    assert "Only `import json` is allowed" in validation["error"]


def test_transform_script_allows_json_import():
    script = """
import json

def transform(record):
    return {
        "id": record.get("id"),
        "payload": json.dumps({"name": record.get("name")}),
    }
"""
    validation = validate_transform_script(script)
    assert validation["valid"] is True
