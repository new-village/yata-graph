import pytest
from src.main import app

def test_get_schema(api_client):
    response = api_client.get("/api/v1/schema")
    assert response.status_code == 200
    res = response.json()
    
    assert "nodes" in res
    assert "edges" in res
    
    # Check simple content expectation
    # nodes table should have id, node_type, display_name...
    node_columns = {col["name"] for col in res["nodes"]}
    assert "id" in node_columns
    assert "node_type" in node_columns
    
    # edges table should have id, source_id, target_id, edge_type...
    edge_columns = {col["name"] for col in res["edges"]}
    assert "id" in edge_columns
    assert "source_id" in edge_columns
    assert "target_id" in edge_columns

    # Check type of a known column
    # id in nodes is BIGINT
    id_col = next(col for col in res["nodes"] if col["name"] == "id")
    assert id_col["type"] == "BIGINT"
