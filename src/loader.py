import duckdb
import yaml
import os
from typing import Optional

def load_config(config_path: str = "config/icij.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_connection(database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database)

def load_data(
    config_path: str = "config/icij.yaml", 
    conn: Optional[duckdb.DuckDBPyConnection] = None
) -> duckdb.DuckDBPyConnection:
    config = load_config(config_path)
    
    if conn is None:
        conn = get_connection()
    
    # Enable DuckPGQ first
    print("Enabling DuckPGQ extension...")
    conn.execute("INSTALL duckpgq FROM community")
    conn.execute("LOAD duckpgq")

    node_labels = {}
    relationships_table = ""
    
    # 1. Load Tables (as TEMP Tables)
    for source in config.get("sources", []):
        table_name = source["table"]
        file_path = source["path"]
        label = source.get("label")
        
        print(f"Loading {table_name} from {file_path}...")
        
        # Use TEMP TABLE to avoid persistence overhead
        conn.execute(f"CREATE OR REPLACE TEMP TABLE {table_name} AS SELECT * FROM '{file_path}'")
        
        if label:
            node_labels[label] = table_name
        elif table_name == "relationships":
            relationships_table = table_name

    # 2. Generate Dynamic Property Graph DDL
    print("Defining Property Graph 'icij_graph'...")
    
    # Vertex Tables definition
    vertex_defs = []
    for label, table in node_labels.items():
        vertex_defs.append(f"{table} LABEL {label}")
    
    # Edge Tables definition
    # Since relationships are pre-typed in 'relationships_typed.parquet', we can query distinct types directly
    # from the loaded TEMP table.
    
    combinations = conn.execute(f"""
        SELECT DISTINCT start_label, end_label FROM {relationships_table}
    """).fetchall()
    
    edge_defs = []
    for start_label, end_label in combinations:
        rel_subname = f"rel_{start_label}_{end_label}"
        
        # Create sub-tables (TEMP) for each relation pair
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE {rel_subname} AS 
            SELECT * FROM {relationships_table} 
            WHERE start_label = '{start_label}' AND end_label = '{end_label}'
        """)
        
        start_table = node_labels[start_label]
        end_table = node_labels[end_label]
        
        edge_defs.append(f"""
            {rel_subname}
            SOURCE KEY (node_id_start) REFERENCES {start_table} (node_id)
            DESTINATION KEY (node_id_end) REFERENCES {end_table} (node_id)
            LABEL related_to_{start_label}_{end_label}
        """)

    ddl = f"""
        CREATE OR REPLACE PROPERTY GRAPH icij_graph
        VERTEX TABLES (
            {', '.join(vertex_defs)}
        )
        EDGE TABLES (
            {', '.join(edge_defs)}
        )
    """
    
    # print(ddl) # Debug
    conn.execute(ddl)
    
    return conn

if __name__ == "__main__":
    # Test run
    con = load_data()
    print("Data loaded and Graph created successfully.")
