import duckdb
import yaml
import os
import importlib
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
    
    # Generic Processor Handler
    # If a processor is defined in config, dynamically import and execute it.
    processed_config = config
    if "processor" in config:
        processor_name = config["processor"]
        try:
            print(f"Invoking processor: {processor_name}...")
            # Assuming processor module has a function process_data taking config dict
            module = importlib.import_module(processor_name)
            processed_config = module.process_data(config)
        except Exception as e:
            print(f"Error executing processor {processor_name}: {e}")
            raise e
    
    if conn is None:
        conn = get_connection()
    
    # Enable DuckPGQ extensions
    print("Enabling DuckPGQ extension...")
    conn.execute("INSTALL duckpgq FROM community")
    conn.execute("LOAD duckpgq")

    node_labels = {}
    relationships_table = ""
    
    # 1. Load Tables from Config (Generic)
    for source in processed_config.get("sources", []):
        table_name = source["table"]
        file_path = source["path"]
        label = source.get("label")
        
        print(f"Loader: Loading {table_name} from {file_path}...")
        
        # Consistent approach: Create TEMP TABLE for everything
        conn.execute(f"CREATE OR REPLACE TEMP TABLE {table_name} AS SELECT * FROM '{file_path}'")
        
        if label:
            node_labels[label] = table_name
        # Heuristic: if no label and table name suggests relationships, mark it
        elif table_name == "relationships":
            relationships_table = table_name

    # 2. Dynamic Property Graph Definition
    # This logic assumes a Star Schema / Property Graph where edges connect labeled nodes
    if relationships_table and node_labels:
        print("Loader: Defining Property Graph...")
        
        # Vertex Tables
        vertex_defs = []
        for label, table in node_labels.items():
            vertex_defs.append(f"{table} LABEL {label}")
        
        # Edge Tables
        # Attempt to detect edge types from data if present (start_label, end_label columns)
        # This part depends on the processor having enriched the data.
        # If columns don't exist, we might fallback to generic edge?
        # For now, we assume the Enrichment contract: relationships table has start_label/end_label.
        
        try:
            combinations = conn.execute(f"""
                SELECT DISTINCT start_label, end_label FROM {relationships_table}
            """).fetchall()
            
            edge_defs = []
            for start_label, end_label in combinations:
                rel_subname = f"rel_{start_label}_{end_label}"
                
                # Materialize sub-tables (TEMP) for DuckPGQ compliance
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
            
            if edge_defs:
                ddl = f"""
                    CREATE OR REPLACE PROPERTY GRAPH icij_graph
                    VERTEX TABLES (
                        {', '.join(vertex_defs)}
                    )
                    EDGE TABLES (
                        {', '.join(edge_defs)}
                    )
                """
                conn.execute(ddl)
                print("Loader: Graph 'icij_graph' created successfully.")
                
        except Exception as e:
            print(f"Loader Warning: Could not define dynamic edges based on labels. Graph might be incomplete. Error: {e}")

    return conn

if __name__ == "__main__":
    # Test run
    con = load_data()
    print("Data loaded and Graph created successfully.")
