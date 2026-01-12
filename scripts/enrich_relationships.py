import duckdb
import yaml
import os
import sys

# Add workspace root to python path to allow importing src if needed
sys.path.append(os.getcwd())

def load_config(config_path: str = "config/icij.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def enrich_relationships():
    print("Starting relationships enrichment...")
    config = load_config()
    conn = duckdb.connect(":memory:")
    
    node_labels = {}
    relationships_path = ""
    
    # 1. Load Tables
    for source in config.get("sources", []):
        table_name = source["table"]
        file_path = source["path"]
        label = source.get("label")
        
        if table_name == "relationships":
            relationships_path = file_path
            continue
            
        print(f"Reading {table_name} from {file_path}...")
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM '{file_path}'")
        
        if label:
            node_labels[label] = table_name

    if not relationships_path:
        print("Error: relationships table not found in config.")
        return

    # 2. Create mapping table (NodeID -> Label)
    print("Creating mapping table...")
    union_parts = []
    for label, table in node_labels.items():
        union_parts.append(f"SELECT node_id, '{label}' as label FROM {table}")
    
    create_map_query = f"CREATE OR REPLACE TABLE nodes_map AS {' UNION ALL '.join(union_parts)}"
    conn.execute(create_map_query)
    
    # 3. Enrich relationships
    print(f"Enriching relationships from {relationships_path}...")
    conn.execute(f"CREATE OR REPLACE TABLE relationships_raw AS SELECT * FROM '{relationships_path}'")
    
    enrich_query = """
        CREATE OR REPLACE TABLE relationships_typed AS 
        SELECT 
            r.*,
            s.label as start_label,
            e.label as end_label
        FROM relationships_raw r
        JOIN nodes_map s ON r.node_id_start = s.node_id
        JOIN nodes_map e ON r.node_id_end = e.node_id
    """
    conn.execute(enrich_query)
    
    # 4. Save to Parquet
    output_path = "data/relationships-typed.parquet"
    print(f"Saving to {output_path}...")
    conn.execute(f"COPY relationships_typed TO '{output_path}' (FORMAT PARQUET)")
    
    print("Enrichment complete.")

if __name__ == "__main__":
    enrich_relationships()
