import duckdb
import os
from typing import Dict, List, Any

def process_data(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process CSV data:
    1. Convert CSV to Parquet (if needed/configured).
    2. Enrich relationships with labels.
    
    Returns a modified config dict with paths pointing to the processed parquet files.
    """
    print("Processing data...")
    conn = duckdb.connect(":memory:")
    
    processed_sources = []
    
    # Track labels for enrichment
    node_labels = {}
    
    # 1. Convert Nodes CSV -> Parquet
    for source in config.get("sources", []):
        table_name = source["table"]
        file_path = source["path"]
        label = source.get("label")
        
        if table_name == "relationships":
            # Will handle last
            continue

        if file_path.endswith(".csv"):
            parquet_path = file_path.replace(".csv", ".parquet")
            # In a real app, check timestamps/hashes to avoid re-processing.
            # Here we just overwrite or check existence.
            # Let's simple overwrite for correctness on each run per user request intent (auto-process).
            print(f"Converting {file_path} to {parquet_path}...")
            conn.execute(f"CREATE OR REPLACE TABLE raw_{table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
            conn.execute(f"COPY raw_{table_name} TO '{parquet_path}' (FORMAT PARQUET)")
            
            # Update source to point to parquet
            source_copy = source.copy()
            source_copy["path"] = parquet_path
            processed_sources.append(source_copy)
            
            if label:
                node_labels[label] = parquet_path
                # Keep table in memory map for enrichment query if needed?
                # Actually we can just read parquet again.
        else:
            # Already parquet or other? Just pass through
            processed_sources.append(source)
            if label:
                node_labels[label] = file_path

    # 2. Process Relationships
    # Find the relationships source config
    rel_source = next((s for s in config.get("sources", []) if s["table"] == "relationships"), None)
    if rel_source:
        rel_path = rel_source["path"]
        typed_rel_path = rel_path.replace(".csv", "-typed.parquet").replace(".parquet", "-typed.parquet") 
        # If it was .csv, we want to go straight to -typed.parquet
        
        print(f"Enriching relationships from {rel_path} to {typed_rel_path}...")
        
        # Load Raw Relationships
        if rel_path.endswith(".csv"):
            conn.execute(f"CREATE OR REPLACE TABLE relationships_raw AS SELECT * FROM read_csv_auto('{rel_path}')")
        else:
            conn.execute(f"CREATE OR REPLACE TABLE relationships_raw AS SELECT * FROM '{rel_path}'")

        # Create Mapping Table
        union_parts = []
        for label, path in node_labels.items():
            # We can read directly from the just-created Parquet files
            union_parts.append(f"SELECT node_id, '{label}' as label FROM '{path}'")
        
        if union_parts:
            create_map_query = f"CREATE OR REPLACE TABLE nodes_map AS {' UNION ALL '.join(union_parts)}"
            conn.execute(create_map_query)
            
            # Enrich
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
            
            # Save
            conn.execute(f"COPY relationships_typed TO '{typed_rel_path}' (FORMAT PARQUET)")
            
            # Add to processed sources
            rel_source_copy = rel_source.copy()
            rel_source_copy["path"] = typed_rel_path
            processed_sources.append(rel_source_copy)
        
        else:
            print("Warning: No node labels found, cannot enrich relationships.")
            processed_sources.append(rel_source)

    # Return new config structure
    return {"sources": processed_sources}
