import duckdb
import glob
import os

def parquet_to_csv():
    conn = duckdb.connect(":memory:")
    files = glob.glob("data/*.parquet")
    
    for f in files:
        if "relationships-typed" in f:
            continue
            
        base_name = os.path.basename(f).replace(".parquet", "")
        csv_path = f"data/{base_name}.csv"
        print(f"Converting {f} to {csv_path}...")
        
        conn.execute(f"COPY (SELECT * FROM '{f}') TO '{csv_path}' (HEADER, DELIMITER ',')")

if __name__ == "__main__":
    parquet_to_csv()
