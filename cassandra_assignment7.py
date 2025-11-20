# cassandra_assignment7.py 
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
import csv
import sys
import time
import os

# --- 1. CONFIGURATION ---
# (From your assignment7_db-token.json)
CLIENT_ID = "BSlwRDQDYqEdbIpQKaMNIbJL"
CLIENT_SECRET = "YbsJ8exWvEcNgleXNPPFS8oN99NYmZdCOuh3D_D79ZQ.Y0NCGv6.pD1iytikMs-hIl6za+iJ_BsnBPoEKLqPu.UxoldkBbbeAH6GUrIIyBLLu8MIz5gSUMCsq.XK-AbE"

# (From your config.json)
KEYSPACE = "assignment7_keyspace"
SECURE_BUNDLE = "secure-connect-assignment7-db.zip"

# (The CSV file in folder - 'customers.csv') 
CSV_FILE = "customers.csv" 


class CassandraDB():

    def __init__(self):
        self.cluster = None
        self.session = None
        print("Connecting to Astra DB...")

    def connect(self):
        if not os.path.exists(SECURE_BUNDLE):
            print(f"ERROR: Secure bundle file not found at: {SECURE_BUNDLE}")
            sys.exit(1)
            
        cloud_config = {
            'secure_connect_bundle': SECURE_BUNDLE
        }
        auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
        self.cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        
        try:
            self.session = self.cluster.connect()
            self.session.set_keyspace(KEYSPACE)
            self.session.default_timeout = 30.0 # Set longer timeout for stability
            print(f"Connection successful. Using keyspace: {KEYSPACE}")
        except Exception as e:
            print(f"Failed to connect: {e}")
            sys.exit(1)

    def create_table(self):
        # This schema matches the 6-column CSV file screenshot (image_a4e49f.png)
        cql_create_table = """
        CREATE TABLE IF NOT EXISTS "Customer" (
            "id" text PRIMARY KEY,
            "gender" text,
            "age" int,
            "fname" text,
            "lname" text,
            "number_of_kids" text
        );
        """
        cql_create_gender_index = 'CREATE INDEX IF NOT EXISTS ON "Customer" ("gender");'
        cql_create_age_index = 'CREATE INDEX IF NOT EXISTS ON "Customer" ("age");'

        try:
            print("Creating table 'Customer' (if not exists)...")
            self.session.execute(cql_create_table)
            self.session.execute(cql_create_gender_index)
            self.session.execute(cql_create_age_index)
            print("Table and indexes are ready.")
        except Exception as e:
            print(f"Error creating table: {e}")
            sys.exit(1)

    def load_data(self):
        print(f"Loading data from {CSV_FILE}...")
        
        if not os.path.exists(CSV_FILE):
            print(f"ERROR: CSV file not found at: {CSV_FILE}")
            sys.exit(1)

        with open(CSV_FILE, newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader) # Skip header row
            
            # --- This handles your "dirty" CSV by checking row length ---
            
            # We prepare two different queries
            cql_full = 'INSERT INTO "Customer" ("id", "gender", "age", "fname", "lname", "number_of_kids") VALUES (?, ?, ?, ?, ?, ?)'
            cql_partial = 'INSERT INTO "Customer" ("id", "gender", "age", "number_of_kids") VALUES (?, ?, ?, ?)'
            
            prepared_full = self.session.prepare(cql_full)
            prepared_partial = self.session.prepare(cql_partial)

            batch = BatchStatement()
            count = 0
            rows_in_batch = 0
            skipped_rows = 0

            for row in reader:
                try:
                    age = int(row[2]) if row[2] else None
                    
                    # If row has 6 columns, use the full insert
                    if len(row) == 6:
                        batch.add(prepared_full, (row[0], row[1], age, row[3], row[4], row[5]))
                    # If row has 4 columns (like the start of your file), use the partial insert
                    elif len(row) == 4:
                        batch.add(prepared_partial, (row[0], row[1], age, row[3]))
                    # Otherwise, skip it
                    else:
                        print(f"Skipping malformed row: {row}")
                        skipped_rows += 1
                        continue

                    rows_in_batch += 1
                    count += 1

                    # Execute batch every 100 rows
                    if rows_in_batch >= 100:
                        self.session.execute(batch)
                        batch.clear()
                        rows_in_batch = 0
                        
                except Exception as e:
                    print(f"Error processing row {row}: {e}")
            
            # Execute final batch
            if rows_in_batch > 0:
                self.session.execute(batch)
            
            print(f"Successfully loaded {count} rows. (Skipped {skipped_rows} malformed rows).")

    def query_1(self):
        print("\n--- Running Query 1 ---")
        cql_query = "SELECT \"age\" FROM \"Customer\" WHERE \"id\" = '979863'"
        
        try:
            row = self.session.execute(cql_query).one()
            if row:
                print(f"Query 1 result: Age of customer 979863 is {row.age}")
                return row.age
            else:
                print("Query 1 result: Customer 979863 not found.")
        except Exception as e:
            print(f"Query 1 failed: {e}")

    def query_2(self):
        print("\n--- Running Query 2 ---")
        cql_query = "SELECT * FROM \"Customer\" WHERE \"gender\" = 'MALE' AND \"age\" IN (25, 35) ALLOW FILTERING"
        
        try:
            rows = self.session.execute(cql_query)
            results = list(rows)
            print(f"Query 2 returned {len(results)} rows.")
            
            for r in results:
                # This line now handles the 'None' names gracefully
                print(f"  - ID={r.id}, Name={r.fname or 'N/A'} {r.lname or 'N/A'}, Age={r.age}, Gender={r.gender}")
            return results
        except Exception as e:
            print(f"Query 2 failed: {e}")
            return []

    # --- Helper methods for Insert/Delete screenshots ---

    def insert_demo(self):
        print("\n--- Running Insert Demo ---")
        # --- ✅ THIS IS THE FIX ---
        # It uses '?' placeholders, not '%s'
        cql = 'INSERT INTO "Customer" ("id", "fname", "lname", "gender", "age") VALUES (?, ?, ?, ?, ?);'
        try:
            # Use a string '99999999' for the ID
            self.session.execute(cql, ('99999999', "Demo", "User", "MALE", 35))
            print("Inserted demo record id='99999999'")
        except Exception as e:
            print(f"Insert demo failed: {e}")

    def delete_demo(self):
        print("\n--- Running Delete Demo ---")
        cql = "DELETE FROM \"Customer\" WHERE \"id\" = '99999999';"
        try:
            self.session.execute(cql)
            print("Deleted demo record id='99999999'")
        except Exception as e:
            print(f"Delete demo failed: {e}")

    def close(self):
        try:
            if self.cluster:
                self.cluster.shutdown()
                print("\nCluster shutdown completed.")
        except Exception as e:
            print(f"Error shutting down cluster: {e}")


# --- This is where the script starts ---
if __name__ == "__main__":
    client = CassandraDB()
    try:
        client.connect()
        client.create_table()
        client.load_data()
        
        # Run assignment queries
        client.query_1()
        client.query_2()
        
        # Run demo insert/delete for screenshots
        client.insert_demo()
        time.sleep(1) # Give a moment for insert
        
        try:
            res = client.session.execute('SELECT * FROM "Customer" WHERE "id" = \'99999999\';')
            print("Verify inserted demo:", list(res))
        except Exception as e:
            print(f"Verify select failed: {e}")
            
        client.delete_demo()
        time.sleep(1) # Give a moment for delete
        
        try:
            res = client.session.execute('SELECT * FROM "Customer" WHERE "id" = \'99999999\';')
            print("Verify deleted demo:", list(res))
        except Exception as e:
            print(f"Verify select after delete failed: {e}")
            
    finally:
        # Always close the connection
        client.close()