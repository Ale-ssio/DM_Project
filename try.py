import psycopg2

try:
    conn = psycopg2.connect(
        dbname='Reconciled_Data_Layer',
        user='postgres',
        password='biar',
        host='localhost',
        port='5432'
    )
    print("✅ PostgreSQL connection successful!")
    conn.close()
except Exception as e:
    print(f"❌ Connection failed: {e}")
