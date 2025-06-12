from db_config import get_connection, release_connection

def test_connection():
    try:
        # Try to get a connection
        conn = get_connection()
        print("Successfully connected to the database")
        
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print(f"PostgreSQL version: {version[0]}")
            
        # Release the connection
        release_connection(conn)
        return True
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return False

if __name__ == "__main__":
    test_connection() 