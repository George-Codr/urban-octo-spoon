import psycopg

def test_connection():
    """Test basic connection and simple query"""
    conn = psycopg.connect(
        "host=localhost dbname=testdb user=postgres password=postgres"
    )
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    result = cur.fetchone()
    conn.close()
    assert result[0] == 1

def test_insert_select():
    """Test creating table, inserting, selecting"""
    conn = psycopg.connect(
        "host=localhost dbname=testdb user=postgres password=postgres"
    )
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS test_table;")
    cur.execute("CREATE TABLE test_table(id SERIAL PRIMARY KEY, name TEXT);")
    cur.execute("INSERT INTO test_table(name) VALUES ('Alice'), ('Bob');")
    cur.execute("SELECT name FROM test_table ORDER BY id;")
    rows = cur.fetchall()
    conn.close()
    assert rows == [("Alice",), ("Bob",)]

def test_transaction_commit_rollback():
    """Test commit and rollback behavior"""
    conn = psycopg.connect(
        "host=localhost dbname=testdb user=postgres password=postgres"
    )
    cur = conn.cursor()

    # Commit test
    cur.execute("CREATE TABLE IF NOT EXISTS commit_test(id SERIAL);")
    cur.execute("INSERT INTO commit_test DEFAULT VALUES;")
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM commit_test;")
    count_after_commit = cur.fetchone()[0]

    # Rollback test
    cur.execute("INSERT INTO commit_test DEFAULT VALUES;")
    conn.rollback()
    cur.execute("SELECT COUNT(*) FROM commit_test;")
    count_after_rollback = cur.fetchone()[0]

    conn.close()
    assert count_after_commit == count_after_rollback

def test_parameterized_query():
    """Test parameterized queries"""
    conn = psycopg.connect(
        "host=localhost dbname=testdb user=postgres password=postgres"
    )
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS param_test(name TEXT);")
    cur.execute("INSERT INTO param_test(name) VALUES (%s)", ("Charlie",))
    cur.execute("SELECT name FROM param_test WHERE name=%s", ("Charlie",))
    result = cur.fetchone()
    conn.close()
    assert result[0] == "Charlie"

def test_context_manager():
    """Test connection and cursor context managers"""
    with psycopg.connect(
        "host=localhost dbname=testdb user=postgres password=postgres"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 42;")
            result = cur.fetchone()
    assert result[0] == 42
