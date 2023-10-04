import pytest
from etl_2 import connect_to_postgres

# тестовые параметры
TEST_DB_PARAMS = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5433',
}

# тестирование подключения
def test_connect_to_postgres():
    # Test правильного подключения
    conn = connect_to_postgres(TEST_DB_PARAMS)
    assert conn is not None
    conn.close()

    # Test неправильного подключения
    invalid_db_params = {
        'database': 'invalid_database',
        'user': 'invalid_user',
        'password': 'invalid_password',
        'host': 'invalid_host',
        'port': 'invalid_port',
    }
    conn = connect_to_postgres(invalid_db_params)
    assert conn is None

# Run
if __name__ == '__main__':
    pytest.main()