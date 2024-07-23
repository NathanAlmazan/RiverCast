import os
import psycopg2


class DbConnection:
    _instance = None
    _connection_pool = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(DbConnection, cls).__new__(cls, *args, **kwargs)
            cls._initialize_connection_pool()
        return cls._instance

    @classmethod
    def _initialize_connection_pool(cls):
        try:
            cls._connection_pool = psycopg2.connect(
                user=os.environ.get("DB_USER"),
                password=os.environ.get("DB_PASS"),
                host=os.environ.get("DB_HOST"),
                port=os.environ.get("DB_PORT"),
                database=os.environ.get("DB_NAME")
            )
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error while connecting to database: {error}")
            cls._connection_pool = None

    def get_connection(self):
        if self._connection_pool:
            return self._connection_pool

    def release_connection(self, connection):
        if self._connection_pool:
            self._connection_pool.putconn(connection)

    def close_all_connections(self):
        if self._connection_pool:
            self._connection_pool.closeall()
