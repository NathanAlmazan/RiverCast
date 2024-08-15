import os
import psycopg2


def save_many(query: str, values: list[any]):
    try:
        with psycopg2.connect(
                user=os.environ.get("DB_USER"),
                password=os.environ.get("DB_PASS"),
                host=os.environ.get("DB_HOST"),
                port=os.environ.get("DB_PORT"),
                database=os.environ.get("DB_NAME")
        ) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(query, values)
                connection.commit()
    except psycopg2.DatabaseError as error:
        print(f"Error while executing SQL: {error}")
    except Exception as error:
        print(f'Other error occurred: {error}')


def fetch_many(query: str, values=None) -> list[any]:
    try:
        with psycopg2.connect(
                user="postgres",
                password="n4tj0yc3",
                host="localhost",
                port=5432,
                database="rivercast"
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, values)
                return cursor.fetchall()
    except psycopg2.DatabaseError as error:
        print(f"Error while executing SQL: {error}")
    except Exception as error:
        print(f'Other error occurred: {error}')
