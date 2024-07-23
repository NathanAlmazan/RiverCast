import psycopg2
import requests

from requests.exceptions import HTTPError
from pipelines.DbConnection import DbConnection
from pipelines.utils import format_timestamp, format_float


class RiverLevel(DbConnection):

    def fetch(self):
        pass

    def update(self):
        # initialize db connection
        connection = self.get_connection()
        cursor = connection.cursor()

        try:
            # get data from API
            response = requests.get("https://pasig-marikina-tullahanffws.pagasa.dost.gov.ph/water/main_list.do")
            response.raise_for_status()
            # build INSERT query
            query = f"INSERT INTO public.historical_river (station, river_level, recorded_at) VALUES (%s, %s, %s)"
            values = [(data['obsnm'], format_float(data['wl']), format_timestamp(data['timestr']))
                      for data in response.json()]
            # save data
            cursor.executemany(query, values)
            connection.commit()
            # connection pool clean up
            cursor.close()

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except ValueError as error:
            print(f'Failed to parse value: {error}')
        except (Exception, psycopg2.DatabaseError) as error:
            # Rollback the transaction in case of an error
            connection.rollback()
            # error logs
            print(f"Error while executing SQL: {error}")
        except Exception as error:
            print(f'Other error occurred: {error}')
