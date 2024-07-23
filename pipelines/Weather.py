import os
import psycopg2
import requests

from datetime import datetime
from requests.exceptions import HTTPError
from pipelines.DbConnection import DbConnection


class Weather(DbConnection):

    def fetch(self):
        pass

    def update(self):
        # initialize db connection
        connection = self.get_connection()
        cursor = connection.cursor()

        app_key = os.environ.get("WEATHER_KEY")
        values = []
        stations = [
            (14.733470948631977, 121.13029610938858),   # Rodriguez
            (14.696594811450586, 121.11598773076278),   # San Mateo
            (14.636047609437965, 121.09319749785566),   # Sto. Nino
            (14.590160177521340, 121.09268111172379)    # Rosario
        ]

        try:
            # build INSERT query
            for station in stations:
                lat, lon = station
                # get data from API
                response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?" +
                                        f"lat={lat}&lon={lon}&appid={app_key}&units=metric")
                response.raise_for_status()
                response = response.json()

                values.append((
                    response['name'],                                                           # station
                    datetime.fromtimestamp(response['dt']).strftime("%Y-%m-%d %H:%M:%S+08"),    # recorded_at
                    response['rain']['1h'] if 'rain' in response else None,                     # rainfall
                    response['wind']['speed'] if 'wind' in response else None,                  # wind speed
                    response['main']['temp'],                                                   # temperature
                    response['main']['humidity']                                                # humidity
                ))

            query = f"""
                INSERT INTO public.historical_weather(station, recorded_at, rainfall, wind_speed, temperature, humidity) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
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
