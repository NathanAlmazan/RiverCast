import requests

from requests.exceptions import HTTPError
from pipelines.db_connection import save_many
from pipelines.utils import format_timestamp, format_float


def update():
    try:
        # get data from API
        response = requests.get("https://pasig-marikina-tullahanffws.pagasa.dost.gov.ph/water/main_list.do")
        response.raise_for_status()
        # build INSERT query
        query = f"INSERT INTO public.historical_river (station, river_level, recorded_at) VALUES (%s, %s, %s)"
        values = [(data['obsnm'], format_float(data['wl']), format_timestamp(data['timestr']))
                  for data in response.json()]
        # save data
        save_many(query, values)

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except ValueError as error:
        print(f'Failed to parse value: {error}')
    except Exception as error:
        print(f'Other error occurred: {error}')
