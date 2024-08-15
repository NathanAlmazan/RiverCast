import os
import joblib
import requests
import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from requests.exceptions import HTTPError
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from pipelines.db_connection import fetch_many, save_many


def forecast():
    try:
        # get forecast data from API
        lat = 14.636047609437965
        lon = 121.09319749785566
        app_key = os.environ.get("FORECAST_KEY")

        response = requests.get(f"https://api.openweathermap.org/data/3.0/onecall?" +
                                f"lat={lat}&lon={lon}&exclude=current,minutely&appid={app_key}")
        response.raise_for_status()
        results = response.json()

        forecasts = [{
            "forecast_for": datetime.fromtimestamp(data['dt']).strftime("%Y-%m-%d %H:%M:%S+08"),
            "temperature": float(data['temp']),
            "humidity": float(data['humidity']),
            "wind_speed": float(data['wind_speed']),
            "rainfall": float(data['rain']['1h']) if 'rain' in data else 0.0
        } for data in results['hourly']]

        df = pd.DataFrame(forecasts)
        rain_forecast = np.array([data['rainfall'] for data in forecasts], dtype=np.float32)

        end = datetime.fromisoformat(forecasts[0]['forecast_for'])
        start = end - timedelta(hours=11)

        end = end.strftime("%Y-%m-%d %H:%M:%S")
        start = start.strftime("%Y-%m-%d %H:%M:%S")

        # get historical data
        query = """
            SELECT 
                DATE_TRUNC('hour', recorded_at) AS hour,
                MAX(rainfall) AS max_value
            FROM public.historical_rainfall 
            WHERE station = 'Marikina (Youth Camp)' 
                AND recorded_at >= %s 
                AND recorded_at < %s
            GROUP BY hour
            ORDER BY hour
        """

        history = fetch_many(query, [start, end])
        rain_history = np.array([data for (_, data) in history], dtype=np.float32)

        if len(rain_history) < 11:
            size = 11 - len(rain_history)
            rain_history = np.pad(rain_history, (size, 0), mode='constant', constant_values=0)
        elif len(rain_history) > 11:
            rain_history = rain_history[-11:]

        base_dir = os.path.abspath(os.path.dirname(__file__))

        # forecast river level
        inputs = np.concatenate([rain_history, rain_forecast], dtype=np.float32)
        # scale input
        scaler: StandardScaler = joblib.load(os.path.join(base_dir, 'checkpoints', 'input_scaler.joblib'))
        inputs = scaler.transform(np.reshape(inputs, (-1, 1)))
        # format windows
        inputs = np.lib.stride_tricks.sliding_window_view(np.reshape(inputs, (-1,)), window_shape=12, writeable=True)
        # forecast river level
        model: RandomForestRegressor = joblib.load(os.path.join(base_dir, 'checkpoints', 'forecaster.joblib'))
        predictions = model.predict(inputs)
        # de-normalize river level
        scaler: StandardScaler = joblib.load(os.path.join(base_dir, 'checkpoints', 'label_scaler.joblib'))
        predictions = scaler.inverse_transform(np.reshape(predictions, (-1, 1)))

        # save forecasts
        df['river_level'] = np.reshape(predictions, (-1,))
        values = df.values.tolist()

        query = f"""
            INSERT INTO public.forecast(forecast_for, temperature, humidity, wind_speed, rainfall, river_level) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        save_many(query, values)
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except ValueError as error:
        print(f'Failed to parse value: {error}')
    except Exception as error:
        print(f'Other error occurred: {error}')
