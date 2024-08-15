from flask import Flask, jsonify
from datetime import datetime
from flask_apscheduler import APScheduler
from models import generate_forecast
from pipelines import update_weather, update_rainfall, update_river_level
from pipelines.db_connection import fetch_many

# initialize application
app = Flask(__name__)

# initialize scheduler
scheduler = APScheduler()
scheduler.api_enabled = True
scheduler.init_app(app)
scheduler.start()


@app.route('/current')
def current_data():  # put application's code here
    query = """
        SELECT * FROM public.historical_river
        WHERE station = 'Sto Nino'
        ORDER BY recorded_at DESC LIMIT 5
    """
    results = fetch_many(query)

    return jsonify(results), 200


@app.route('/forecast')
def forecast_data():  # put application's code here
    query = """
            SELECT 
                DATE_TRUNC('hour', forecast_for) AS hour,
                AVG(river_level) AS forecast
            FROM public.forecast 
            WHERE forecast_for >= NOW()
            GROUP BY hour
            ORDER BY hour
        """
    results = fetch_many(query)

    return jsonify(results), 200


@scheduler.task('interval', id='pipeline_updates', seconds=10, misfire_grace_time=900)
def pipeline_updates():
    # update pipelines
    update_weather()
    update_rainfall()
    update_river_level()
    # generate forecast
    generate_forecast()
    # update log
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Pipelines updated at {timestamp}.")


if __name__ == '__main__':
    app.run()
