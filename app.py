from flask import Flask
from datetime import datetime
from flask_apscheduler import APScheduler
from pipelines import update_weather, update_rainfall, update_river_level

# initialize application
app = Flask(__name__)

# initialize scheduler
scheduler = APScheduler()
scheduler.api_enabled = True
scheduler.init_app(app)
scheduler.start()


@app.route('/')
def home_page():  # put application's code here
    return 'RiverCast!'


@scheduler.task('interval', id='pipeline_updates', seconds=1800, misfire_grace_time=900)
def pipeline_updates():
    # update pipelines
    update_weather()
    update_rainfall()
    update_river_level()
    # update log
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Pipelines updated at {timestamp}.")


if __name__ == '__main__':
    app.run()
