from flask import Flask
from datetime import datetime
from flask_apscheduler import APScheduler
from pipelines import Weather, Rainfall, RiverLevel

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
    # initialize pipelines
    river = RiverLevel()
    weather = Weather()
    rainfall = Rainfall()
    # update pipelines
    river.update()
    weather.update()
    rainfall.update()
    # update log
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Pipelines updated at {timestamp}.")


if __name__ == '__main__':
    app.run()
