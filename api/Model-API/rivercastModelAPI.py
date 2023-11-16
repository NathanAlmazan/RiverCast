from flask import Flask, jsonify, send_file
import io
from rivercastModel import forecast, sendCleanData, sendrawData
import matplotlib
matplotlib.use('Agg')  # Use a non-GUI backend
import matplotlib.pyplot as plt
import requests
import csv
from datetime import datetime, timedelta
from datetime import date
import pymysql
import mysql.connector
from sqlalchemy import create_engine



app = Flask(__name__)


mydb = mysql.connector.connect(
  host="database-1.cccp1zhjxtzi.ap-southeast-1.rds.amazonaws.com",
  user="admin",
  password="Nath1234",
  database= "rivercast"
)

engine = create_engine("mysql+pymysql://" + "admin" + ":" + "Nath1234" + "@" + "database-1.cccp1zhjxtzi.ap-southeast-1.rds.amazonaws.com" + "/" + "rivercast")

cursor = mydb.cursor()

print(mydb)

# Function to generate and save the plot
def save_plot(data, filename):
    plt.plot(data)
    image_stream = io.BytesIO()
    plt.savefig(image_stream, format='png')
    image_stream.seek(0)
    plt.clf()  # Clear the plot for the next use
    return image_stream

# Endpoint for prediction forecast
@app.route('/predict', methods=['GET'])
def predict():
    forecast_df = forecast()
    json_forecast = forecast_df.to_json(orient='split', date_format='iso')
    return jsonify(json_forecast)

# Endpoint for raw data plot
@app.route('/raw_data_plot', methods=['GET'])
def raw_data_plot():
    rd = sendrawData()
    image_stream = save_plot(rd, 'raw_data_plot.png')
    return send_file(image_stream, mimetype='image/png')

# Endpoint for clean data plot
@app.route('/clean_data_plot', methods=['GET'])
def clean_data_plot():
    cd = sendCleanData()
    image_stream = save_plot(cd, 'clean_data_plot.png')
    return send_file(image_stream, mimetype='image/png')

# Endpoint for clean data plot
@app.route('/addPredictionToDB', methods=['GET'])
def addPrediction():

    df1 = forecast()[0]

    df1.to_sql(name='rivercast_waterlevel_prediction', con=engine, index = False, if_exists='append')

    df2 = forecast()[1]

    df2.to_sql(name='rivercast_waterlevel_obs', con=engine, index = False, if_exists='append')

    return jsonify("Add Prediction Values to DB initiated")
    
@app.route('/addTrueValuesToDB', methods=['GET'])
def addTrueValues():

    df2 = forecast()[1]

    df2.to_sql(name='rivercast_waterlevel_obs', con=engine, index = False, if_exists='append')

    return jsonify("Add True Values to DB initiated")


@app.route('/updateModelData', methods=['GET'])
def updateModelData():
    d = datetime.today()
    h = d.hour
    m = d.minute
    s = d.second
    ms = d.microsecond

    datetoday = d - timedelta(hours=h, minutes=m, seconds=s, microseconds=ms)

    startDate = datetoday
    endDate = datetime.today()
    weatherbit = f'https://api.weatherbit.io/v2.0/history/hourly?lat=14.679696901082357&lon=121.10970052493437&start_date={startDate}&end_date={endDate}&tz=local&key=2b382660ad4843188647514206bf330e'


    


    return jsonify(datetoday)






if __name__ == '__main__':
    app.run(debug=True)


