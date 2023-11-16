from fastapi import FastAPI
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
from fastapi.responses import JSONResponse
import mysql.connector
import re

app = FastAPI()


mydb = mysql.connector.connect(
  host="database-1.cccp1zhjxtzi.ap-southeast-1.rds.amazonaws.com",
  user="admin",
  password="Nath1234",
  database= "rivercast"
)

cursor = mydb.cursor()

print(mydb)


api_key = '64c4e1ad007831acf4e8312e26913ccf' #API KEY

#STO. NINO LATITUDE AND LONGITUDE
sn_lat = '14.63599506157467'
sn_lon = '121.09307648574968'

#SAN MATEO LATITUDE AND LONGITUDE
sm_lat = '14.679513550799184'
sm_lon = '121.10969674629524'

#TUMANA LATITUDE AND LONGITUDE
tm_lat = '14.656428471192843'
tm_lon = '121.09630822942425'

#CURRENT WEATHER URLs SN(STO.NINO) SM(SAN MATEO) TM(TUMANA)
sn_current_url = f'https://api.openweathermap.org/data/2.5/weather?lat={sn_lat}&lon={sn_lon}&appid={api_key}'
sm_current_url = f'https://api.openweathermap.org/data/2.5/weather?lat={sm_lat}&lon={sm_lon}&appid={api_key}'
tm_current_url = f'https://api.openweathermap.org/data/2.5/weather?lat={tm_lat}&lon={tm_lon}&appid={api_key}'


sto_nino_waterlevel = []
san_mateo_waterlevel = []
tumana_waterlevel = []

sn_current_weather = []
sm_current_weather = []
tm_current_weather = []


#function for ibm forecast
def get_IBM_forecast_weather(location, url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    detail = soup.find('details', {'id': 'detailIndex0'})
    
    time = detail.find('h3', {'data-testid': 'daypartName'}).text
    temp1 = detail.find('span', {'data-testid': 'TemperatureValue'}).text.strip('(°)')
    humi4 = detail.find('li', {'data-testid': 'HumiditySection'}).find('span', {'data-testid': 'PercentageValue'}).text.strip('(%)')
    preci3 = detail.find('span', {'data-testid': 'AccumulationValue'}).text.replace(' mm', '').replace(' cm', '')
    
    dnow = datetime.now()
    strD = dnow.strftime("%Y-%m-%d") + " " + time + ":00"
    
    date_format = '%Y-%m-%d %H:%M:%S'
    dateobh = datetime.strptime(strD, date_format)
    
    return {
        "location": location,
        "date_time": dateobh.strftime('%Y-%m-%d %H:%M:%S'),
        "humidity": humi4,
        "temperature": temp1,
        "precipitation": preci3
    }

#get all locations and links for ibm forecasting
IBM_locations = {
    "Sto. Nino": "https://weather.com/en-PH/weather/hourbyhour/l/14.63599506157467,121.09307648574968",
    "San Mateo": "https://weather.com/en-PH/weather/hourbyhour/l/14.679513550799184,121.10969674629524",
    "Tumana": "https://weather.com/en-PH/weather/hourbyhour/l/14.656428471192843,121.09630822942425"
}

#initialize the items container for ibm forecasted weather
IBM_forecasted_weather = []

def get_IBM_forecast():
    IBM_forecasted_weather.clear()
    #get all locations and links for ibm forecasting
    for location, url in IBM_locations.items():
        location_weather = get_IBM_forecast_weather(location, url)
        IBM_forecasted_weather.append(location_weather)


def get_OW_forecast_weather(location, url):
    ow_forecast_req = requests.get(url)
    ow_forecast_res = ow_forecast_req.json()

    location_weather_data = []  # Create a list to store weather data for the location

    for forecast in ow_forecast_res['list']:
        date_time_str = forecast['dt_txt']
        humidity = forecast['main']['humidity']
        temp = forecast['main']['temp']
        preci = forecast.get('rain', {}).get('1h', 0)  # Handle cases where 'rain' or '1h' is missing.

        date_time_obj = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
        new_date_time_obj = date_time_obj + timedelta(hours=8)
        new_date_time_str = new_date_time_obj.strftime('%Y-%m-%d %H:%M:%S')

        kelv_to_celc = round(temp - 273.15, 2)

        # Append the weather data to the location_weather_data list
        location_weather_data.append({
            "location": location,
            "date_time": new_date_time_str,
            "humidity": humidity,
            "temperature": kelv_to_celc,
            "precipitation": preci
        })

    return location_weather_data  # Return the list of weather data for the location


OW_locations = {
    "Sto. Nino": "https://pro.openweathermap.org/data/2.5/forecast/hourly?lat=14.63599506157467&lon=121.09307648574968&cnt=1&appid=64c4e1ad007831acf4e8312e26913ccf",
    "San Mateo": "https://pro.openweathermap.org/data/2.5/forecast/hourly?lat=14.679513550799184&lon=121.10969674629524&cnt=1&appid=64c4e1ad007831acf4e8312e26913ccf",
    "Tumana": "https://pro.openweathermap.org/data/2.5/forecast/hourly?lat=14.656428471192843&lon=121.09630822942425&cnt=1&appid=64c4e1ad007831acf4e8312e26913ccf"
}

forecasted_OW_weather = []

def get_OW_forecast():
    forecasted_OW_weather.clear()
    for location, url in OW_locations.items():
        location_OW_weather = get_OW_forecast_weather(location, url)
        forecasted_OW_weather.extend(location_OW_weather)  # Extend the list with data for the current location



def get_current_weather():
    global db_time_date
    sn_current_weather.clear()
    today = datetime.now() + timedelta(hours=8)
    db_time_date = today 

    sn_response_c = requests.get(sn_current_url)
    sn_req_c = sn_response_c.json()

    sm_response_c = requests.get(sm_current_url)
    sm_req_c = sm_response_c.json()

    tm_response_c = requests.get(tm_current_url)
    tm_req_c = tm_response_c.json()

    for current_weather in [sn_req_c]:
        global sn_humidity, sn_temperature, sn_preci
        sn_current_weather.clear()
        time_date = current_weather['dt']
        humidity = current_weather['main']['humidity']
        temp = current_weather['main']['temp']
        preci = current_weather.get('rain', {}).get('1h', 0)  # Handle cases where 'rain' or '1h' is missing.

        unix_converted = datetime.utcfromtimestamp(time_date).strftime('%Y-%m-%d %H:%M:%S') #convert unix UTC code to readable date and time

        date_time_obj = datetime.strptime(unix_converted, '%Y-%m-%d %H:%M:%S')
        new_date_time_obj = date_time_obj + timedelta(hours=8)
        new_date_time_str = new_date_time_obj.strftime('%Y-%m-%d %H:%M:%S')

        kelv_to_celc = round(temp - 273.15, 2)

        sn_current_weather.append({
            "location": "Sto. Nino",
            "humidity": humidity,
            "date_time": new_date_time_str,
            "temperature": kelv_to_celc,
            "precipitation": preci
        })

        sn_humidity = humidity
        sn_temperature = kelv_to_celc
        sn_preci = preci

        

    for current_weather in [sm_req_c]:
        global sm_humidity, sm_temperature, sm_preci
        sm_current_weather.clear()
        time_date = current_weather['dt']
        humidity = current_weather['main']['humidity']
        temp = current_weather['main']['temp']
        preci = current_weather.get('rain', {}).get('1h', 0)  # Handle cases where 'rain' or '1h' is missing.

        unix_converted = datetime.utcfromtimestamp(time_date).strftime('%Y-%m-%d %H:%M:%S')

        date_time_obj = datetime.strptime(unix_converted, '%Y-%m-%d %H:%M:%S')
        new_date_time_obj = date_time_obj + timedelta(hours=8)
        new_date_time_str = new_date_time_obj.strftime('%Y-%m-%d %H:%M:%S')

        kelv_to_celc = round(temp - 273.15, 2)

        sm_current_weather.append({
            "location": "San Mateo",
            "humidity": humidity,
            "date_time": new_date_time_str, 
            "temperature": kelv_to_celc,
            "precipitation": preci
        })

        sm_humidity = humidity
        sm_temperature = kelv_to_celc
        sm_preci = preci



    for current_weather in [tm_req_c]:
        global tm_humidity, tm_temperature, tm_preci
        tm_current_weather.clear()
        time_date = current_weather['dt']
        humidity = current_weather['main']['humidity']
        temp = current_weather['main']['temp']
        preci = current_weather.get('rain', {}).get('1h', 0)  # Handle cases where 'rain' or '1h' is missing.

        unix_converted = datetime.utcfromtimestamp(time_date).strftime('%Y-%m-%d %H:%M:%S')

        date_time_obj = datetime.strptime(unix_converted, '%Y-%m-%d %H:%M:%S')
        new_date_time_obj = date_time_obj + timedelta(hours=8)
        new_date_time_str = new_date_time_obj.strftime('%Y-%m-%d %H:%M:%S')

        kelv_to_celc = round(temp - 273.15, 2)

        tm_current_weather.append({
            "location": "Tumana",
            "humidity": humidity,
            "date_time": new_date_time_str,
            "temperature": kelv_to_celc,
            "precipitation": preci
        })

        tm_humidity = humidity
        tm_temperature = kelv_to_celc
        tm_preci = preci

def get_water_level():

    sto_nino_waterlevel.clear()
    san_mateo_waterlevel.clear()
    tumana_waterlevel.clear()


    global sn_wl_c, sm_wl_c, tm_wl_c
    get_date_wl = "http://121.58.193.173:8080/water/map.do"
    page = requests.get(get_date_wl)

    soup = BeautifulSoup(page.content, "html.parser")

    # Find the JavaScript code containing the date
    script_code = soup.find('script', text=re.compile(r'function getMapList\(\)\s*{.*\n?.*var ymdhmVal = dateFns\.format\(([^)]+)\);', re.DOTALL))


    date_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})", script_code.text)
    extracted_date = date_match.group(0)
    strDate = str(extracted_date)
    cleanDate = strDate.replace("-", "").replace(" ", "").replace(":", "")

    water_level_JSON = f'http://121.58.193.173:8080/water/map_list.do?ymdhm={cleanDate}'

    wl_JSON_res = requests.get(water_level_JSON)
    wl_req = wl_JSON_res.json()
    wl_values = []

    target_loc = ["Sto Nino", "San Mateo-1", "Tumana Bridge"]

    for item in wl_req:
        loc = item["obsnm"]
        if loc in target_loc:
            wl = item["wl"]
            wl = wl.strip('(*)')
            wl_values.append((loc, wl))

    sto_nino_waterlevel.append({
    "location": wl_values[1][0],
    "water_level": wl_values[1][1],
    })
    san_mateo_waterlevel.append({
    "location": wl_values[0][0],
    "water_level": wl_values[0][1],
    })
    tumana_waterlevel.append({
    "location": wl_values[2][0],
    "water_level": wl_values[2][1],
    })

    sn_wl_c = wl_values[1][1]
    sm_wl_c = wl_values[0][1]
    tm_wl_c = wl_values[2][1]

Nangka_preci = []
MtOro_preci = []
SanMateo_preci = []
MarikinaYouthCamp_preci = []

def get_PAGASA_precipitation_past1HR():

    MarikinaYouthCamp_preci.clear()
    MtOro_preci.clear()
    Nangka_preci.clear()
    SanMateo_preci.clear()

    global myc_rf_1hr, mo_rf_1hr, ngnk_rf_1hr, sm_rf_1hr, pagaasa_dt
    get_date_wl = "http://121.58.193.173:8080/water/map.do"
    page = requests.get(get_date_wl)

    soup = BeautifulSoup(page.content, "html.parser")

    # Find the JavaScript code containing the date
    script_code = soup.find('script', text=re.compile(r'function getMapList\(\)\s*{.*\n?.*var ymdhmVal = dateFns\.format\(([^)]+)\);', re.DOTALL))


    date_match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})", script_code.text)
    extracted_date = date_match.group(0)
    strDate = str(extracted_date)
    cleanDate = strDate.replace("-", "").replace(" ", "").replace(":", "")

    rain_fall_JSON = f'http://121.58.193.173:8080/rainfall/map_list.do?ymdhm={cleanDate}'

    rf1h_JSON_res = requests.get(rain_fall_JSON)
    rf1h_req = rf1h_JSON_res.json()
    rf1h_values = []

    target_loc = ["Marikina (Youth Camp)", "Mt. Oro", "Nangka", "San Mateo-2"]

    for item in rf1h_req:
        loc = item["obsnm"]
        if loc in target_loc:
            rf1h = item["rf1hr"]
            rf1h = rf1h.strip('(*)')
            rf1h_values.append((loc, rf1h))

    MarikinaYouthCamp_preci.append({
    "location": rf1h_values[0][0],
    "precipitation_last_hour": rf1h_values[0][1],
    })
    MtOro_preci.append({
    "location": rf1h_values[1][0],
    "precipitation_last_hour": rf1h_values[1][1],
    })
    Nangka_preci.append({
    "location": rf1h_values[2][0],
    "precipitation_last_hour": rf1h_values[2][1],
    })
    SanMateo_preci.append({
    "location": rf1h_values[3][0],
    "precipitation_last_hour": rf1h_values[3][1],
    })

    dt_db = strDate + ":00"
    
    pagaasa_dt = dt_db
    myc_rf_1hr = rf1h_values[0][1]
    mo_rf_1hr = rf1h_values[1][1]
    ngnk_rf_1hr = rf1h_values[2][1]
    sm_rf_1hr = rf1h_values[3][1]





@app.get("/get_weather_data")
async def get_weather_data():
    get_water_level()
    get_current_weather()
    get_IBM_forecast()
    get_OW_forecast()
    get_PAGASA_precipitation_past1HR()
 
    data = {
        "sto_nino_waterlevel": sto_nino_waterlevel,
        "san_mateo_waterlevel": san_mateo_waterlevel,
        "tumana_waterlevel": tumana_waterlevel,
        "sto_nino_current_weather": sn_current_weather,
        "san_mateo_current_weather": sm_current_weather,
        "tumana_current_weather": tm_current_weather,
        "OpenWeather_weather_forecast": forecasted_OW_weather,
        "IBM_weather_forcast": IBM_forecasted_weather,
        "Marikina_Youth_Camp_preci1HR": MarikinaYouthCamp_preci,
        "Mt_Oro_preci1HR": MtOro_preci,
        "Nangka_preci1HR": Nangka_preci,
        "San_Mateo_preci1HR": SanMateo_preci
    }

    return JSONResponse(content=data)


@app.get("/post_to_db")
async def post_to_db():
        
        get_current_weather()
        get_water_level()
        get_OW_forecast()

    
        current_add_db = ("INSERT INTO observed_data "
                "(sto_nino_current_waterlevel, san_mateo_current_waterlevel, tumana_current_waterlevel, sto_nino_current_humidity, sto_nino_current_temperature, sto_nino_current_precipitation, san_mateo_current_humidity, san_mateo_current_temperature, san_mateo_current_precipitation, tumana_current_humidity, tumana_current_temperature, tumana_current_precipitation, date_time) "
                "VALUES (%(sn_wl)s, %(sm_wl)s, %(tm_wl)s, %(sn_h)s, %(sn_t)s, %(sn_p)s, %(sm_h)s, %(sm_t)s, %(sm_p)s, %(tm_h)s, %(tm_t)s, %(tm_p)s, %(c_dt)s)")
        
        forecast_add_db = ("INSERT INTO forecasted_data "
                "(sto_nino_forecast_waterlevel, san_mateo_forecast_waterlevel, tumana_forecast_waterlevel, sto_nino_forecast_humidity, sto_nino_forecast_temperature, sto_nino_forecast_precipitation, san_mateo_forecast_humidity, san_mateo_forecast_temperature, san_mateo_forecast_precipitation, tumana_forecast_humidity, tumana_forecast_temperature, tumana_forecast_precipitation, date_time) "
                "VALUES (%(sn_wl)s, %(sm_wl)s, %(tm_wl)s, %(sn_h)s, %(sn_t)s, %(sn_p)s, %(sm_h)s, %(sm_t)s, %(sm_p)s, %(tm_h)s, %(tm_t)s, %(tm_p)s, %(c_dt)s)")
        

        
        current_db_ob_data = {
            'sn_wl': sn_wl_c,
            'sm_wl': sm_wl_c,
            'tm_wl': tm_wl_c,
            'sn_h': sn_humidity,
            'sn_t': sn_temperature,
            'sn_p': sn_preci,
            'sm_h': sm_humidity,
            'sm_t': sm_temperature,
            'sm_p': sm_preci,
            'tm_h': tm_humidity,
            'tm_t': tm_temperature,
            'tm_p': tm_preci,
            'c_dt': db_time_date
        }

        #TEMPORARY
        sn_wl_f = None
        sm_wl_f = None
        tm_wl_f = None

        forecasted_db_ob_data = {
            'sn_wl': sn_wl_f,
            'sm_wl': sm_wl_f,
            'tm_wl': tm_wl_f,
            'sn_h': forecasted_OW_weather[0]['humidity'],
            'sn_t': forecasted_OW_weather[0]['temperature'],
            'sn_p': forecasted_OW_weather[0]['precipitation'],
            'sm_h': forecasted_OW_weather[1]['humidity'],
            'sm_t': forecasted_OW_weather[1]['temperature'],
            'sm_p': forecasted_OW_weather[1]['precipitation'],
            'tm_h': forecasted_OW_weather[2]['humidity'],
            'tm_t': forecasted_OW_weather[2]['temperature'],
            'tm_p': forecasted_OW_weather[2]['precipitation'],
            'c_dt': forecasted_OW_weather[2]['date_time']
        }



        cursor.execute(current_add_db, current_db_ob_data)
    
        cursor.execute(forecast_add_db, forecasted_db_ob_data)

        
        mydb.commit()
        


@app.get("/post_IBM_to_DB")
async def post_IBM_to_db():
        get_IBM_forecast()
        get_PAGASA_precipitation_past1HR()

        IBM_forecast_add_db = ("INSERT INTO IBM_weather_forecast "
        "(sto_nino_forecast_waterlevel, san_mateo_forecast_waterlevel, tumana_forecast_waterlevel, sto_nino_IBM_forecast_humidity, sto_nino_IBM_forecast_temperature, sto_nino_IBM_forecast_precipitation, san_mateo_IBM_forecast_humidity, san_mateo_IBM_forecast_temperature, san_mateo_IBM_forecast_precipitation, tumana_IBM_forecast_humidity, tumana_IBM_forecast_temperature, tumana_IBM_forecast_precipitation, date_time) "
        "VALUES (%(sn_wl)s, %(sm_wl)s, %(tm_wl)s, %(sn_h)s, %(sn_t)s, %(sn_p)s, %(sm_h)s, %(sm_t)s, %(sm_p)s, %(tm_h)s, %(tm_t)s, %(tm_p)s, %(c_dt)s)")

        PAGASA_add_db = ("INSERT INTO PAGASA_obs_preci "
        "(myc_preci_p1hr, mo_preci_p1hr, nangka_preci_p1hr, sm_preci_p1hr, date_time)"
        "VALUES (%(myc_preci)s, %(mto_preci)s, %(nnka_preci)s, %(sm_preci)s, %(c_dt)s)")

        #temporary
        sn_wl_f = None
        sm_wl_f = None
        tm_wl_f = None
        
        db_IBM_data = {
            'sn_wl': sn_wl_f,
            'sm_wl': sm_wl_f,
            'tm_wl': tm_wl_f,
            'sn_h': IBM_forecasted_weather[0]['humidity'],
            'sn_t': IBM_forecasted_weather[0]['temperature'],
            'sn_p': IBM_forecasted_weather[0]['precipitation'],
            'sm_h': IBM_forecasted_weather[1]['humidity'],
            'sm_t': IBM_forecasted_weather[1]['temperature'],
            'sm_p': IBM_forecasted_weather[1]['precipitation'],
            'tm_h': IBM_forecasted_weather[2]['humidity'],
            'tm_t': IBM_forecasted_weather[2]['temperature'],
            'tm_p': IBM_forecasted_weather[2]['precipitation'],
            'c_dt': IBM_forecasted_weather[0]['date_time']
        }

        fPAGASA_db_ob_data = {
            'myc_preci': myc_rf_1hr,
            'mto_preci': mo_rf_1hr,
            'nnka_preci': ngnk_rf_1hr,
            'sm_preci': sm_rf_1hr,
            'c_dt': pagaasa_dt
        }

        cursor.execute(IBM_forecast_add_db, db_IBM_data)


        cursor.execute(PAGASA_add_db, fPAGASA_db_ob_data)

        mydb.commit()