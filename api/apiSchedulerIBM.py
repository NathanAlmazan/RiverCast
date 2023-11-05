from apscheduler.schedulers.background import BackgroundScheduler
import requests
import time


print("SCHEDULER RUNNING!")

def fetch_weather_data():
    start_time = time.time()
    response = requests.get('http://ec2-18-142-249-114.ap-southeast-1.compute.amazonaws.com:8000/post_IBM_to_DB')
    end_time = time.time()
    
    if response.status_code == 200:
        print(f"IBM Weather data fetched successfully in {end_time - start_time} seconds")
        # Adjust the interval based on the time it took to fetch the data
        elapsed_time = end_time - start_time
        new_interval = max(1, 3600 - elapsed_time)  # Ensure a minimum interval of 1 second
        scheduler.reschedule_job('fetch_weather_data', trigger='interval', seconds=new_interval)
    else:
        print("IBM Failed to fetch weather data")

# Create a scheduler and add the job to run every 1 hour initially
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_weather_data, 'interval', hours=1, id='fetch_weather_data')

# Start the scheduler immediately
scheduler.start()

# Fetch weather data immediately upon starting the script
fetch_weather_data()

try:
    # Keep the script running
    while True:
        pass
except (KeyboardInterrupt, SystemExit):
    # Shut down the scheduler gracefully on exit
    scheduler.shutdown()
