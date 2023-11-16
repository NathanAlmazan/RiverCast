
import pymysql
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd

import csv

mydb = mysql.connector.connect(
    host="database-1.cccp1zhjxtzi.ap-southeast-1.rds.amazonaws.com",
    user="admin",
    password="Nath1234",
    database="rivercast"
)

df = pd.read_csv('riverlevel.csv')
mainDataToDB = df

engine = create_engine("mysql+pymysql://" + "admin" + ":" + "Nath1234" + "@" + "database-1.cccp1zhjxtzi.ap-southeast-1.rds.amazonaws.com" + "/" + "rivercast")

def test():
    print("hello")
    df = mainDataToDB
    df.to_sql(name="modelData1", con=engine, index=False, if_exists="replace")
test()

def ggg():
    QUERY = 'SELECT * FROM rivercast.ModelData_1;'

    cur = mydb.cursor()
    cur.execute(QUERY)
    result = cur.fetchall()

    # Extract column names from the cursor description
    columns = [desc[0] for desc in cur.description]

    # Specify the column to exclude (change 'column_to_exclude' to the actual column name)
    column_to_exclude = 'Datetime'

    # Exclude the specified column
    columns_to_write = [col for col in columns if col != column_to_exclude]

    # Open the CSV file for writing
    with open('dbdump01.csv', 'w', newline='') as csvfile:
        # Create a CSV writer
        csv_writer = csv.writer(csvfile)

        # Write the modified column names as the header
        csv_writer.writerow(columns_to_write)

        # Get the indices of the columns to write
        indices_to_write = [i for i, col in enumerate(columns) if col != column_to_exclude]

        # Write the data rows excluding the specified column
        for row in result:
            csv_writer.writerow([row[i] for i in indices_to_write])

    print(f"CSV file 'dbdump01.csv' has been created excluding column '{column_to_exclude}'.")

