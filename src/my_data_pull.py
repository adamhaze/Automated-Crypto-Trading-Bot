import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine

# Load ENV Files
env_vars = {} # or dict {}
with open('env.txt') as f:
    for line in f:
        if line.startswith('#') or not line.strip():
            continue
        key, value = line.strip().split('=', 1)
        env_vars[key]= value
print(env_vars)

# Run this Cell to ensure connected to DB
try:
    connection = mysql.connector.connect(host=env_vars['HOST'],
                                         database=env_vars['DB'],
                                         user=env_vars['USER'],
                                         password=env_vars['PASSWORD'])
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

except Error as e:
    print("Error while connecting to MySQL", e)
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")


"""
# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                        .format(user=env_vars['USER'],
                               pw=env_vars['PASSWORD'],
                               host=env_vars['HOST'],
                               db=env_vars['DB']))
cols = ['index','Unix Timestamp','Date','Symbol','Open','High','Low','Close','Volume']

df = pd.read_sql_table('BTC_Ticker_Data', con=engine, index_col='index')
df.to_csv('Master_Data_File.csv', index=True)

MasterData = pd.read_csv('Master_Data_File.csv')
MasterData.set_index('index')
"""