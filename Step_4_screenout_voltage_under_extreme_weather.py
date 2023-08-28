import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import copy

cleaned_data_address="C:\\Users\\wan397\\OneDrive - CSIRO\\Desktop\\grid_impact\\mydb.db"
connection = sqlite3.connect(cleaned_data_address)
cur = connection.cursor()

extreme_weather = pd.read_sql('SELECT time_ FROM extreme_weather', connection)

all_data=pd.read_sql('SELECT * FROM cleaned_data_table_jun', connection)
all_data_copy=copy.deepcopy(all_data)


#####shorten the data of voltages########
cur.execute("DROP TABLE IF EXISTS voltage_data_under_extreme_weather")
sql_create_projects_table = "CREATE TABLE voltage_data_under_extreme_weather ( time_ char, generation  integer,   temp  integer, ganeration_changes_lab integer, A integer, B integer, C integer ); "
cur.execute(sql_create_projects_table)
connection.commit()

voltage_all_data=pd.read_sql('SELECT * FROM Voltage_june', connection)
voltage_all_data_copy=copy.deepcopy(voltage_all_data)

for x in range(len(voltage_all_data )):
    tempurary = voltage_all_data.iloc[x][0].split('T')[0]
    matched=0
    for y in range ( len(extreme_weather)):
        target = extreme_weather.iloc[y][0]
        if(tempurary ==target):
            matched=1
            break
    if(matched==0):
        voltage_all_data_copy.drop(x,axis=0,inplace=True)
voltage_all_data_copy.to_sql('voltage_data_under_extreme_weather', connection, if_exists='replace', index = False)

# ####For shortened dataset only under extreme weather#####
# cur.execute("DROP TABLE IF EXISTS all_data_under_extreme_weather")
# sql_create_projects_table = "CREATE TABLE all_data_under_extreme_weather ( time_ char, generation  integer,   temp  integer, ganeration_changes_lab integer, A integer, B integer, C integer ); "
# cur.execute(sql_create_projects_table)
# connection.commit()

# for x in range(len(all_data )):
#     tempurary = all_data.iloc[x][4] #.split('T')[0]
#     if(tempurary ==0):
#         all_data_copy.drop(x,axis=0,inplace=True)
#         # print(1)
# all_data_copy.to_sql('all_data_under_extreme_weather', connection, if_exists='replace', index = False)
print(1)
