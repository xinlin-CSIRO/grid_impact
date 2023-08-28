import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
from datetime import datetime
# well-cleaned dataset is here#########
cleaned_data_address="C:\\Users\\wan397\\OneDrive - CSIRO\\Desktop\\grid_impact\\mydb.db"
connection = sqlite3.connect(cleaned_data_address)
cur = connection.cursor()
# query = "ALTER TABLE cleaned_data_table \
#            ADD label_generation_changes integer"
# cur.execute(query)
#
# query = "ALTER TABLE cleaned_data_table \
#            ADD extreme_weather integer"
# cur.execute(query)



sql_time = np.array(pd.read_sql('SELECT time_ FROM managed_data_2', connection))[0:104544]
sql_temp = np.array(pd.read_sql('SELECT temp FROM managed_data_2', connection))[0:104544]
sql_gas = np.array(pd.read_sql('SELECT gas FROM managed_data_2', connection))[0:104544]
sql_power = np.array(pd.read_sql('SELECT power FROM managed_data_2', connection))[0:104544]

#########PV generation#####
generation=sql_power-sql_gas
generation_1st_diff= np.diff(generation[:,0])

# plt.plot(generation_1st_diff, 'b*')
# plt.show()

_, q3_1st = np.percentile(generation_1st_diff, [5, 99])
labels=np.zeros(len(generation_1st_diff)+1)
n_=0
for x in range(1, len(generation_1st_diff)+1):
    # if(generation_1st_diff[x-1]<q1_1st) or (generation_1st_diff[x-1]>q3_1st):
    if (generation_1st_diff[x - 1] > q3_1st):
        labels[x]=1
        n_+=1

plt.plot(labels)
plt.show()
########temp (weather)#####
##1. reconstucture

n_days=363
n_steps=288
sql_time_=np.reshape(sql_time,(n_days, n_steps))
sql_temp_=np.reshape(sql_temp,(n_days, n_steps))

temp_max=np.zeros(n_days)
temp_min=np.zeros(n_days)
# Q1,Q3=np.zeros(288),np.zeros(288)
for x in range (n_days):
    a=sql_temp_[x, :]
    if(-20 not in a):
        temp_max[x]=max(a)
        temp_min[x] = min (a)

_, hottest_= np.percentile(temp_max, [0, 75])
chilled_est,_= np.percentile(temp_min, [25, 100])
# for x in range (n_days):
#     a=sql_temp_[x, :]
#     plt.plot(a)
# # plt.plot(temp_max, 'b*')
# # plt.plot(temp_min, 'b*')
# plt.show()
exetreme_weather_=0
if exetreme_weather_ ==1:
    cur.execute("DROP TABLE IF EXISTS extreme_weather")
    sql_create_projects_table = "CREATE TABLE extreme_weather ( No integer, time_ str); "
    cur.execute(sql_create_projects_table)
    connection.commit()

labels_temp=np.zeros(104544)
num=0
for x in range (n_days):
    a_day=sql_temp_[x, :]
    min_=min(a_day)
    max_=max(a_day)
    head = x * 288
    rear = x * 288 + 288
    if(min_<(chilled_est) or (max_>(hottest_))):
        labels_temp[head:rear]=1
        num+=1
        if exetreme_weather_ == 1:
            date_=sql_time_[x, 0].split('T')[0]
            sqlite_insert = """INSERT INTO extreme_weather (No, time_) VALUES (?,?);"""
            data_tuple =  (num, date_)  # (t-1, t, t)--> diff at t
            cur.execute(sqlite_insert, data_tuple)
            connection.commit()

    # Q1[x], Q3[x] = np.percentile(a, [10, 90])
# for x in range (363):
#     a=sql_temp_[x, :]
#     plt.plot(a)


cur.execute("DROP TABLE IF EXISTS cleaned_data_table_Jun")
sql_create_projects_table = "CREATE TABLE cleaned_data_table_Jun ( time_ char, generation  integer,   temp  integer, ganeration_changes_lab integer, temp_changes_lab integer ); "
cur.execute(sql_create_projects_table)
connection.commit()
print(0)


for x in range (len(sql_power)):
    sqlite_insert = """INSERT INTO cleaned_data_table_Jun (time_, generation, temp, ganeration_changes_lab,  temp_changes_lab) VALUES (?, ?,  ?, ?, ?);"""
    data_tuple = (sql_time[x,0], generation[x,0], sql_temp[x,0],labels[x], labels_temp[x] ) # (t-1, t, t)--> diff at t
    cur.execute(sqlite_insert, data_tuple)
    connection.commit()

# cur.execute(sqlite_query__)





print(1)

