import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

temp_data="C:\\Users\\wan397\\PycharmProjects\\Work_with_Narman\\Temperature.csv"
gas_data="C:\\Users\\wan397\\PycharmProjects\\Work_with_Narman\\GasTurbines.csv"
real_power_data="C:\\Users\\wan397\\PycharmProjects\\Work_with_Narman\\RealPower.csv"


temp_data=np.array(pd.read_csv(temp_data).values)[:,1:2]
gas_data=np.array(pd.read_csv(gas_data).values)
real_power_data=np.array(pd.read_csv(real_power_data).values)[:,0:2]

limits=min(len(gas_data), len(real_power_data))
time_=[]
for x in range(limits):
    y=real_power_data[x, 0].split(':')
    temp=y[0]+':'+y[1]
    time_.append(temp)
    # print(1)
power_=real_power_data[:, 1]
gas_=gas_data[0:limits,1]

##############temp data cleaning#############################
current_hour=0
times=0
value=0
new_temp_data=[]
new_temp_date=[]
length_=5
temp_=[]
for x in range (limits):
    head=x*length_
    rear=x*length_+5
    date_=np.average(temp_data[head:rear])
    temp_.append(date_)

# well-cleaned dataset is here#########
cleaned_data_address="C:\\Users\\wan397\\OneDrive - CSIRO\\Desktop\\grid_impact\\mydb.db"
connection = sqlite3.connect(cleaned_data_address)
cur = connection.cursor()
cur.execute("DROP TABLE IF EXISTS managed_data_2")
sql_create_projects_table = "CREATE TABLE managed_data_2 ( time_ char, power  integer,  gas integer, temp  integer); "
cur.execute(sql_create_projects_table)
connection.commit()


time_=np.array(time_)
power_=np.array(power_)
gas_=np.array(gas_)
temp_=np.array(temp_)

for x in range (limits):
    sqlite_insert_ = """INSERT OR IGNORE INTO managed_data_2 (time_, power,  gas, temp) VALUES (?, ?, ?, ?);"""
    data_tuple = (time_[x], power_[x], gas_[x], temp_[x])  # (t-1, t, t)--> diff at t
    cur.execute(sqlite_insert_, data_tuple)
    connection.commit()


# cleaned_date=np.array(cleaned_date)
# cleaned_data=np.array(cleaned_data)
# whole_dataset = np.column_stack(cleaned_date, cleaned_data)








print(1)

print(1)
