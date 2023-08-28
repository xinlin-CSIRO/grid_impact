import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

A_data="C:\\Users\\wan397\\OneDrive - CSIRO\\Desktop\\grid_impact\\VoltageAN.csv"
B_data="C:\\Users\\wan397\\OneDrive - CSIRO\\Desktop\\grid_impact\\VoltageBN.csv"
C_data="C:\\Users\\wan397\\OneDrive - CSIRO\\Desktop\\grid_impact\\VoltageCN.csv"

A_data=np.array(pd.read_csv(A_data).values)
B_data=np.array(pd.read_csv(B_data).values)
C_data=np.array(pd.read_csv(C_data).values)
A_data_=[]
B_data_=[]
C_data_=[]
HEAD=['2022-03-01T00','03']
TAIL=['2023-04-30T23','58']

begin=0
for x in range(len(A_data)):
   a = A_data[x,0].split(':')
   if(a[0]==HEAD[0] and a[1]==HEAD[1]) or (begin==1):
       time_=a[0]+':'+a[1]
       A_data_.append([time_, A_data[x,1]])
       begin=1
   if(a[0]==TAIL[0] and a[1]==TAIL[1]):
       break

begin=0
for x in range(len(B_data)):
   a=B_data[x,0].split(':')
   if(a[0]==HEAD[0] and a[1]==HEAD[1]) or (begin==1):
       time_ = a[0] + ':' + a[1]
       B_data_.append([time_, B_data[x, 1]])
       begin=1
   if(a[0]==TAIL[0] and a[1]==TAIL[1]):
       break

begin=0
for x in range(len(C_data)):
   a=C_data[x,0].split(':')
   if(a[0]==HEAD[0] and a[1]==HEAD[1]) or (begin==1):
       time_ = a[0] + ':' + a[1]
       C_data_.append([time_, C_data[x, 1]])
       begin=1
   if(a[0]==TAIL[0] and a[1]==TAIL[1]):
       break
A_data_=np.array(A_data_)
B_data_=np.array(B_data_)
C_data_=np.array(C_data_)
final_matched=[]
range_=5
length_=len(A_data_)
for x in range (0,length_):
    target=A_data_[x,0]
    value_a=A_data_[x,1]
    value_b=None
    value_c = None
    for y in range (max(0, (x-range_)), min(len(B_data_),(x+range_))):
        # print(y)
        if(B_data_[y,0]==target):
            value_b = B_data_[y, 1]
            break
    for z in range (max(0, (x-range_)), min(len(C_data_),(x+range_))):
        if(C_data_[z,0]==target):
            value_c = C_data_[z, 1]
            break
    final_matched.append([target,value_a,value_b,value_c])

final_matched_df = pd.DataFrame(final_matched, columns = ['Time','Voltage_A','Voltage_B','Voltage_C'])

# well-cleaned dataset is here#########
cleaned_data_address="C:\\Users\\wan397\\OneDrive - CSIRO\\Desktop\\grid_impact\\mydb.db"
connection = sqlite3.connect(cleaned_data_address)
cur = connection.cursor()

cur.execute("DROP TABLE IF EXISTS Voltage_june")
sql_create_projects_table = "CREATE TABLE Voltage_june ( time_ char, A  integer,  B integer, C  integer); "
cur.execute(sql_create_projects_table)
connection.commit()

final_matched_df.to_sql('Voltage_june', connection, if_exists='replace', index = False)


print(1)