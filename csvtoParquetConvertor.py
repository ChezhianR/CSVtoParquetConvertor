import os
import pandas as pd
import dateutil.parser
import datetime

import socket
import binascii

print(os.getcwd())
print("\n")

dateparser= lambda dt: int(datetime.datetime.strptime(dt, "%m/%d/%Y %I:%M:%S %p").timestamp() )

hexval =binascii.hexlify(socket.inet_aton('0.0.0.0')).upper()

loaded_df=pd.read_csv(filepath_or_buffer="C:/Users/chezhian_ravikumar/Documents/Python Scripts/Chezhian/test/20181225.csv",sep=",") .fillna(value=hexval)

loaded_df.company_number=loaded_df.company_number.astype('int')
loaded_df.device_number=loaded_df.device_number.astype('int')
loaded_df.Start_time=  (pd.to_datetime(loaded_df.Start_time)).astype('int64')//1e9
loaded_df.End_Time=  loaded_df.Start_time #(pd.to_datetime(loaded_df.End_Time)).astype('int64')//1e9

 

loaded_df.columns=["company_number","device_number","DeviceNumber","log_line_count","start_time","end_time"]
 

#loaded_df.start_time=dateutil.parser.parse(loaded_df.start_time).timestamp()
 

print(loaded_df.dtypes)


converted= loaded_df.to_parquet("C:/Users/chezhian_ravikumar/Documents/Python Scripts/Chezhian/test/Converted.parquet" )
print(loaded_df.head())
print(loaded_df.columns.values)
print("conversion Completed")
print("\n")

print("reading Parquet")
par = pd.read_parquet("C:/Users/chezhian_ravikumar/Documents/Python Scripts/Chezhian/test/Converted.parquet")
print(par.head())
