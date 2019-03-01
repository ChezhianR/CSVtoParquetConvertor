import datetime
import os
import uuid as uid
import pandas as pd
import shutil
import numpy as np


## Check if directory exists and create if needed
def CheckandCreateFolders(ipfilename,otfolder,otfile):
    strpartion = datetime.datetime.strptime(ipfilename[:8],"%Y%m%d").strftime("%Y-%m-%d")
    outfolder = str.format(otfolder,strpartion)
    outfile=str.format(otfile,uid.uuid4())
    if os.path.exists(outfolder):
        shutil.rmtree(outfolder)
    os.makedirs(outfolder) 
    return outfolder, outfile

# convert file and place in given folder
def processfile(ipfolder,ipfilename,otfolder,otfile):
    ipfilepath=str.format(ipfolder+ipfilename)
    opfilepath=str.format(otfolder+otfile)
    #csvinput=pd.read_csv(filepath_or_buffer=ipfilepath,sep=",",dtype={'company_number':int,'device_number':int,'source_binary_ip':str },parse_dates=["Start_time","End_Time"])
    csvinput=pd.read_csv(filepath_or_buffer=ipfilepath,sep=",").fillna(value=' ')
    
    # Convert datetime to epoch
    csvinput.Start_time=  (pd.to_datetime(csvinput.Start_time)).astype('int64')//1e9
    csvinput.End_Time= csvinput.Start_time # (pd.to_datetime(csvinput.End_Time)).astype('int64')//1e9

    #Rename Columns to match schema 
    csvinput.columns=["company_number","device_number","source_binary_ip","log_line_count","start_time","end_time"]
    csvinput.to_parquet(compression="snappy",fname=opfilepath)

def main():
    foldertolook="C:/Users/chezhian_ravikumar/Documents/Python Scripts/chezhian/2016/"
    opfolder="C:/Users/chezhian_ravikumar/Documents/Python Scripts/chezhian/tblLogAggregrateSummary/partion={}/"
    filename="part-00000-{}.c000.parquet"

    files = next(os.walk(foldertolook))[2]

    for f in files:
        outfolder, outfile = CheckandCreateFolders(f,opfolder,filename)
        processfile(foldertolook,f,outfolder,outfile)
        #print(format("processed {} to {}",foldertolook+f,outfolder+outfile),end="\n")
        print("processed {} to {} at {}".format(foldertolook+f,outfolder+outfile, datetime.datetime.now()),end="\n")
        #print(f+ "\n" + f[:8]+ "\n" +outfolder+"\n"+outfile)

    
if __name__ == "__main__":
    main()


