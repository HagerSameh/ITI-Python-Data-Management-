import argparse 
from subprocess import PIPE, Popen
from os import listdir
from os.path import isfile, join 
import os   
import json 
import pandas as pd
from pandas.io.json import json_normalize 
from datetime import datetime
import pytz  

from datetime import datetime
startTime = datetime.now()

parser = argparse.ArgumentParser()
parser.add_argument("dir", help="Enter directory path")
parser.add_argument("-u", "--unix", action="store_true", dest="unixTimeFormat", default=False,help="Covert timestamp to Unix Time Format")
args = parser.parse_args()
files = [item for item in listdir(args.dir) if (".json" in item)]
#print (files)

checksums = {}
duplicates = []

for filename in files:
    with Popen(["md5sum", args.dir+"/"+filename], stdout=PIPE) as proc:
        checksum = proc.stdout.read().split()[0]
       
        if checksum in checksums:
            duplicates.append(filename)
        checksums[checksum] = filename

print(f"Found Duplicates: {duplicates}")  

for i in duplicates: 
  os.remove(i) 
#for i in duplicates: 
 # print (i)   

for filename in files:
    if filename not in duplicates: 
      if '_done'in filename:  
         print ( 'File ' + filename  + ' is already processed' ) 
      else:
          
         records = [json.loads(line) for line in open(filename)]
         df = json_normalize(records)  
         df=df[['a','tz','r', 'u', 't', 'hc', 'cy', 'll']]  

         new = df["a"].str.split("/", n = 1, expand = True) 
         df['web_browser']=new[0]
        
         #to cut the operating_sytem from column 'a'
         new = df["a"].str.split("(", n = 1, expand = True) 
         new= new[1].str.split(" ", n = 1, expand = True)
         new= new[0].str.split(";", n = 1, expand = True)
         df['operating_sys']=new[0] 

         tmp = df["r"].str.split("//", expand = True)    
         tmp= tmp[1].str.split("/", expand = True)  
         df['from_url']=tmp[0] 

         tmp = df["r"].str.split("//", expand = True)    
         tmp= tmp[1].str.split("/", expand = True)  
         df['to_url']=tmp[0] 

         #create new column just to be in the same sequence 
         df['city']=df['cy']
        
        #to get the longitude in new column out of column'll'
         df['longitude']=df['ll'].str[0]
        
        #to get the latitude in new column out of column'll'
         df['latitude']=df['ll'].str[1] 
         if args.unixTimeFormat:
            df['time_in']=df['t']
            df['time_out']=df['hc']
         else:    
           df['time_in'] = pd.to_datetime(df['t'] , utc = True)
           df['time_out'] = pd.to_datetime(df['hc'],  utc = True)
        
        
 
         df = df.dropna() 
         df=df.drop(df.columns[df.columns.get_loc('a'):df.columns.get_loc('ll')+1], axis=1)


         print('there are {} rows transformed from file:{}'.format(df.shape[0], args.dir+"/"+filename))   
         #print ( df.head(3)) 
         #to save the files
         outputfile=filename.replace('.json',' ')
         df.to_csv('/home/hager/Desktop/trial/'+outputfile+'.csv') 
         os.rename(filename, filename+ '_done')

#print the execution time of the script
execution_time= (datetime.now() - startTime)
print("The total execution time of the script is: {}".format(execution_time))

	     



   
