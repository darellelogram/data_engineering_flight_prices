import pandas as pd
import numpy as np
from datetime import datetime
import re
biz = pd.read_csv("data/business.csv")
eco = pd.read_csv("data/economy.csv")
coord = pd.read_csv("data/india_cities.csv")

biz["class"] = "business"
eco["class"] = "economic"

combi = eco.append(biz)
#date['inputDate'] = pd.to_datetime(date['inputDate'])
combi["date"] = pd.to_datetime(combi["date"],"d-m-Y")
combi["dept_day"] = combi["date"].dt.day_name()

combi.rename({"dep_time": "departure_time", "from": "source_city", 
            "time_taken": "duration", "stop": "stops", "arr_time": "arrival_time",
           "to":"destination_city"}, axis = 1, inplace = True)
combi['duration'] = combi['duration'].astype(str)


dd = pd.DataFrame(combi["date"].astype(str).str.split("-",expand = True).to_numpy().astype(int),columns = ["year","month","day"])
combi["days_left"] = np.where(dd["month"] > 2, dd["day"] +18, np.where(dd["month"] == 2, dd["day"] -10, dd["day"]))

temp = pd.DataFrame(combi["duration"].str.split(expand = True).to_numpy().astype(str), 
                    columns = ["hour","minute"])

temp["hour"] = temp["hour"].apply(lambda x: re.sub("[^0-9]","",x)).astype(int)
temp["minute"] = temp["minute"].apply(lambda r: re.sub("[^0-9]","",r))  
temp["minute"] = np.where(temp["minute"] == "", 0, temp["minute"]) 
temp["minute"] = temp["minute"].astype(int) #converting data type

combi["duration_new"] = np.around((temp["hour"] + (temp["minute"]/60)),2)

combi["stops"] = combi["stops"].apply(lambda r: re.sub("[^0-9]","",r)) # taking only digits
combi["stops"] = np.where(combi["stops"] == "", 0, combi["stops"]) # replacign "" with 0
combi["stops"] = combi["stops"].astype(int)


def makeDurationCategory(x):
    if x < 3:
        return 'Short Haul'
    elif x < 6:
        return 'Medium Haul'
    else:
        return 'Long Haul'
    
combi['durationCategory'] = combi['duration_new'].astype(int).apply(makeDurationCategory)

def makeTimeBin(x):
    if (x >= '00:00:00') & (x < '04:00:00'):
        return 'Midnight'
    elif (x >= "04:00:00") & (x < "08:00:00"):
        return "Early Morning"
    elif (x >= "08:00:00") & (x <"12:00:00"):
        return "Late Morning"
    elif (x >= "12:00:00") & (x < "16:00:00") :
        return "Afternoon"
    elif (x >= "16:00:00") & (x < "20:00:00"):
        return "Evening"
    else:
        return "Night"
    
combi["departure_time"] = pd.to_datetime(combi["departure_time"], format="%H:%M:%S")
combi["departure_time"] = combi["departure_time"].dt.strftime("%H:%M:%S")
combi["departure_time_bin"] = combi["departure_time"].apply(makeTimeBin)

combi["arrival_time"] = pd.to_datetime(combi["arrival_time"], format="%H:%M:%S")
combi["arrival_time"] = combi["arrival_time"].dt.strftime("%H:%M:%S")
combi["arrival_time_bin"] = combi["arrival_time"].apply(makeTimeBin)

combi = pd.merge(combi,coord, left_on="source_city", right_on="city", how="left")
combi.rename({"latitude": "source_latitiude", "longitude": "source_longitude"}, axis = 1, inplace = True)

combi = pd.merge(combi, coord, left_on="destination_city", right_on="city", how="left")
combi.rename({"latitude": "destination_latitiude", "destination": "source_longitude"}, axis = 1, inplace = True)

combi.drop(columns=['city_x', 'country_x',"city_y","country_y"])

combi.to_excel("final.xlsx")
