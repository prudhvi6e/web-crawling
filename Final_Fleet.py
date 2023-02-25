#!/usr/bin/env python
# coding: utf-8

# In[1]:


#### Created by Nancy Goyal 


import os
import mdfreader
import pandas as pd
import numpy as np
import operator
import datetime
import pathlib
import time
from datetime import datetime
from tkinter import *
from tkinter import messagebox
from tkinter import filedialog as fd
import traceback
from tkinter import ttk
import shutil
from openpyxl import load_workbook
import warnings
warnings.filterwarnings("ignore")


# This code is checking if certain data criteria are met.
# 
# check_for_all_met_criteria function takes two parameters: required_fields and latest_time_count. It checks if the length of required_fields is equal to latest_time_count. If so, it returns True, otherwise, it returns False.
# 
# check_for_time_group function takes two parameters: required_fields and yop. It creates an empty dictionary count_for_time and initializes a counter count_for_time[latest_time] to 0. It then loops through the keys of yop dictionary, and:
# 
# If the key starts with "time", the function calls check_for_all_met_criteria and checks if all the required fields have been met. If so, it breaks out of the loop, otherwise, it resets count_for_time to an empty dictionary and sets latest_time to the current key.
# 
# If the key doesn't start with "time", it checks if the key is in the required_fields list (ignoring case). If so, it increments the count_for_time[latest_time] by 1.
# 
# Finally, it checks if the key [latest_time]_group exists in yop. If so, it returns that key, otherwise it returns "group_group".
# 
# filter_data function takes two parameters: required_fields and yop. It calls check_for_time_group to determine which group the data belongs to, and then checks if there is any data in that group that meets certain criteria. If so, it returns True, otherwise it returns False.

# In[2]:


def check_for_all_met_criteria(required_fields, latest_time_count) :
    if len(required_fields) == latest_time_count :
        return True
    else :
        return False
    
    
def check_for_time_group(required_fields, yop):
    count_for_time = {}
    try :
        latest_time = list(yop.keys())[0]
        count_for_time[latest_time] = 0
        for data in yop.keys() :
    #         print(data)
            if data.startswith("time") :
                met = check_for_all_met_criteria(required_fields, count_for_time[latest_time]) 
                if met :
                    break
                else :
                    count_for_time = {}
                    count_for_time[data] = 0
                    latest_time = data
            else :
    #             print(latest_time)
                if data.lower() in  (string.lower() for string in required_fields) :
                    count_for_time[latest_time] = count_for_time[latest_time] + 1
        if str(list(count_for_time.keys())[0]) + "_group" in yop.keys() :
            return str(list(count_for_time.keys())[0]) + "_group"
        else :
            return  "group_group"
    except :
        return "group_group"

def filter_data(required_fields, yop) :
    ApfCrk_EngSpeed_key = check_for_time_group(required_fields, yop)
#     print(ApfCrk_EngSpeed_key)
    if "group_group" not in ApfCrk_EngSpeed_key :
        dataframe_engspeed = yop[ApfCrk_EngSpeed_key][required_fields]
        if len(dataframe_engspeed.loc[(dataframe_engspeed[required_fields[0]] < -100 ) | (dataframe_engspeed[required_fields[0]] > 8000)]) > 0 :
            return True
    else :
        return True
    return False


# ### Catalyst Temperature

# To extract maximum temperatures for various sensors ("DC1_Th1" to "DC1_Th5") for each date and computes the date-wise maximum temperatures. The temperatures are filtered based on a change rate, if the rate of change is above 100 or if it's a positive change after a negative change with magnitude greater than 100, the temperature value is ignored. The final temperature values are stored in a dictionary with date as the key and maximum temperatures as the values. The dictionary is then appended to a list of dictionaries which keeps track of the maximum temperatures for all dates. Finally, this list is converted into a Pandas dataframe for further analysis.

# In[3]:


def get_max_temperature(dc1_th5_list) :
    Rate_of_change_of_Temp = [0]
    for i in range(1, len(dc1_th5_list)) :
        Rate_of_change_of_Temp.append(dc1_th5_list[i] - dc1_th5_list[i-1])
    freq_flag = [0]
    for i in range(1, len(Rate_of_change_of_Temp)) :

        if (Rate_of_change_of_Temp[i-1] > 100) or (freq_flag[i-1] > 0 and Rate_of_change_of_Temp[i] > -100) :
            freq_flag.append(1)
        else :
            freq_flag.append(0)
    dc1_th5_list_final = []
    if len(freq_flag) == len(dc1_th5_list) : 
        for i in range(0, len(freq_flag)) :
            if freq_flag[i] == 0 :
                dc1_th5_list_final.append(dc1_th5_list[i])
    return max(dc1_th5_list_final)

def get_per_day_cata_temp(yop, date_, cata_temp_dataframe_key) :
    date_wise_max_cat_temp = {}
    max_value_DC1_Th1 = get_max_temperature(yop[cata_temp_dataframe_key["DC1_Th1"]]["DC1_Th1"].to_list())
    max_value_DC1_Th2 = get_max_temperature(yop[cata_temp_dataframe_key["DC1_Th2"]]["DC1_Th2"].to_list())
    max_value_DC1_Th3 = get_max_temperature(yop[cata_temp_dataframe_key["DC1_Th3"]][ "DC1_Th3"].to_list())
    max_value_DC1_Th4 = get_max_temperature(yop[cata_temp_dataframe_key["DC1_Th4"]]["DC1_Th4"].to_list())
    max_value_DC1_Th5 = get_max_temperature(yop[cata_temp_dataframe_key["DC1_Th5"]][ "DC1_Th5"].to_list())
    #     print(max_value_)
    date_wise_max_cat_temp[str(date_)] = {"DC1_Th1_Max" : max_value_DC1_Th1, "DC1_Th2_Max" : max_value_DC1_Th2,
                                           "DC1_Th3_Max" : max_value_DC1_Th3, "DC1_Th4_Max" : max_value_DC1_Th4,
                                           "DC1_Th5_Max" : max_value_DC1_Th5}
#     print(date_wise_max_cat_temp)
    return date_wise_max_cat_temp


def catalyst_temperature_data(yop, date_,cata_temp_required_fields, date_wise_max_cat_temp) :
    cata_temp_dataframe_key = {}
    for fields in cata_temp_required_fields :
        cata_temp_dataframe_key[fields] = check_for_time_group([fields], yop)
        if "group_group" in cata_temp_dataframe_key[fields] :
            return date_wise_max_cat_temp
        
    date_wise_max_cat_temp.append(get_per_day_cata_temp(yop, date_, cata_temp_dataframe_key))
    return date_wise_max_cat_temp 

def catalyst_temperature_dataframe(date_wise_max_cat_temp) :
    dates = []
    DC1_Th1_Max = []
    DC1_Th2_Max = []
    DC1_Th3_Max = []
    DC1_Th4_Max = []
    for data in date_wise_max_cat_temp :
        for dt in data.keys() :
            dates.append(dt)
            DC1_Th1_Max.append(data[dt]["DC1_Th1_Max"])
            DC1_Th2_Max.append(data[dt]["DC1_Th2_Max"])
            DC1_Th3_Max.append(data[dt]["DC1_Th3_Max"])
            DC1_Th4_Max.append(data[dt]["DC1_Th4_Max"])
    df_cata_temp = pd.DataFrame({"CT_date" : dates, "DC1_Th1_Max" : DC1_Th1_Max, "DC1_Th2_Max" : DC1_Th2_Max,"DC1_Th3_Max" : DC1_Th3_Max
                                ,"DC1_Th4_Max" : DC1_Th4_Max})
    df_cata_temp = df_cata_temp.groupby("CT_date").max()
    df_cata_temp.reset_index(inplace=True)
    
    return df_cata_temp



# ### Catalyst Monitoring

# The first function get_index_mapping returns a list of dictionaries, each dictionary represents the start and end indices of the contiguous sequences of 1s in either of the two lists FldCat_xocat1s_list or FldCat_xocat1f_list.
# 
# The second function catalyst_monitoring takes 3 arguments: yop, date_, and cata_monit_dataframe_key, and returns 4 lists, x, y1, y2, and date_mapping. The function first calls get_index_mapping to get the indices of the contiguous sequences of 1s, then for each sequence it calculates the maximum value of the field "FldCat_cattdlyb" in this sequence, the corresponding value of "FldCat_catgav2" at this maximum value, the corresponding value of "DlgCat_soxvcat" at this maximum value, and finally appends the date date_ to the date_mapping list.
# 
# The third function catalyst_monitoring_data takes 4 arguments yop, date_, cata_monit_required_fields, and 4 lists FldCat_catgav2_x1, FldCat_cattdlyb_y1, DlgCat_soxvcat_y2, Dates_mapping. The function first defines the cata_monit_dataframe_key dictionary by checking the presence of certain fields in the yop dataframe and then calls the catalyst_monitoring function to get the values of x, y1, y2, and date_mapping and finally appends these values to the corresponding lists FldCat_catgav2_x1, FldCat_cattdlyb_y1, DlgCat_soxvcat_y2, Dates_mapping and returns these lists.

# In[4]:


def get_index_mapping(FldCat_xocat1s_list, FldCat_xocat1f_list) :
    zero_to_one_check = False
    index_mapping = []
    for i in range(0, len(FldCat_xocat1s_list)) :
        if FldCat_xocat1s_list[i] == 0 and zero_to_one_check:
            zero_to_one_check = False
            mapping["end"] = i
            index_mapping.append(mapping)
        elif FldCat_xocat1s_list[i] == 1 and not zero_to_one_check:
            zero_to_one_check = True
            mapping = {"start" : i - 1}
    zero_to_one_check = False
    for i in range(0, len(FldCat_xocat1f_list)) :
        if FldCat_xocat1f_list[i] == 0 and zero_to_one_check:
            zero_to_one_check = False
            mapping["end"] = i
            index_mapping.append(mapping)
        elif FldCat_xocat1f_list[i] == 1 and not zero_to_one_check:
            zero_to_one_check = True
            mapping = {"start" : i - 1}
    return index_mapping

def catalyst_monitoring(yop, date_, cata_monit_dataframe_key) :
    y1 = []
    x = []
    y2 = []
    date_mapping = []
    FldCat_xocat1s_list = yop[cata_monit_dataframe_key["FldCat_xocat1s"]]["FldCat_xocat1s"].to_list()

    FldCat_xocat1f_list = yop[cata_monit_dataframe_key["FldCat_xocat1f"]]["FldCat_xocat1f"].to_list()
    index_mapping = get_index_mapping(FldCat_xocat1s_list, FldCat_xocat1f_list)

        
    for mapping in index_mapping :

        y1.append(yop[cata_monit_dataframe_key["FldCat_cattdlyb"]][mapping['start'] : mapping['end'] + 1]["FldCat_cattdlyb"].max())
        max_index = yop[cata_monit_dataframe_key["FldCat_cattdlyb"]][mapping['start'] : mapping['end'] + 1]["FldCat_cattdlyb"].idxmax()
        x.append(yop[cata_monit_dataframe_key["FldCat_catgav2"]][mapping['start'] : mapping['end'] + 1].loc[max_index]["FldCat_catgav2"])
        y2.append(yop[cata_monit_dataframe_key["DlgCat_soxvcat"]][mapping['start'] : mapping['end'] + 1].loc[max_index]["DlgCat_soxvcat"])
        date_mapping.append(date_)
    return x,y1,y2, date_mapping


def catalyst_monitoring_data(yop, date_, cata_monit_required_fields, FldCat_catgav2_x1, FldCat_cattdlyb_y1, DlgCat_soxvcat_y2, Dates_mapping) :
    cata_monit_dataframe_key = {}
    for fields in cata_monit_required_fields :
        cata_monit_dataframe_key[fields] = check_for_time_group([fields], yop)
        if "group_group" in cata_monit_dataframe_key[fields] :
            return FldCat_catgav2_x1, FldCat_cattdlyb_y1, DlgCat_soxvcat_y2, Dates_mapping
        
    x,y1,y2, dates = catalyst_monitoring(yop, date_, cata_monit_dataframe_key)
    FldCat_catgav2_x1.extend(x)
    FldCat_cattdlyb_y1.extend(y1)
    DlgCat_soxvcat_y2.extend(y2)
    Dates_mapping.extend(dates)
    return FldCat_catgav2_x1, FldCat_cattdlyb_y1, DlgCat_soxvcat_y2, Dates_mapping


# ### Misfire Monitoring

# The function get_misfire_monitoring returns a dictionary of maximum misfire monitoring values for a given date. It takes in the following arguments:
# 
# yop_misfire: A data frame containing misfire monitoring data
# date_: A date for which maximum misfire monitoring data is to be retrieved
# misfire_moni_dataframe_key: A dictionary that maps misfire monitoring fields to the corresponding column names in the yop_misfire data frame.
# The function first retrieves the maximum value of FldMsf_cmiswa2 field and its corresponding index. Then it retrieves the values of fields FldMsf_cmis24, FldMsf_crough2, FldMsf_cmis21, FldMsf_cmis22, FldMsf_cmis23 from the yop_misfire data frame for the rows with index greater than or equal to the index of the maximum FldMsf_cmiswa2 value.
# 
# The function returns a dictionary with date as key and a dictionary containing the retrieved values as values.
# 
# The misfire_monitoring_data function uses the get_misfire_monitoring function to get the misfire monitoring data for multiple dates and returns a dictionary containing the data for all the dates.

# In[5]:


def get_misfire_monitoring(yop_misfire, date_,  misfire_moni_dataframe_key) :
    date_wise_max_misfire_monit = {}
    df_misfire_monit = yop_misfire[misfire_moni_dataframe_key["FldMsf_cmiswa2"]].copy()
    df_misfire_monit.reset_index(inplace=True)
    FldMsf_cmiswa2_max = df_misfire_monit['FldMsf_cmiswa2'].max()
    data_index = df_misfire_monit.loc[df_misfire_monit["FldMsf_cmiswa2"].idxmax()]["index"]
    
    df_misfire_monit_FldMsf_cmis24 = yop_misfire[misfire_moni_dataframe_key["FldMsf_cmis24"]].copy()
    
    df_misfire_monit_FldMsf_cmis24.reset_index(inplace=True)
    FldMsf_cmis24 = df_misfire_monit_FldMsf_cmis24.loc[df_misfire_monit_FldMsf_cmis24["index"] >= data_index]["FldMsf_cmis24"].to_list()[0]
    df_misfire_monit_FldMsf_crough2 = yop_misfire[misfire_moni_dataframe_key["FldMsf_crough2"]].copy()
    df_misfire_monit_FldMsf_crough2.reset_index(inplace=True)
    FldMsf_crough2 = df_misfire_monit_FldMsf_crough2.loc[df_misfire_monit_FldMsf_crough2["index"] >= data_index]["FldMsf_crough2"].to_list()[0]
    
    df_misfire_monit_FldMsf_cmis21 = yop_misfire[misfire_moni_dataframe_key["FldMsf_cmis21"]].copy()
#     print(df_misfire_monit_FldMsf_cmis21)
    df_misfire_monit_FldMsf_cmis21.reset_index(inplace=True)
    FldMsf_cmis21 = df_misfire_monit_FldMsf_cmis21.loc[df_misfire_monit_FldMsf_cmis21["index"] >= data_index]["FldMsf_cmis21"].to_list()[0]
    
    df_misfire_monit_FldMsf_cmis22 = yop_misfire[misfire_moni_dataframe_key["FldMsf_cmis22"]].copy()
    df_misfire_monit_FldMsf_cmis22.reset_index(inplace=True)
    FldMsf_cmis22 = df_misfire_monit_FldMsf_cmis22.loc[df_misfire_monit_FldMsf_cmis22["index"] >= data_index]["FldMsf_cmis22"].to_list()[0]
    
    df_misfire_monit_FldMsf_cmis23 = yop_misfire[misfire_moni_dataframe_key["FldMsf_cmis23"]].copy()
    df_misfire_monit_FldMsf_cmis23.reset_index(inplace=True)
    FldMsf_cmis23 = df_misfire_monit_FldMsf_cmis23.loc[df_misfire_monit_FldMsf_cmis23["index"] >= data_index]["FldMsf_cmis23"].to_list()[0]
    

    date_wise_max_misfire_monit[str(date_)] = {"Max_FldMsf_cmiswa2" : FldMsf_cmiswa2_max, "FldMsf_crough2_corrosp" : FldMsf_crough2,
                                              "FldMsf_cmis21" : FldMsf_cmis21, "FldMsf_cmis22" : FldMsf_cmis22,
                                              "FldMsf_cmis23" : FldMsf_cmis23, "FldMsf_cmis24" : FldMsf_cmis24}
    return date_wise_max_misfire_monit

def misfire_monitoring_data(yop , date_, misfire_monit_requirement_fields, date_wise_max_misfire_monit) :
    misfire_moni_dataframe_key = {}
    for fields in misfire_monit_requirement_fields :
        misfire_moni_dataframe_key[fields] = check_for_time_group([fields], yop)
        if "group_group" in misfire_moni_dataframe_key[fields] :
            return date_wise_max_misfire_monit
        
    date_wise_max_misfire_monit.append(get_misfire_monitoring(yop, date_, misfire_moni_dataframe_key ))
    return date_wise_max_misfire_monit

def misfire_monitoring_dataframe(date_wise_max_misfire_monit) :
    dates = []
    Max_FldMsf_cmiswa2 = []
    FldMsf_crough2_corrosp = []
    FldMsf_cmis21 = []
    FldMsf_cmis22 = []
    FldMsf_cmis23 = []
    FldMsf_cmis24 = []
    for data in date_wise_max_misfire_monit :
        for dt in data.keys() :
            dates.append(dt)
            Max_FldMsf_cmiswa2.append(data[dt]["Max_FldMsf_cmiswa2"])
            FldMsf_crough2_corrosp.append(data[dt]["FldMsf_crough2_corrosp"])
            FldMsf_cmis21.append(data[dt]["FldMsf_cmis21"])
            FldMsf_cmis22.append(data[dt]["FldMsf_cmis22"])
            FldMsf_cmis23.append(data[dt]["FldMsf_cmis23"])
            FldMsf_cmis24.append(data[dt]["FldMsf_cmis24"])
            
    df_misfire_monit = pd.DataFrame({"MFM_Date" : dates, "Max_FldMsf_cmiswa2" : Max_FldMsf_cmiswa2, "FldMsf_crough2_corrosp" : FldMsf_crough2_corrosp
                                    , "FldMsf_cmis21" : FldMsf_cmis21, "FldMsf_cmis22" : FldMsf_cmis22, "FldMsf_cmis23": FldMsf_cmis23,
                                    "FldMsf_cmis24": FldMsf_cmis24})
    df_misfire_monit = df_misfire_monit.groupby("MFM_Date").max()
    df_misfire_monit.reset_index(inplace=True)
    return df_misfire_monit


# ### IUPR

# In[6]:


def get_monitoring_iupr(yop_iupr, date_, moni_iupr_dataframe_key, monitoring_iupr_required_fields) :
    
    date_wise_iupr_monit = {}
#     print(len(date_wise_grp))
    data_dict = {}
    for fields in monitoring_iupr_required_fields :
#             print(fields)
        df_monit_iupr = yop_iupr[moni_iupr_dataframe_key[fields]]
        if fields in df_monit_iupr.columns :
            data_dict[fields] = df_monit_iupr[fields].max()
#                 print(data_dict)
    date_wise_iupr_monit[str(date_)] = data_dict
    return date_wise_iupr_monit

def get_date_wise_monitoring_from_files(yop, date_, monitoring_iupr_required_fields, date_wise_iupr_monit) :
    
    moni_iupr_dataframe_key = {}
    for fields in monitoring_iupr_required_fields :
        moni_iupr_dataframe_key[fields] = check_for_time_group([fields], yop)
#         print(moni_iupr_dataframe_key[fields])
        if "group_group" in moni_iupr_dataframe_key[fields] :
            
            return date_wise_iupr_monit
    date_wise_iupr_monit.append(get_monitoring_iupr(yop, date_, moni_iupr_dataframe_key, monitoring_iupr_required_fields))
    return date_wise_iupr_monit   

def get_dataframe_from_date_wise_monit(date_wise_iupr_monit, Dates_str) :
    dates = []
    data_dict = {}
    for data in date_wise_iupr_monit :
        for dt in data.keys() :
            dates.append(dt)
            for fields in data[dt] :
                if fields in data_dict :
                    data_dict[fields].append(data[dt][fields])
                else :
                    data_dict[fields] = [data[dt][fields]]
    data_dict[Dates_str] = dates
    df_monit_iupr = pd.DataFrame(data_dict)
    df_monit_iupr = df_monit_iupr.groupby(Dates_str).max()
    df_monit_iupr.reset_index(inplace=True)
    df_monit_iupr
    return df_monit_iupr

def get_ratio(num, deno) :
    if deno == 0 :
        return 0
    else :
        return num/deno


# ### General IUPR

# In[7]:


def get_gen_IUPR_monitoring(yop_gen_iupr, date_,  gen_IUPR_moni_dataframe_key, gen_iupr_fields) :
    
    date_wise_max_misfire_monit = {}
    data = {}
    for fields in gen_iupr_fields :
        if fields in gen_IUPR_moni_dataframe_key : 
            df_gen_iupr = yop_gen_iupr[gen_IUPR_moni_dataframe_key[fields]][[fields]]
            data["Max_" + str(fields)] = df_gen_iupr[fields].max()
        else :
            data["Max_" + str(fields)] = None
        
    date_wise_max_misfire_monit[str(date_)] = data
    return date_wise_max_misfire_monit

def gen_iupr_monitoring_data(yop , date_, gen_iupr_requirement_fields, date_wise_max_gen_iupr) :
    gen_iupr_dataframe_key = {}
    count = 0
    for fields in gen_iupr_requirement_fields :
        check_key = check_for_time_group([fields], yop)
        
        if "group_group" in check_key :
            count = count + 1
        else :
            gen_iupr_dataframe_key[fields] = check_key
    if count == len(gen_iupr_requirement_fields) :
        return date_wise_max_gen_iupr
    date_wise_max_gen_iupr.append(get_gen_IUPR_monitoring(yop, date_,  gen_iupr_dataframe_key, gen_iupr_requirement_fields))
    return date_wise_max_gen_iupr

def gen_iupr_monitoring_dataframe(date_wise_max_gen_iupr) :
    dates = []
    DgsRate_rateigcnt = []
    DgsRate_rategenden = []
    DgsRate_cntnumvvtr = []
    DgsRate_cntnumexvvta = []
    DgsRate_cntnumegrcl = []
    for data in date_wise_max_gen_iupr :
        for dt in data.keys() :
            dates.append(dt)
            DgsRate_rateigcnt.append(data[dt]["Max_DgsRate_rateigcnt"])
            DgsRate_rategenden.append(data[dt]["Max_DgsRate_rategenden"])
            DgsRate_cntnumvvtr.append(data[dt]["Max_DgsRate_cntnumvvtr"])
            DgsRate_cntnumexvvta.append(data[dt]["Max_DgsRate_cntnumexvvta"])
            DgsRate_cntnumegrcl.append(data[dt]["Max_DgsRate_cntnumegrcl"])
            
    df_gen_iupr = pd.DataFrame({"IUPR_GEN_Date" : dates, "Max_DgsRate_rateigcnt" : DgsRate_rateigcnt, "Max_DgsRate_rategenden" : DgsRate_rategenden,
                                    "Max_DgsRate_cntnumvvtr": DgsRate_cntnumvvtr, "Max_DgsRate_cntnumexvvta": DgsRate_cntnumexvvta,
                                    "Max_DgsRate_cntnumegrcl":DgsRate_cntnumegrcl})
    df_gen_iupr = df_gen_iupr.groupby("IUPR_GEN_Date").max()
    df_gen_iupr.reset_index(inplace=True)
    return df_gen_iupr


# ### FO2 Monitoring

# In[8]:


def get_index_mapping_fo2(FldOxpc_xooxcss_list,  index_time_list, index_mapping) :
    zero_to_one_check = False
    for i in range(0, len(FldOxpc_xooxcss_list)) :
        if FldOxpc_xooxcss_list[i] == 0 and zero_to_one_check:
            zero_to_one_check = False
        elif FldOxpc_xooxcss_list[i] == 1 and not zero_to_one_check:
            zero_to_one_check = True
            index_mapping.append({"index" : index_time_list[i]})
    
    return index_mapping

def fo2_monitoring(yop, date_,fo2_monit_dataframe_key, fo2_monit_required_fields) :
    index_mapping = []
    x = []
    date_mapping = []
    #### FldOxpc_xooxcss 
    time_key_FldOxpc_xooxcss = fo2_monit_dataframe_key["FldOxpc_xooxcss"].split("_group")[0]
    fo2_monit_required_fields_FldOxpc_xooxcss = ["FldOxpc_xooxcss"]
    fo2_monit_required_fields_FldOxpc_xooxcss.append(time_key_FldOxpc_xooxcss)
    df_fo2_monitoring_FldOxpc_xooxcss = yop[fo2_monit_dataframe_key["FldOxpc_xooxcss"]][fo2_monit_required_fields_FldOxpc_xooxcss]
    
    
    FldOxpc_xooxcss_list = df_fo2_monitoring_FldOxpc_xooxcss["FldOxpc_xooxcss"].to_list()
    index_time_list_FldOxpc_xooxcss = []
        
    for data in df_fo2_monitoring_FldOxpc_xooxcss[time_key_FldOxpc_xooxcss] :
        index_time_list_FldOxpc_xooxcss.append(data)
        
        
    #### FldOxpc_xooxcsf
    time_key_FldOxpc_xooxcsf = fo2_monit_dataframe_key["FldOxpc_xooxcsf"].split("_group")[0]
    fo2_monit_required_fields_FldOxpc_xooxcsf = ["FldOxpc_xooxcsf"]
    fo2_monit_required_fields_FldOxpc_xooxcsf.append(time_key_FldOxpc_xooxcsf)
    df_fo2_monitoring_FldOxpc_xooxcsf = yop[fo2_monit_dataframe_key["FldOxpc_xooxcsf"]][fo2_monit_required_fields_FldOxpc_xooxcsf]
    
    
    FldOxpc_xooxcsf_list = df_fo2_monitoring_FldOxpc_xooxcsf["FldOxpc_xooxcsf"].to_list()
    index_time_list_FldOxpc_xooxcsf = []
        
    for data in df_fo2_monitoring_FldOxpc_xooxcsf[time_key_FldOxpc_xooxcsf] :
        index_time_list_FldOxpc_xooxcsf.append(data)
    
    
    
    index_mapping = get_index_mapping_fo2(FldOxpc_xooxcss_list, index_time_list_FldOxpc_xooxcss, index_mapping)
    index_mapping = get_index_mapping_fo2(FldOxpc_xooxcsf_list, index_time_list_FldOxpc_xooxcsf, index_mapping)
#         print(index_mapping)
        
    for mapping in index_mapping :
        x.append(mapping["index"])
        date_mapping.append(date_)
                
    return x, date_mapping

def fo2_mapping_data(yop,date_, x,dates, fo2_monit_dataframe_key,fo2_monit_required_fields) :
    y_opt = []
    time_key = fo2_monit_dataframe_key["FldOxpc_tfboxsavsdl"].split("_group")[0]
    fo2_monit_required_fields_2_1 = ["FldOxpc_tfboxsavsdl"]
    fo2_monit_required_fields_2_1.append(time_key)
    
    df_fo2_monitoring_all = yop[fo2_monit_dataframe_key["FldOxpc_tfboxsavsdl"]][fo2_monit_required_fields_2_1]
    
    for ind in range(0,len(x) ) :
        y_opt.append(df_fo2_monitoring_all.loc[(df_fo2_monitoring_all[time_key] >= x[ind])]["FldOxpc_tfboxsavsdl"].to_list()[0])
    return y_opt

def fo2_monitoring_data(yop, date_, fo2_monit_required_fields, index_mapping_x, FldOxpc_tfboxsavsdl_y, Dates_mapping):
    fo2_monit_dataframe_key = {}
    for fields in fo2_monit_required_fields :
        fo2_monit_dataframe_key[fields] = check_for_time_group([fields], yop)
#         print(fo2_monit_dataframe_key[fields])
        if "group_group" in fo2_monit_dataframe_key[fields] :
            return index_mapping_x, FldOxpc_tfboxsavsdl_y, Dates_mapping
    x, dates = fo2_monitoring(yop, date_, fo2_monit_dataframe_key,fo2_monit_required_fields )
    y = fo2_mapping_data(yop,date_, x,dates, fo2_monit_dataframe_key,fo2_monit_required_fields)
    index_mapping_x.extend(x)
    FldOxpc_tfboxsavsdl_y.extend(y)
    Dates_mapping.extend(dates)
    return index_mapping_x, FldOxpc_tfboxsavsdl_y, Dates_mapping


# ### RO2 Monitoring

# In[9]:


def get_index_mapping_ro2(FldOxscr_xosoxcr1f_list, FldOxscr_xosoxcr1s_list, index_time_list) :
    zero_to_one_check = False
    index_mapping = []
    mapping_FldOxscr_xosoxcr1f = []
    for i in range(0, len(FldOxscr_xosoxcr1s_list)) :
        if FldOxscr_xosoxcr1s_list[i] == 0 and zero_to_one_check:
            zero_to_one_check = False
        elif FldOxscr_xosoxcr1s_list[i] == 1 and not zero_to_one_check:
            zero_to_one_check = True
            index_mapping.append({"index" : index_time_list[i]})
    for i in range(0, len(FldOxscr_xosoxcr1f_list)) :
        if FldOxscr_xosoxcr1f_list[i] == 1 :
            mapping_FldOxscr_xosoxcr1f.append({"index" : index_time_list[i]})
#     print(index_mapping, mapping_FldOxscr_xosoxcr1f)
    return index_mapping, mapping_FldOxscr_xosoxcr1f

def ro2_monitoring(yop, date_, ro2_monit_dataframe_key,ro2_monit_required_fields_1) :
    time_key = ro2_monit_dataframe_key.split("_group")[0]
    ro2_monit_required_fields_1_1 = ro2_monit_required_fields_1[:]
    ro2_monit_required_fields_1_1.append(time_key)
    df_ro2_monitoring_all = yop[ro2_monit_dataframe_key][ro2_monit_required_fields_1_1]
    
    df_ro2_monitoring_all.reset_index(inplace=True)
    df_ro2_monitoring_all['just_date'] = df_ro2_monitoring_all['index'].dt.date
    df_ro2_monitoring_all.set_index("index", inplace=True)
    date_wise_grp = df_ro2_monitoring_all.groupby("just_date")
    x = []
    date_mapping = []
    FldOxscr_xosoxcr1f_index = []
    FldOxscr_xosoxcr1f_data_mapping = []
   
    df_ro2_monitoring = df_ro2_monitoring_all
    FldOxscr_xosoxcr1f_list = df_ro2_monitoring["FldOxscr_xosoxcr1f"].to_list()

    FldOxscr_xosoxcr1s_list = df_ro2_monitoring["FldOxscr_xosoxcr1s"].to_list()
    index_time_list = []
    for data in df_ro2_monitoring[time_key] :
        index_time_list.append(data) 
        
    index_mapping, mapping_FldOxscr_xosoxcr1f = get_index_mapping_ro2(FldOxscr_xosoxcr1f_list, FldOxscr_xosoxcr1s_list, index_time_list)

    for mapping in index_mapping :

        x.append(mapping["index"])
        date_mapping.append(date_)
            
    for mapping in mapping_FldOxscr_xosoxcr1f :

        FldOxscr_xosoxcr1f_index.append(mapping["index"])
        FldOxscr_xosoxcr1f_data_mapping.append(date_)    
            
    return x, date_mapping, FldOxscr_xosoxcr1f_index, FldOxscr_xosoxcr1f_data_mapping

def ro2_mapping_data(yop, date_, x,dates, FldOxscr_xosoxcr1f_index, FldOxscr_xosoxcr1f_data_mapping, ro2_monit_dataframe_key_2, ro2_monit_required_fields_2) :
    y_opt = []
    time_key = ro2_monit_dataframe_key_2.split("_group")[0]
    ro2_monit_required_fields_2_1 = ro2_monit_required_fields_2[:]
    ro2_monit_required_fields_2_1.append(time_key)
    df_ro2_monitoring_all = yop[ro2_monit_dataframe_key_2][ro2_monit_required_fields_2_1]
    
    
    df_ro2_monitoring_all.reset_index(inplace=True)
    df_ro2_monitoring_all['just_date'] = df_ro2_monitoring_all['index'].dt.date
    df_ro2_monitoring_all.set_index("index", inplace=True)
    for ind in range(0,len(x) ) :
        selected_df = df_ro2_monitoring_all.loc[(df_ro2_monitoring_all[time_key] > x[ind])]
        y_opt.append(selected_df["FldOxscr_cosocr1d"].to_list()[0])
    for ind in range(0, len(FldOxscr_xosoxcr1f_index)) :
        selected_df = df_ro2_monitoring_all.loc[(df_ro2_monitoring_all[time_key] > x[ind]) & (df_ro2_monitoring_all["FldOxscr_xosoxcr1e"] == 1)]
        y_opt.append(selected_df["FldOxscr_cosocr1d"].to_list()[0])
    return y_opt

def ro2_monitoring_data(yop, date_, ro2_monit_required_fields_1, ro2_monit_required_fields_2, index_mapping_x, FldOxscr_cosocr1d_y, Dates_mapping) :
    
    ro2_monit_dataframe_key_1 = check_for_time_group(ro2_monit_required_fields_1, yop)

    ro2_monit_dataframe_key_2 = check_for_time_group(ro2_monit_required_fields_2, yop)
#     print(ro2_monit_dataframe_key_1, ro2_monit_dataframe_key_2)
    if "group_group" not in ro2_monit_dataframe_key_1 and "group_group" not in ro2_monit_dataframe_key_2:
        x, dates, FldOxscr_xosoxcr1f_index, FldOxscr_xosoxcr1f_data_mapping = ro2_monitoring(yop, date_,ro2_monit_dataframe_key_1, ro2_monit_required_fields_1)
        y = ro2_mapping_data(yop, date_, x,dates, FldOxscr_xosoxcr1f_index, FldOxscr_xosoxcr1f_data_mapping, ro2_monit_dataframe_key_2, ro2_monit_required_fields_2)
        index_mapping_x.extend(x)
        FldOxscr_cosocr1d_y.extend(y)
        Dates_mapping.extend(dates)
    return index_mapping_x, FldOxscr_cosocr1d_y, Dates_mapping



# ### Fuel System

# In[10]:


def get_max_fuel_system_monitoring(fuel_monit_dataframe_key, yop, fuel_monit_required_fields, date_) :
    df_fuel_monitoring_all = yop[fuel_monit_dataframe_key][fuel_monit_required_fields]
    
    df_fuel_monitoring_all.reset_index(inplace=True)
    df_fuel_monitoring_all['just_date'] = df_fuel_monitoring_all['index'].dt.date
    df_fuel_monitoring_all.set_index("index", inplace=True)
    date_wise_grp = df_fuel_monitoring_all.groupby("just_date")
# #     print("______________")
    date_mapping = {}
    df_fuel_monitoring = df_fuel_monitoring_all
    for fields in fuel_monit_required_fields :
#         print(df_fuel_monitoring[fields].max())
        date_mapping[fields] = df_fuel_monitoring[fields].max()
    date_mapping["Date"] = date_
    return date_mapping

def fuel_system_monitoring(yop, date_,fuel_monit_required_fields_1, fuel_monit_required_fields_2, index_mapping_x, FldFue_cofslean, FldFue_cofsrich, FldFue_cofsleangas, FldFue_cofsrichgas) :
    
    fuel_monit_dataframe_key_1 = check_for_time_group(fuel_monit_required_fields_1, yop)

    fuel_monit_dataframe_key_2 = check_for_time_group(fuel_monit_required_fields_2, yop)
    data_mapping = {}
    if "group_group" not in fuel_monit_dataframe_key_1 :
        data_mapping = get_max_fuel_system_monitoring(fuel_monit_dataframe_key_1, yop, fuel_monit_required_fields_1, date_)
#         print("**********",data_mapping,"******************")
        if len(data_mapping) > 0 :
            for data in data_mapping :
#                 print(data)
                if 'FldFue_cofslean' == data :
                    FldFue_cofslean.append(str(data_mapping[data]))
                    FldFue_cofsleangas.append("")

                if 'FldFue_cofsrich' == data :
                    FldFue_cofsrich.append(str(data_mapping[data]))
                    FldFue_cofsrichgas.append("")

                if 'Date' == data :
                    index_mapping_x.append(data_mapping[data])

    if "group_group" not in fuel_monit_dataframe_key_2 :
        data_mapping = get_max_fuel_system_monitoring(fuel_monit_dataframe_key_2, yop, fuel_monit_required_fields_2, date_)
#         print("***************", data_mapping)
        if len(data_mapping) > 0 :
            for data in data_mapping :
#                 print(data)
                if 'FldFue_cofsleangas' == data :
                    FldFue_cofsleangas.append(str(data_mapping[data]))
                    FldFue_cofslean.append("")

                if 'FldFue_cofsrichgas' == data :
                    FldFue_cofsrichgas.append(str(data_mapping[data]))
                    FldFue_cofsrich.append("")

                if 'Date' == data :
                    index_mapping_x.append(data_mapping[data])

#         print("___________FldFue_cofslean:", FldFue_cofslean)
    return index_mapping_x, FldFue_cofslean, FldFue_cofsrich, FldFue_cofsleangas, FldFue_cofsrichgas


# ### VVTi Monitoring

# In[11]:


def get_index_mapping_vvti(FldVti_xovvtrs_list, FldVti_vtdobdsm_list) :
    zero_to_one_check = False
    index_mapping = []
    for i in range(0, len(FldVti_xovvtrs_list)) :
        
        if FldVti_xovvtrs_list[i] == 0 and zero_to_one_check:
            zero_to_one_check = False
            
        elif FldVti_xovvtrs_list[i] == 1 and not zero_to_one_check:
            zero_to_one_check = True
            
            index_mapping.append({"index" : FldVti_vtdobdsm_list[i]})
    return index_mapping


def vvti_monitoring(yop, date_, vvti_monit_dataframe_key, vvti_monit_required_fields) :
    time_key_FldVti_xovvtrs = vvti_monit_dataframe_key["FldVti_xovvtrs"].split("_group")[0]
    vvti_monit_required_fields_FldVti_xovvtrs = ["FldVti_xovvtrs"]
    vvti_monit_required_fields_FldVti_xovvtrs.append(time_key_FldVti_xovvtrs)
    
    df_vvti_monitoring_FldVti_xovvtrs = yop[vvti_monit_dataframe_key["FldVti_xovvtrs"]][vvti_monit_required_fields_FldVti_xovvtrs]
    date_mapping = []
    FldVti_vtdobdsm = []
    FldVti_xovvtrs_list = df_vvti_monitoring_FldVti_xovvtrs["FldVti_xovvtrs"].to_list()

    time_key_FldVti_vtdobdsm = vvti_monit_dataframe_key["FldVti_vtdobdsm"].split("_group")[0]
    vvti_monit_required_fields_FldVti_vtdobdsm = ["FldVti_vtdobdsm"]
    vvti_monit_required_fields_FldVti_vtdobdsm.append(time_key_FldVti_vtdobdsm)
    
    df_vvti_monitoring_FldVti_vtdobdsm = yop[vvti_monit_dataframe_key["FldVti_vtdobdsm"]][vvti_monit_required_fields_FldVti_vtdobdsm]
    
    FldVti_vtdobdsm_list = df_vvti_monitoring_FldVti_vtdobdsm["FldVti_vtdobdsm"].to_list()
#     print(list(set(FldVti_vtdobdsm_list)))
#     print(list(set(FldVti_xovvtrs_list)))
    index_mapping = get_index_mapping_vvti(FldVti_xovvtrs_list, FldVti_vtdobdsm_list)
#     print(len(index_mapping))
    for mapping in index_mapping :
        FldVti_vtdobdsm.append(mapping["index"])
        date_mapping.append(date_)
#         FldVti_vtdobdsm.append(df_vvti_monitoring_all.loc[df_vvti_monitoring_all[time_key] == mapping["index"]]["FldVti_vtdobdsm"].to_list()[0] )
#     print(x, index_mapping)
    return  date_mapping, FldVti_vtdobdsm



def vvti_monitoring_data(yop,date_, vvti_monit_required_fields, index_mapping_x, 
                        FldVti_vtdobdsm_y, Dates_mapping) :
    
    vvti_monit_dataframe_key = {}
    for fields in vvti_monit_required_fields :
        vvti_monit_dataframe_key[fields] = check_for_time_group([fields], yop)
#         print(vvti_monit_dataframe_key[fields])
        if "group_group" in vvti_monit_dataframe_key[fields] :
            
            return index_mapping_x, FldVti_vtdobdsm_y, Dates_mapping
    
    dates, FldVti_vtdobdsm = vvti_monitoring(yop, date_, vvti_monit_dataframe_key,  vvti_monit_required_fields)
    index_mapping_x.extend(dates)
    FldVti_vtdobdsm_y.extend(FldVti_vtdobdsm)
    Dates_mapping.extend(dates)
    return index_mapping_x, FldVti_vtdobdsm_y, Dates_mapping


# ### VVTe Monitoring

# In[12]:


def get_index_mapping_vvte(FldVte_xovvtaf_ex_list, FldVte_vtdobdsm_ex_list) :
    zero_to_one_check = False
    index_mapping = []
    for i in range(0, len(FldVte_xovvtaf_ex_list)) :
        if FldVte_xovvtaf_ex_list[i] == 0 and zero_to_one_check:
            zero_to_one_check = False
        elif FldVte_xovvtaf_ex_list[i] == 1 and not zero_to_one_check:
            zero_to_one_check = True
            index_mapping.append({"index" : FldVte_vtdobdsm_ex_list[i]})
    return index_mapping


def vvte_monitoring(yop, date_, vvte_monit_dataframe_key, vvte_monit_required_fields_1) :
    time_key_FldVte_xovvtas_ex = vvte_monit_dataframe_key["FldVte_xovvtas_ex"].split("_group")[0]
    vvte_monit_required_fields_FldVte_xovvtas_ex = ["FldVte_xovvtas_ex"]
    vvte_monit_required_fields_FldVte_xovvtas_ex.append(time_key_FldVte_xovvtas_ex)
    
    df_vvte_monitoring_FldVte_xovvtas_ex = yop[vvte_monit_dataframe_key["FldVte_xovvtas_ex"]][vvte_monit_required_fields_FldVte_xovvtas_ex]
    
    date_mapping = []
    FldVte_vtdobdsm_ex = []
    FldVte_xovvtas_ex_list = df_vvte_monitoring_FldVte_xovvtas_ex["FldVte_xovvtas_ex"].to_list()
    
    time_key_FldVte_vtdobdsm_ex = vvte_monit_dataframe_key["FldVte_vtdobdsm_ex"].split("_group")[0]
    vvte_monit_required_fields_FldVte_vtdobdsm_ex = ["FldVte_vtdobdsm_ex"]
    vvte_monit_required_fields_FldVte_vtdobdsm_ex.append(time_key_FldVte_vtdobdsm_ex)
    
    df_vvte_monitoring_all = yop[vvte_monit_dataframe_key["FldVte_vtdobdsm_ex"]][vvte_monit_required_fields_FldVte_vtdobdsm_ex]
    
    
    
    FldVte_vtdobdsm_ex_list = df_vvte_monitoring_all["FldVte_vtdobdsm_ex"].to_list()
    
    index_mapping = get_index_mapping_vvte(FldVte_xovvtas_ex_list, FldVte_vtdobdsm_ex_list)
#     print(len(index_mapping))
    for mapping in index_mapping :
        FldVte_vtdobdsm_ex.append(mapping["index"])
        date_mapping.append(date_)
#         FldVte_vtdobdsm_ex.append(df_vvti_monitoring_all.loc[df_vvti_monitoring_all[time_key] == mapping["index"]]["FldVte_vtdobdsm_ex"].to_list()[0] )
    return  date_mapping, FldVte_vtdobdsm_ex




def vvte_monitoring_data(yop, date_, vvte_monit_required_fields, index_mapping_x, FldVte_vtdobdsm_ex_y, Dates_mapping) :
    
    vvte_monit_dataframe_key = {}
    for fields in vvte_monit_required_fields :
        vvte_monit_dataframe_key[fields] = check_for_time_group([fields], yop)
#         print(vvte_monit_dataframe_key[fields])
        if "group_group" in vvte_monit_dataframe_key[fields] :
            
            return index_mapping_x, FldVte_vtdobdsm_ex_y, Dates_mapping
    dates, FldVte_vtdobdsm_ex = vvte_monitoring(yop, date_, vvte_monit_dataframe_key, vvte_monit_required_fields)
    index_mapping_x.extend(dates)
    FldVte_vtdobdsm_ex_y.extend(FldVte_vtdobdsm_ex)
    Dates_mapping.extend(dates)
    return index_mapping_x, FldVte_vtdobdsm_ex_y, Dates_mapping



# ### CrCmi Monitoring

# In[13]:


def crcmi_monitoring(yop, date_, crcmi_monit_dataframe_key, crcmi_monitoring_fields) :
    df_crcmi_monitoring_all = yop[crcmi_monit_dataframe_key][crcmi_monitoring_fields]
    x_min_value = []
    x_max_value = []
    date_mapping = []
    df_crcmi_monitoring_all = df_crcmi_monitoring_all.loc[(df_crcmi_monitoring_all["FldCrcmi_xocrcmex"] == 1) & (df_crcmi_monitoring_all["FldCrcmi_xVtiClnExp"] == 1)]
    min_value = df_crcmi_monitoring_all["FldInf_VtiOfsAdvance_bank_[0]"].min()
    max_value = df_crcmi_monitoring_all["FldInf_VtiOfsAdvance_bank_[0]"].max()
    date_mapping.append(date_)
    x_min_value.append(min_value)
    x_max_value.append(max_value)
    return x_min_value, x_max_value, date_mapping

def crcmi_monitoring_data(yop,date_, crcmi_monitoring_fields, FldInf_VtiOfsAdvance_bank_min_value_y, FldInf_VtiOfsAdvance_bank_max_value_y,  Dates_mapping) :
    
    crcmi_monit_dataframe_key = check_for_time_group(crcmi_monitoring_fields, yop)
#     print(crcmi_monit_dataframe_key)
    if "group_group" not in crcmi_monit_dataframe_key:
        x_min_value,x_max_value, dates = crcmi_monitoring(yop, date_, crcmi_monit_dataframe_key, crcmi_monitoring_fields)
        FldInf_VtiOfsAdvance_bank_min_value_y.extend(x_min_value)
        FldInf_VtiOfsAdvance_bank_max_value_y.extend(x_max_value)
        Dates_mapping.extend(dates)
    return FldInf_VtiOfsAdvance_bank_min_value_y, FldInf_VtiOfsAdvance_bank_max_value_y,  Dates_mapping



# ### CrCme Monitoring

# In[14]:


def crcme_monitoring(yop, date_, crcme_monit_dataframe_key, crcme_monitoring_fields) :
    df_crcme_monitoring = yop[crcme_monit_dataframe_key][crcme_monitoring_fields]
    x_min_value = []
    x_max_value = []
    date_mapping = []
    df_crcme_monitoring = df_crcme_monitoring.loc[(df_crcme_monitoring["FldCrcme_xocrcmex_ex"] == 1) & (df_crcme_monitoring["FldCrcme_xVteClnExp"] == 1)]
    min_value = df_crcme_monitoring["FldInf_VteOfsAdvance_bank_[0]"].min()
    max_value = df_crcme_monitoring["FldInf_VteOfsAdvance_bank_[0]"].max()
        
        
    date_mapping.append(date_)
    x_min_value.append(min_value)
    x_max_value.append(max_value)
#     print(min_value, max_value)
    return x_min_value, x_max_value, date_mapping
 
def crcme_monitoring_data(yop, date_, crcme_monitoring_fields, FldInf_VteOfsAdvance_bank_min_value_y, FldInf_VteOfsAdvance_bank_max_value_y, Dates_mapping) :
    
    crcme_monit_dataframe_key = check_for_time_group(crcme_monitoring_fields, yop)
#     print(crcme_monit_dataframe_key)
    if "group_group" not in crcme_monit_dataframe_key:
        x_min_value,x_max_value, dates = crcme_monitoring(yop, date_, crcme_monit_dataframe_key, crcme_monitoring_fields)
        FldInf_VteOfsAdvance_bank_min_value_y.extend(x_min_value)
        FldInf_VteOfsAdvance_bank_max_value_y.extend(x_max_value)
        Dates_mapping.extend(dates)
    return FldInf_VteOfsAdvance_bank_min_value_y, FldInf_VteOfsAdvance_bank_max_value_y, Dates_mapping


# ### EGR Monitoring

# In[15]:


def get_index_mapping_egr_FldEgrc_xoegrcls(FldEgrc_xoegrcls_list,  index_time_list, index_mapping) :
    zero_to_one_check = False
    
    for i in range(0, len(FldEgrc_xoegrcls_list)) :
        if FldEgrc_xoegrcls_list[i] == 0 and zero_to_one_check:
            zero_to_one_check = False
        elif FldEgrc_xoegrcls_list[i] == 1 and not zero_to_one_check:
            zero_to_one_check = True
            index_mapping.append({"index" : index_time_list[i]})
    
    return index_mapping
def get_index_mapping_egr_FldEgrc_xoegrclf( FldEgrc_xoegrclf_list, index_time_list, index_mapping) :
    zero_to_one_check = False
    for i in range(0, len(FldEgrc_xoegrclf_list)) :
        if FldEgrc_xoegrclf_list[i] == 0 and zero_to_one_check:
            zero_to_one_check = False
        elif FldEgrc_xoegrclf_list[i] == 1 and not zero_to_one_check:
            zero_to_one_check = True
            index_mapping.append({"index" : index_time_list[i]})
    return index_mapping
def egr_monitoring(yop, date_, egr_monit_dataframe_key, egr_monitoring_fields) :
    x = []
    date_mapping = []
    
    time_key_FldEgrc_xoegrcls = egr_monit_dataframe_key["FldEgrc_xoegrcls"].split("_group")[0]
    egr_monitoring_fields_FldEgrc_xoegrcls = ["FldEgrc_xoegrcls"]
    egr_monitoring_fields_FldEgrc_xoegrcls.append(time_key_FldEgrc_xoegrcls)
    df_egr_monitoring_FldEgrc_xoegrcls = yop[egr_monit_dataframe_key["FldEgrc_xoegrcls"]][egr_monitoring_fields_FldEgrc_xoegrcls]
    
    index_time_list_FldEgrc_xoegrcls = []
    for data in df_egr_monitoring_FldEgrc_xoegrcls[time_key_FldEgrc_xoegrcls] :
        index_time_list_FldEgrc_xoegrcls.append(data) 
    
    FldEgrc_xoegrcls_list = df_egr_monitoring_FldEgrc_xoegrcls["FldEgrc_xoegrcls"].to_list()

    time_key_FldEgrc_xoegrclf = egr_monit_dataframe_key["FldEgrc_xoegrclf"].split("_group")[0]
    egr_monitoring_fields_FldEgrc_xoegrclf = ["FldEgrc_xoegrclf"]
    egr_monitoring_fields_FldEgrc_xoegrclf.append(time_key_FldEgrc_xoegrclf)
    df_egr_monitoring_FldEgrc_xoegrclf = yop[egr_monit_dataframe_key["FldEgrc_xoegrclf"]][egr_monitoring_fields_FldEgrc_xoegrclf]
    
    index_time_list_FldEgrc_xoegrclf = []
    for data in df_egr_monitoring_FldEgrc_xoegrclf[time_key_FldEgrc_xoegrclf] :
        index_time_list_FldEgrc_xoegrclf.append(data) 
       
        
    FldEgrc_xoegrclf_list = df_egr_monitoring_FldEgrc_xoegrclf["FldEgrc_xoegrclf"].to_list()
    index_mapping = []
    index_mapping = get_index_mapping_egr_FldEgrc_xoegrcls(FldEgrc_xoegrcls_list,  index_time_list_FldEgrc_xoegrcls, index_mapping)
    index_mapping = get_index_mapping_egr_FldEgrc_xoegrclf( FldEgrc_xoegrclf_list, index_time_list_FldEgrc_xoegrclf, index_mapping) 

        
    for mapping in index_mapping :

        x.append(mapping["index"])
        date_mapping.append(date_)
    return x, date_mapping

def egr_mapping_data(yop, date_,  x,dates, egr_monit_dataframe_key, egr_monitoring_fields) :
    y_FldEgrc_odpmegr = []
    y_FldEgrc_oegrcdpml = []
    time_key_FldEgrc_odpmegr = egr_monit_dataframe_key["FldEgrc_odpmegr"].split("_group")[0]
    egr_monitoring_fields_FldEgrc_odpmegr = ["FldEgrc_odpmegr"]
    egr_monitoring_fields_FldEgrc_odpmegr.append(time_key_FldEgrc_odpmegr)
    df_egr_monitoring_FldEgrc_odpmegr = yop[egr_monit_dataframe_key["FldEgrc_odpmegr"]][egr_monitoring_fields_FldEgrc_odpmegr]
    
    time_key_FldEgrc_oegrcdpml = egr_monit_dataframe_key["FldEgrc_oegrcdpml"].split("_group")[0]
    egr_monitoring_fields_FldEgrc_oegrcdpml = ["FldEgrc_oegrcdpml"]
    egr_monitoring_fields_FldEgrc_oegrcdpml.append(time_key_FldEgrc_oegrcdpml)
    df_egr_monitoring_FldEgrc_oegrcdpml = yop[egr_monit_dataframe_key["FldEgrc_oegrcdpml"]][egr_monitoring_fields_FldEgrc_oegrcdpml]
    
    for ind in range(0,len(x) ) :
        selected_df_FldEgrc_odpmegr = df_egr_monitoring_FldEgrc_odpmegr.loc[(df_egr_monitoring_FldEgrc_odpmegr[time_key_FldEgrc_odpmegr] > x[ind])]
        selected_df_FldEgrc_oegrcdpml = df_egr_monitoring_FldEgrc_oegrcdpml.loc[(df_egr_monitoring_FldEgrc_oegrcdpml[time_key_FldEgrc_oegrcdpml] > x[ind])]
        y_FldEgrc_odpmegr.append(selected_df_FldEgrc_odpmegr["FldEgrc_odpmegr"].to_list()[0])
        y_FldEgrc_oegrcdpml.append(selected_df_FldEgrc_oegrcdpml["FldEgrc_oegrcdpml"].to_list()[0])
    return y_FldEgrc_odpmegr, y_FldEgrc_oegrcdpml


def egr_monitoring_data(yop, date_, egr_monitoring_fields, FldEgrc_odpmegr, FldEgrc_oegrcdpml, Dates_mapping) :
    
    egr_monit_dataframe_key = {}
    for fields in egr_monitoring_fields :
        egr_monit_dataframe_key[fields] = check_for_time_group([fields], yop)
#         print(egr_monit_dataframe_key[fields])
        if "group_group" in egr_monit_dataframe_key[fields] :
            
            return FldEgrc_odpmegr, FldEgrc_oegrcdpml, Dates_mapping
    x, date_mapping = egr_monitoring(yop, date_, egr_monit_dataframe_key, egr_monitoring_fields)
    y_FldEgrc_odpmegr, y_FldEgrc_oegrcdpml = egr_mapping_data(yop, date_, x,date_mapping, egr_monit_dataframe_key, egr_monitoring_fields) 
    FldEgrc_odpmegr.extend(y_FldEgrc_odpmegr)
    FldEgrc_oegrcdpml.extend(y_FldEgrc_oegrcdpml)
    Dates_mapping.extend(date_mapping)
    return FldEgrc_odpmegr, FldEgrc_oegrcdpml, Dates_mapping


# ### Throttle Adaptation

# In[16]:


def throttle_adaptation_monitoring(yop, date_, throttle_monit_dataframe_key,throttle_adaption_monitoring_fields ) :
    date_wise_max_throttle_adaptation_ = {}
    df_throttle_adaptation_monitoring_all = yop[throttle_monit_dataframe_key][throttle_adaption_monitoring_fields]

    max_value_AirFbk_AdpFlow00_BU = df_throttle_adaptation_monitoring_all["AirFbk_AdpFlow00_BU"].max()
    max_value_AirFbk_AdpFlow01_BU = df_throttle_adaptation_monitoring_all["AirFbk_AdpFlow01_BU"].max()
    max_value_AirFbk_AdpFlow02_BU = df_throttle_adaptation_monitoring_all[ "AirFbk_AdpFlow02_BU"].max()
    max_value_AirFbk_AdpFlow03_BU = df_throttle_adaptation_monitoring_all["AirFbk_AdpFlow03_BU"].max()
    max_value_AirFbk_AdpFlow04_BU = df_throttle_adaptation_monitoring_all[ "AirFbk_AdpFlow04_BU"].max()
    max_value_AirFbk_AdpFlow05_BU = df_throttle_adaptation_monitoring_all["AirFbk_AdpFlow05_BU"].max()
    max_value_AirFbk_AdpFlow06_BU = df_throttle_adaptation_monitoring_all[ "AirFbk_AdpFlow06_BU"].max()
    date_wise_max_throttle_adaptation_[str(date_)] = {"AirFbk_AdpFlow00_BU" : max_value_AirFbk_AdpFlow00_BU, 
                                                       "AirFbk_AdpFlow01_BU" : max_value_AirFbk_AdpFlow01_BU,
                                           "AirFbk_AdpFlow02_BU" : max_value_AirFbk_AdpFlow02_BU, 
                                                       "AirFbk_AdpFlow03_BU" : max_value_AirFbk_AdpFlow03_BU,
                                           "AirFbk_AdpFlow04_BU" : max_value_AirFbk_AdpFlow04_BU,
                                                    
                                                      "AirFbk_AdpFlow05_BU" : max_value_AirFbk_AdpFlow05_BU,
                                                     "AirFbk_AdpFlow06_BU" : max_value_AirFbk_AdpFlow06_BU}
#     print(date_wise_max_throttle_adaptation)
    return date_wise_max_throttle_adaptation_


def throttle_adaptation_data_monitoring(yop, date_, throttle_adaption_monitoring_fields, date_wise_max_throttle_adaptation) :
    throttle_monit_dataframe_key = check_for_time_group(throttle_adaption_monitoring_fields, yop)
    if "group_group" not in throttle_monit_dataframe_key:
        data = throttle_adaptation_monitoring(yop, date_, throttle_monit_dataframe_key, throttle_adaption_monitoring_fields )
        date_wise_max_throttle_adaptation.append(data)
    return date_wise_max_throttle_adaptation

def throttle_adaptiation_clubbed_data(date_wise_max_throttle_adaptation) :
    dates = []
    AirFbk_AdpFlow00_BU_Max = []
    AirFbk_AdpFlow01_BU_Max = []
    AirFbk_AdpFlow02_BU_Max = []
    AirFbk_AdpFlow03_BU_Max = []
    AirFbk_AdpFlow04_BU_Max = []
    AirFbk_AdpFlow05_BU_Max = []
    AirFbk_AdpFlow06_BU_Max = []
    for data in date_wise_max_throttle_adaptation :
        for dt in data.keys() :
            dates.append(dt)
            AirFbk_AdpFlow00_BU_Max.append(data[dt]["AirFbk_AdpFlow00_BU"])
            AirFbk_AdpFlow01_BU_Max.append(data[dt]["AirFbk_AdpFlow01_BU"])
            AirFbk_AdpFlow02_BU_Max.append(data[dt]["AirFbk_AdpFlow02_BU"])
            AirFbk_AdpFlow03_BU_Max.append(data[dt]["AirFbk_AdpFlow03_BU"])
            AirFbk_AdpFlow04_BU_Max.append(data[dt]["AirFbk_AdpFlow04_BU"])
            AirFbk_AdpFlow05_BU_Max.append(data[dt]["AirFbk_AdpFlow05_BU"])
            AirFbk_AdpFlow06_BU_Max.append(data[dt]["AirFbk_AdpFlow06_BU"])
    df_throttle_adaptation = pd.DataFrame({"Throttle_Adap_Dates" : dates, "AirFbk_AdpFlow00_BU_Max" : AirFbk_AdpFlow00_BU_Max,
                             "AirFbk_AdpFlow01_BU_Max" : AirFbk_AdpFlow01_BU_Max,
                             "AirFbk_AdpFlow02_BU_Max" : AirFbk_AdpFlow02_BU_Max
                            ,"AirFbk_AdpFlow03_BU_Max" : AirFbk_AdpFlow03_BU_Max,
                             "AirFbk_AdpFlow04_BU_Max" : AirFbk_AdpFlow04_BU_Max,
                            "AirFbk_AdpFlow05_BU_Max": AirFbk_AdpFlow05_BU_Max,
                            "AirFbk_AdpFlow06_BU_Max": AirFbk_AdpFlow06_BU_Max})
    df_throttle_adaptation = df_throttle_adaptation.groupby("Throttle_Adap_Dates").max()
    df_throttle_adaptation.reset_index(inplace=True)
    return df_throttle_adaptation


# ### Fuel Adaptation

# In[17]:


def fuel_adaptation_monitoring(yop,date_, fuel_adaption_monitoring_fields, fuel_monit_dataframe_key) :
    df_fuel_adaptation_monitoring_all = yop[fuel_monit_dataframe_key][fuel_adaption_monitoring_fields]
    
    x_max_value = []
    date_mapping = []
    max_value = max(df_fuel_adaptation_monitoring_all["LmdInj_CmpRatio"].to_list(), key=abs)
        
        
    date_mapping.append(date_)
    x_max_value.append(max_value)
    return  x_max_value, date_mapping
def fetch_required_df_fuel_adaptation(Dates_mapping_fuel_adap,LmdInj_CmpRatio_max_value ) :
    df_fuel_adaptation = pd.DataFrame({"Fuel_Adap_Date": Dates_mapping_fuel_adap, "LmdInj_CmpRatio":LmdInj_CmpRatio_max_value })
    if len(df_fuel_adaptation) > 0 :
        data_grps = df_fuel_adaptation.groupby("Fuel_Adap_Date")
        final_dates = []
        final_max_value = []
        for grps in data_grps.groups :
            df_fuel_adaptation_monitoring_all = data_grps.get_group(grps)
            max_value = max(df_fuel_adaptation_monitoring_all["LmdInj_CmpRatio"].to_list(), key=abs)
            final_dates.append(grps)
            final_max_value.append(max_value)
        df_fuel_adaptation = pd.DataFrame({"Fuel_Adap_Date": final_dates, "LmdInj_CmpRatio":final_max_value })
    else :
        df_fuel_adaptation = pd.DataFrame({"Fuel_Adap_Date": [], "LmdInj_CmpRatio":[] })
    return df_fuel_adaptation
       
    

def fuel_adaptation_monitoring_data(yop, date_, fuel_adaption_monitoring_fields, LmdInj_CmpRatio_max_value,Dates_mapping) :
    fuel_monit_dataframe_key = check_for_time_group(fuel_adaption_monitoring_fields, yop)
#     print(fuel_monit_dataframe_key)
    if "group_group" not in fuel_monit_dataframe_key:
        x_max_value, date_mapping = fuel_adaptation_monitoring(yop,date_, fuel_adaption_monitoring_fields, fuel_monit_dataframe_key)
        LmdInj_CmpRatio_max_value.extend(x_max_value)
        Dates_mapping.extend(date_mapping)
    return LmdInj_CmpRatio_max_value,Dates_mapping


# ### Fuel Adaptation Zone Wise

# In[18]:


fuel_zone_wise_monitoring_fields1 = ["LmdInj_AdpRatio_CNG_[0]"]
fuel_zone_wise_monitoring_fields2 = ["LmdInj_AdpRatio_PET_[0]"]
fuel_zone_wise_monitoring_fields3 = ["LmdInj_AdpRatio00_BU"]
fuel_zone_wise_monitoring_fields4 = ["LmdInj_AdpRatio00_CNG_BU"]
fuel_zone_wise_monitoring_fields5 = ["LmdInj_AdpRatio01_BU"]
fuel_zone_wise_monitoring_fields6 = ["LmdInj_AdpRatio01_CNG_BU"]
fuel_zone_wise_monitoring_fields7 = ["LmdInj_AdpRatio02_BU"]
fuel_zone_wise_monitoring_fields8 = ["LmdInj_AdpRatio02_CNG_BU"]
fuel_zone_wise_monitoring_fields9 = ["LmdInj_AdpRatio10_BU"]
fuel_zone_wise_monitoring_fields10 = ["LmdInj_AdpRatio10_CNG_BU"]
fuel_zone_wise_monitoring_fields11 = ["LmdInj_AdpRatio11_BU"]
fuel_zone_wise_monitoring_fields12 = ["LmdInj_AdpRatio11_CNG_BU"]
fuel_zone_wise_monitoring_fields13 = ["LmdInj_AdpRatio12_BU"]

fuel_zone_wise_monitoring_fields14 = ["LmdInj_AdpRatio12_CNG_BU"]
fuel_zone_wise_monitoring_fields15 = ["LmdInj_AdpRatioIdl_BU"]
fuel_zone_wise_monitoring_fields16 = ["LmdInj_AdpRatioIdl_CNG_BU"]

all_fuel_zone_fields = [fuel_zone_wise_monitoring_fields1, fuel_zone_wise_monitoring_fields2, fuel_zone_wise_monitoring_fields3,
                       fuel_zone_wise_monitoring_fields4, fuel_zone_wise_monitoring_fields5, fuel_zone_wise_monitoring_fields6,
                       fuel_zone_wise_monitoring_fields7, fuel_zone_wise_monitoring_fields8, fuel_zone_wise_monitoring_fields9,
                       fuel_zone_wise_monitoring_fields10, fuel_zone_wise_monitoring_fields11, fuel_zone_wise_monitoring_fields12,
                       fuel_zone_wise_monitoring_fields13, fuel_zone_wise_monitoring_fields14, fuel_zone_wise_monitoring_fields15,
                       fuel_zone_wise_monitoring_fields16]




def fuel_adap_zone_wise_monitoring(yop, date_, all_fuel_zone_fields, fields_dataframes) :
    for fields in all_fuel_zone_fields :
        fuel_monit_dataframe_key = check_for_time_group(fields, yop)
#         print(fuel_monit_dataframe_key)
        if "group_group" not in fuel_monit_dataframe_key:
            df_fuel_adaptation_monitoring_all = yop[fuel_monit_dataframe_key][fields]
            df_fuel_adaptation_monitoring_all["just_date"] = date_
            if fields[0] in fields_dataframes :
                fields_dataframes[fields[0]].append(df_fuel_adaptation_monitoring_all)
            else :
                fields_dataframes[fields[0]] = [df_fuel_adaptation_monitoring_all]
    return fields_dataframes

def find_max_length_key_andadd_nan(fields_analysis) :
    max_length = 0
    for keys in fields_analysis :
        if len(fields_analysis[keys]) > max_length :
            max_length = len(fields_analysis[keys])
    for keys in fields_analysis :
        if len(fields_analysis[keys]) < max_length :
    
            fields_analysis[keys].extend([np.nan for i in range(0, (max_length - len(fields_analysis[keys]) ))])
        
    return fields_analysis

def merge_for_all_columns(df_clubbed) :
    df_clubbed_all = pd.DataFrame({ "LmdInj_AdpRatio_CNG_[0]_Date":[],"LmdInj_AdpRatio_CNG_[0]_Min" :[],"LmdInj_AdpRatio_CNG_[0]_Max" :[],"LmdInj_AdpRatio_PET_[0]_Date":[],"LmdInj_AdpRatio_PET_[0]_Min" :[],"LmdInj_AdpRatio_PET_[0]_Max" :[],"LmdInj_AdpRatio00_BU_Date":[],"LmdInj_AdpRatio00_BU_Min" :[],
                                   "LmdInj_AdpRatio00_BU_Max" :[],"LmdInj_AdpRatio00_CNG_BU_Date":[],"LmdInj_AdpRatio00_CNG_BU_Min" :[],"LmdInj_AdpRatio00_CNG_BU_Max" :[],"LmdInj_AdpRatio01_BU_Date":[],"LmdInj_AdpRatio01_BU_Min" :[],"LmdInj_AdpRatio01_BU_Max" :[],"LmdInj_AdpRatio01_CNG_BU_Date":[],"LmdInj_AdpRatio01_CNG_BU_Min" :[],"LmdInj_AdpRatio01_CNG_BU_Max" :[],"LmdInj_AdpRatio02_BU_Date":[],"LmdInj_AdpRatio02_BU_Min" :[],"LmdInj_AdpRatio02_BU_Max" :[],"LmdInj_AdpRatio02_CNG_BU_Date":[],
                                   "LmdInj_AdpRatio02_CNG_BU_Min" :[],"LmdInj_AdpRatio02_CNG_BU_Max" :[],"LmdInj_AdpRatio10_BU_Date":[],"LmdInj_AdpRatio10_BU_Min" :[],"LmdInj_AdpRatio10_BU_Max" :[],"LmdInj_AdpRatio10_CNG_BU_Min" :[],"LmdInj_AdpRatio10_CNG_BU_Date":[], "LmdInj_AdpRatio10_CNG_BU_Max" :[],"LmdInj_AdpRatio11_BU_Date":[],"LmdInj_AdpRatio11_BU_Min" :[],"LmdInj_AdpRatio11_BU_Max" :[],"LmdInj_AdpRatio11_CNG_BU_Date":[],
                                   "LmdInj_AdpRatio11_CNG_BU_Min" :[],"LmdInj_AdpRatio11_CNG_BU_Max" :[],"LmdInj_AdpRatio12_BU_Date":[],"LmdInj_AdpRatio12_BU_Min" :[],"LmdInj_AdpRatio12_BU_Max" :[],"LmdInj_AdpRatio12_CNG_BU_Date":[],
                                   "LmdInj_AdpRatio12_CNG_BU_Min" :[],"LmdInj_AdpRatio12_CNG_BU_Max" :[],"LmdInj_AdpRatioIdl_BU_Date":[],"LmdInj_AdpRatioIdl_BU_Min" :[],"LmdInj_AdpRatioIdl_BU_Max" :[], "LmdInj_AdpRatioIdl_CNG_BU_Date":[],"LmdInj_AdpRatioIdl_CNG_BU_Min" :[],"LmdInj_AdpRatioIdl_CNG_BU_Max" :[]})
    df_final_clubbed = pd.DataFrame()
    for cols in df_clubbed_all.columns :
        if cols in df_clubbed.columns.to_list() :
            df_final_clubbed[cols] = df_clubbed[cols]
        else :
            df_final_clubbed[cols] = [np.nan for i in range(0, len(df_clubbed))]
    return df_final_clubbed

def fuel_adap_zone_wise_monitoring_frames(fields_dataframes) :
    fields_analysis = {}      
    for fields in fields_dataframes :
        concatenated_dfs = fields_dataframes[fields][0]
        for other_dfs in range(1, len(fields_dataframes[fields])) :
            concatenated_dfs = concatenated_dfs.append(fields_dataframes[fields][other_dfs], ignore_index = True)
        date_wise_grp = concatenated_dfs.groupby("just_date")
        for grps in date_wise_grp.groups :
    #         print("****", date_wise_grp.get_group(grps))
            df_fuel_adaptation_monitoring = date_wise_grp.get_group(grps)
            max_value = max(df_fuel_adaptation_monitoring[fields].to_list(), key=abs)
            min_value = min(df_fuel_adaptation_monitoring[fields].to_list(), key=abs)
#             print(grps, min_value, max_value)
            if str(fields) + "_Min" in fields_analysis and str(fields) + "_Max" in fields_analysis and str(fields) + "_Date" in fields_analysis:
                fields_analysis[str(fields) + "_Date"].append(grps)
                fields_analysis[str(fields) + "_Min"].append(min_value)
                fields_analysis[str(fields) + "_Max"].append(max_value)
                
            else :
                fields_analysis[str(fields) + "_Date"] = [grps]
                fields_analysis[str(fields) + "_Min"] = [min_value]
                fields_analysis[str(fields) + "_Max"] = [max_value]
#     print(fields_analysis)
    
    fields_analysis = find_max_length_key_andadd_nan(fields_analysis)
    df_fuel_all_zones = pd.DataFrame(fields_analysis)
    df_fuel_all_zones = merge_for_all_columns(df_fuel_all_zones)
    df_fuel_all_zones.dropna(inplace=True)
    
    return df_fuel_all_zones


# ### Fuel Adaptation OnIdle with 3 sec delay

# In[19]:


def find_selected_cmpratio(cp_required_df) :
    timediff_max = max(cp_required_df["timediff"])
    timediff_min = min(cp_required_df["timediff"])
#     print(timediff_min, timediff_max)
    selected_LmdInj_CmpRatio = []
    
    for i in range(timediff_min, timediff_max, 3) :
        selected_i =  i + 3
        sel = cp_required_df.loc[cp_required_df["timediff"] >selected_i]
        if len(sel) > 0 :
            sel.reset_index(inplace=True)
            data = sel.iloc[0]
            if data["CmbReq_EngCtrStatus"] == 1 and data["TxcSts_VehSpeed"] == 0.0 :
                selected_LmdInj_CmpRatio.append(data["LmdInj_CmpRatio"])

    return selected_LmdInj_CmpRatio


def fuel_adaptation_monitoring_onidle_3sec(yop,date_, fuel_adaption_monitoring_onidle_fields, fuel_monit_onidle_dataframe_key) :
    df_fuel_adaptation_monitoring_all = yop[fuel_monit_onidle_dataframe_key][fuel_adaption_monitoring_onidle_fields]
    df_fuel_adaptation_monitoring_all.reset_index(inplace=True)
    df_fuel_adaptation_monitoring_all["timediff"] = ((df_fuel_adaptation_monitoring_all['index'] - df_fuel_adaptation_monitoring_all["index"][0]).dt.total_seconds())
    df_fuel_adaptation_monitoring_all['timediff'] = df_fuel_adaptation_monitoring_all['timediff'].astype(int)
    selected_LmdInj_CmpRatio = find_selected_cmpratio(df_fuel_adaptation_monitoring_all)
    
    x_max_value = []
    date_mapping = []
    if len(selected_LmdInj_CmpRatio) > 0 :
        max_value = max(selected_LmdInj_CmpRatio, key=abs)
        date_mapping.append(date_)
        x_max_value.append(max_value)
#     print("***************************", x_max_value)
    return  x_max_value, date_mapping

def fetch_required_df_fuel_adaptation_onidle(Dates_mapping_fuel_adap,LmdInj_CmpRatio_max_value ) :
    df_fuel_adaptation = pd.DataFrame({"Fuel_Adap_Date_onidle": Dates_mapping_fuel_adap, "LmdInj_CmpRatio_onidle":LmdInj_CmpRatio_max_value })
    if len(df_fuel_adaptation) > 0 :
        data_grps = df_fuel_adaptation.groupby("Fuel_Adap_Date_onidle")
        final_dates = []
        final_max_value = []
        for grps in data_grps.groups :
            df_fuel_adaptation_monitoring_all = data_grps.get_group(grps)
            max_value = max(df_fuel_adaptation_monitoring_all["LmdInj_CmpRatio_onidle"].to_list(), key=abs)
            final_dates.append(grps)
            final_max_value.append(max_value)
        df_fuel_adaptation = pd.DataFrame({"Fuel_Adap_Date_onidle": final_dates, "LmdInj_CmpRatio_onidle":final_max_value })
    else :
        df_fuel_adaptation = pd.DataFrame({"Fuel_Adap_Date_onidle": [], "LmdInj_CmpRatio_onidle":[] })
    return df_fuel_adaptation



def fuel_adaptation_monitoring_data_onidle_3sec(yop, date_, fuel_adaption_monitoring_onidle_fields, LmdInj_CmpRatio_max_value,Dates_mapping) :
    fuel_monit_onidle_dataframe_key = check_for_time_group(fuel_adaption_monitoring_onidle_fields, yop)
#     print(fuel_monit_onidle_dataframe_key)
    if "group_group" not in fuel_monit_onidle_dataframe_key:
        x_max_value, date_mapping = fuel_adaptation_monitoring_onidle_3sec(yop,date_, fuel_adaption_monitoring_onidle_fields, fuel_monit_onidle_dataframe_key)
        LmdInj_CmpRatio_max_value.extend(x_max_value)
        Dates_mapping.extend(date_mapping)
#         print("came here")
    return LmdInj_CmpRatio_max_value,Dates_mapping


# ### Fuel Adaptation 3 sec consecutive

# In[20]:


def update_dataframes_fuel_adaptation(yop,cp_required_key, required_fields_fuel_ad) :
    cp_required_df = {}
    for fields in required_fields_fuel_ad :
        cp_required_df[fields] = yop[cp_required_key[fields]]
        cp_required_df[fields] = cp_required_df[fields][[fields]]
        cp_required_df[fields].reset_index(inplace=True)
        try :
            cp_required_df[fields]["timediff"] = ((cp_required_df[fields]['index'] - cp_required_df[fields]["index"][0]).dt.total_seconds())
            cp_required_df[fields]['timediff'] = cp_required_df[fields]['timediff'].astype(int)
        except :
            cp_required_df[fields]['timediff'] = 86400
    return cp_required_df


def get_selected_LmdInj_CmpRatio_based_on_PdlAcp_Position(cp_required_df) :
    cp_required_df_1 = cp_required_df["PdlAcp_Position"]
    timediff_max = max(cp_required_df_1["timediff"])
    timediff_min = min(cp_required_df_1["timediff"])
    selected_PdlAcp_Position = []
    for i in range(timediff_min, timediff_max-3) :
        selected_i =  i + 3
        if len(list(set(cp_required_df_1.iloc[i : selected_i]["PdlAcp_Position"].to_list()))) == 1 :
            sel = cp_required_df_1.loc[cp_required_df_1["timediff"] >selected_i]
            if len(sel) > 0 :
                data = sel.iloc[0]
                if data["PdlAcp_Position"] != 0 :
                    selected_PdlAcp_Position.append(data)
    return selected_PdlAcp_Position

def get_all_data_merged(selected_PdlAcp_Position, date_, cp_required_df, fuel_adaptation_3sec_data) :
    df_selected_PdlAcp_Position = pd.DataFrame(selected_PdlAcp_Position)
    df_selected_PdlAcp_Position.reset_index(inplace=True)
    
    df_LmdInj_CmpRatio = cp_required_df["LmdInj_CmpRatio"]
    df_LmdInj_CmpRatio.reset_index(inplace=True)
    LmdInj_CmpRatio_list = []
    
    for row in df_selected_PdlAcp_Position.itertuples() :
        LmdInj_CmpRatio_list.append(df_LmdInj_CmpRatio.loc[df_LmdInj_CmpRatio["index"] > row.index]["LmdInj_CmpRatio"].to_list()[0])
    if len(LmdInj_CmpRatio_list) > 0 :
        max_LmdInj_CmpRatio = max(LmdInj_CmpRatio_list, key=abs)

        df_selected_LmdInj_CmpRatio_selected = df_LmdInj_CmpRatio.loc[df_LmdInj_CmpRatio["LmdInj_CmpRatio"] == max_LmdInj_CmpRatio]
#         print(df_selected_LmdInj_CmpRatio_selected["index"])
#         print(df_selected_PdlAcp_Position.loc[df_selected_PdlAcp_Position["index"] <= df_selected_LmdInj_CmpRatio_selected["index"].to_list()[0]])
        if len(df_selected_LmdInj_CmpRatio_selected) > 0 :
            if len(df_selected_PdlAcp_Position.loc[df_selected_PdlAcp_Position["index"] < df_selected_LmdInj_CmpRatio_selected["index"].to_list()[0]]["PdlAcp_Position"].to_list()) > 0 :
                PdlAcp_Position = df_selected_PdlAcp_Position.loc[df_selected_PdlAcp_Position["index"] < df_selected_LmdInj_CmpRatio_selected["index"].to_list()[0]]["PdlAcp_Position"].to_list()[-1]
            else :
                PdlAcp_Position = None
            requested_data_1 = cp_required_df["ItkAir_AirChgRatio"].loc[cp_required_df["ItkAir_AirChgRatio"]["index"] > df_selected_LmdInj_CmpRatio_selected["index"].to_list()[0]]
            if len(requested_data_1) > 0 :
                ItkAir_AirChgRatio = requested_data_1["ItkAir_AirChgRatio"].to_list()[0]
            else :
                ItkAir_AirChgRatio = None
            requested_data_2 = cp_required_df["ApfCrk_EngSpeed"].loc[cp_required_df["ApfCrk_EngSpeed"]["index"] > df_selected_LmdInj_CmpRatio_selected["index"].to_list()[0]]
            if len(requested_data_2) > 0 :
                ApfCrk_EngSpeed = requested_data_2["ApfCrk_EngSpeed"].to_list()[0]
            else :
                ApfCrk_EngSpeed = None
            fuel_adaptation_3sec_data.append({"Date_Fuel_AD_3_Sec" : date_, "PdlAcp_Position" : PdlAcp_Position,
                                             "LmdInj_CmpRatio_3_sec": df_selected_LmdInj_CmpRatio_selected["LmdInj_CmpRatio"].to_list()[0],
                                             "ItkAir_AirChgRatio": ItkAir_AirChgRatio, "ApfCrk_EngSpeed": ApfCrk_EngSpeed})
    return fuel_adaptation_3sec_data

def fuel_adaptation_monitoring_data_3sec_cons(yop, date_, fuel_adaption_monitoring_3_sec_cons_fields, fuel_adaptation_3sec_data) :
    
    cp_required_key = {}
    for fields in fuel_adaption_monitoring_3_sec_cons_fields :
        cp_required_key[fields] = check_for_time_group([fields], yop)
#         print(cp_required_key[fields])
        if "group_group" in cp_required_key[fields] :
            return fuel_adaptation_3sec_data
    
    
    cp_required_df = update_dataframes_fuel_adaptation(yop,cp_required_key, fuel_adaption_monitoring_3_sec_cons_fields)
    selected_PdlAcp_Position = get_selected_LmdInj_CmpRatio_based_on_PdlAcp_Position(cp_required_df)
    fuel_adaptation_3sec_data = get_all_data_merged(selected_PdlAcp_Position, date_, cp_required_df, fuel_adaptation_3sec_data)
#         print("came here")
    return fuel_adaptation_3sec_data

def fetch_required_fuel_adaptation_3_sec_data(fuel_adaptation_3_sec_cons_data) : 
    df_fuel_adaptation = pd.DataFrame(fuel_adaptation_3_sec_cons_data)
    if len(df_fuel_adaptation) > 0 :
        data_grps = df_fuel_adaptation.groupby("Date_Fuel_AD_3_Sec")
        final_dates = []
        final_LmdInj_CmpRatio = []
        final_PdlAcp_Position = []
        final_ItkAir_AirChgRatio = []
        final_ApfCrk_EngSpeed = []
        for grps in data_grps.groups :
            df_fuel_adaptation_monitoring_all = data_grps.get_group(grps)
            max_value = max(df_fuel_adaptation_monitoring_all["LmdInj_CmpRatio_3_sec"].to_list(), key=abs)
            df_selected_LmdInj_CmpRatio_selected = df_fuel_adaptation_monitoring_all.loc[df_fuel_adaptation_monitoring_all["LmdInj_CmpRatio_3_sec"] == max_value]

            final_dates.append(grps)
            final_LmdInj_CmpRatio.append(max_value)
            final_PdlAcp_Position.append(df_selected_LmdInj_CmpRatio_selected["PdlAcp_Position"].to_list()[0])
            final_ItkAir_AirChgRatio.append(df_selected_LmdInj_CmpRatio_selected["ItkAir_AirChgRatio"].to_list()[0])
            final_ApfCrk_EngSpeed.append(df_selected_LmdInj_CmpRatio_selected["ApfCrk_EngSpeed"].to_list()[0])

        df_fuel_adaptation = pd.DataFrame({"Date_Fuel_AD_3_Sec": final_dates, "LmdInj_CmpRatio_3_sec":final_LmdInj_CmpRatio,
                                          "PdlAcp_Position": final_PdlAcp_Position, "ItkAir_AirChgRatio" : final_ItkAir_AirChgRatio,
                                          "ApfCrk_EngSpeed": final_ApfCrk_EngSpeed})
    else :
        df_fuel_adaptation = pd.DataFrame({"Date_Fuel_AD_3_Sec": [], "LmdInj_CmpRatio_3_sec":[],
                                          "PdlAcp_Position": [], "ItkAir_AirChgRatio" : [],
                                          "ApfCrk_EngSpeed": []})
    return df_fuel_adaptation


# ### Temperature Monitoring

# In[21]:


def get_per_day_temp_monit(yop, date_, temp_monit_dataframe_key, required_field1) :
    df_temp_monit = yop[temp_monit_dataframe_key][required_field1]
    if len(df_temp_monit) > 0 :
        max_value = max(df_temp_monit[required_field1].max())
    else :
        max_value = np.nan
    #     print(max_value_)
    
    return max_value


def temperature_monitoring_data(yop, date_, temp_required_field1, temp_required_field2, temp_required_field3,temp_required_field4, date_wise_max_temp_monit) :
    temp_monit_key1 = check_for_time_group(temp_required_field1, yop)
    temp_monit_key2 = check_for_time_group(temp_required_field2, yop)
    temp_monit_key3 = check_for_time_group(temp_required_field3, yop)
    temp_monit_key4 = check_for_time_group(temp_required_field4, yop)
    date_wise_max_dict = {}
    
    if "group_group" in temp_monit_key1 or "group_group" in temp_monit_key2 or "group_group" in temp_monit_key3 or "group_group" in temp_monit_key4:
        date_wise_max_dict[str(date_)] = {}
        if "group_group" not in temp_monit_key1 :
            date_wise_max_dict[str(date_)][temp_required_field1[0]] = get_per_day_temp_monit(yop, date_, temp_monit_key1, temp_required_field1)
        else :
            date_wise_max_dict[str(date_)][temp_required_field1[0]] = None

        if "group_group" not in temp_monit_key2 :
            date_wise_max_dict[str(date_)][temp_required_field2[0]] = get_per_day_temp_monit(yop, date_, temp_monit_key2, temp_required_field2) 
        else :
            date_wise_max_dict[str(date_)][temp_required_field2[0]] =  None

        if "group_group" not in temp_monit_key3 :
            date_wise_max_dict[str(date_)][temp_required_field3[0]] = get_per_day_temp_monit(yop, date_, temp_monit_key3, temp_required_field3) 
        else :
            date_wise_max_dict[str(date_)][temp_required_field3[0]] = None
            
        if "group_group" not in temp_monit_key4 :
            date_wise_max_dict[str(date_)][temp_required_field4[0]] = get_per_day_temp_monit(yop, date_, temp_monit_key4, temp_required_field4) 
        else :
            date_wise_max_dict[str(date_)][temp_required_field4[0]] = None
    if len(date_wise_max_dict) > 0 :
        date_wise_max_temp_monit.append(date_wise_max_dict)
    return date_wise_max_temp_monit 

def temperature_monitoring_dataframe(date_wise_max_temp_monit) :
    dates = []
    ExhTmp_CatTemperature = []
    ApfOil_EngOilTemperature = []
    EngClt_Temperature = []
    TxcSts_VehSpeed = []
    for data in date_wise_max_temp_monit :
        for dt in data.keys() :
            dates.append(dt)
            ExhTmp_CatTemperature.append(data[dt]["ExhTmp_CatTemperature"])
            ApfOil_EngOilTemperature.append(data[dt]["ApfOil_EngOilTemperature"])
            EngClt_Temperature.append(data[dt]["EngClt_Temperature"])
            TxcSts_VehSpeed.append(data[dt]["TxcSts_VehSpeed"])
    df_temp_monit = pd.DataFrame({"TM_date" : dates, "ExhTmp_CatTemperature" : ExhTmp_CatTemperature, "ApfOil_EngOilTemperature" : ApfOil_EngOilTemperature,
                                 "EngClt_Temperature" : EngClt_Temperature, "TxcSts_VehSpeed": TxcSts_VehSpeed})
    df_temp_monit = df_temp_monit.groupby("TM_date").max()
    df_temp_monit.reset_index(inplace=True)
    
    return df_temp_monit



# ### Torque Adaptation

# In[22]:


def get_index_mapping_torque_adaptation(SymEcu_Status_list, index_time_list) :
    zero_to_one_check = False
    index_mapping = []
    for i in range(0, len(SymEcu_Status_list)) :
        if SymEcu_Status_list[i] == 1 and zero_to_one_check:
            zero_to_one_check = False
        elif SymEcu_Status_list[i] == 0 and not zero_to_one_check:
            zero_to_one_check = True
            index_mapping.append({"index" : index_time_list[i]})
    return index_mapping

def torque_adaptation(yop, date_,torque_adap_dataframe_key, torque_adap_required_fields_1) :
    time_key = torque_adap_dataframe_key.split("_group")[0]
    torque_adap_required_fields_1_1 = torque_adap_required_fields_1[:]
    torque_adap_required_fields_1_1.append(time_key)
    df_torque_adap_all = yop[torque_adap_dataframe_key][torque_adap_required_fields_1_1]
    
    df_torque_adap_all.reset_index(inplace=True)
    df_torque_adap_all['just_date'] = df_torque_adap_all['index'].dt.date
    df_torque_adap_all.set_index("index", inplace=True)
    date_wise_grp = df_torque_adap_all.groupby("just_date")
    x = []
    date_mapping = []
    df_torque_adap = df_torque_adap_all
    SymEcu_Status_list = df_torque_adap["SymEcu_Status"].to_list()

    index_time_list = []
        
    for data in df_torque_adap[time_key] :
        index_time_list.append(data) 
    index_mapping = get_index_mapping_torque_adaptation(SymEcu_Status_list, index_time_list)
    for mapping in index_mapping :
        x.append(mapping["index"])
        date_mapping.append(date_)
                
    return x, date_mapping

def torque_adap_mapping_data(yop,date_, x,dates, torque_adap_dataframe_key_2, torque_adap_required_fields_2) :
    y_opt = []
    time_key = torque_adap_dataframe_key_2.split("_group")[0]
    torque_adap_required_fields_2_1 = torque_adap_required_fields_2[:]
    torque_adap_required_fields_2_1.append(time_key)
    
    df_torque_adap_all = yop[torque_adap_dataframe_key_2][torque_adap_required_fields_2_1]
    
    df_torque_adap_all.reset_index(inplace=True)
    df_torque_adap_all['just_date'] = df_torque_adap_all['index'].dt.date

    df_torque_adap_all.set_index("index", inplace=True)
    
    for ind in range(0,len(x) ) :
        y_opt.append(df_torque_adap_all.loc[(df_torque_adap_all[time_key] >= x[ind])]["TxcLos_LosTorque"].to_list()[0])
    return y_opt

def torque_adaptation_data(yop, date_, torque_adap_required_fields_1, torque_adap_required_fields_2, index_mapping_x, TxcLos_LosTorque_y, Dates_mapping):
    torque_adap_dataframe_key_1 = check_for_time_group(torque_adap_required_fields_1, yop)
    torque_adap_dataframe_key_2 = check_for_time_group(torque_adap_required_fields_2, yop)
    
    if "group_group" not in torque_adap_dataframe_key_1 and "group_group" not in torque_adap_dataframe_key_2:
        x, dates = torque_adaptation(yop, date_, torque_adap_dataframe_key_1,torque_adap_required_fields_1 )
        y = torque_adap_mapping_data(yop,date_, x,dates, torque_adap_dataframe_key_2, torque_adap_required_fields_2)
        index_mapping_x.extend(x)
        TxcLos_LosTorque_y.extend(y)
        Dates_mapping.extend(dates)
    return index_mapping_x, TxcLos_LosTorque_y, Dates_mapping



def torque_adaptation_frames( Dates_mapping, TxcLos_LosTorque_y) :
    df_torque_adaptation = pd.DataFrame({"Date_Torque_Adap" : Dates_mapping, "TxcLos_LosTorque" : TxcLos_LosTorque_y})
    df_torque_adaptation = df_torque_adaptation.groupby("Date_Torque_Adap").max()
    df_torque_adaptation.reset_index(inplace=True)
    return df_torque_adaptation


# ### Load Monitoring
# 

# In[23]:


def get_load_monitoring(yop, date_, load_moni_dataframe_key_1, load_moni_dataframe_key_2, load_monit_required_fields1,load_monit_required_fields2 ) :
    df_load_monit = yop[load_moni_dataframe_key_1][load_monit_required_fields1]
    date_wise_max_load_monit = {}
    data = df_load_monit[load_monit_required_fields1].max()
    
    RteCom_EngineCalculatedLoadDGS_max = data["RteCom_EngineCalculatedLoadDGS"]
    TrqReq_FltRawTorque_max = data["TrqReq_FltRawTorque"]
    DgsClv_load_max = data["DgsClv_load"]
    max_index = df_load_monit[load_monit_required_fields1].idxmax()["RteCom_EngineCalculatedLoadDGS"]
    df_load_monit_2 = yop[load_moni_dataframe_key_2][load_monit_required_fields2]
#     print(max_index)
    ApfCrk_EngSpeed_load_monitoring = df_load_monit_2.loc[df_load_monit_2.index >= max_index]["ApfCrk_EngSpeed"].to_list()[0]
    
    date_wise_max_load_monit[str(date_)] = {"RteCom_EngineCalculatedLoadDGS" : RteCom_EngineCalculatedLoadDGS_max, "TrqReq_FltRawTorque" : TrqReq_FltRawTorque_max,
                                              "DgsClv_load" : DgsClv_load_max, "ApfCrk_EngSpeed_LM" : ApfCrk_EngSpeed_load_monitoring}
    return date_wise_max_load_monit

def load_monitoring_data(yop , date_, load_monit_requirement_fields_1, load_monit_requirement_fields_2, date_wise_max_load_monit) :
    load_moni_dataframe_key_1 = check_for_time_group(load_monit_requirement_fields_1, yop)
    load_moni_dataframe_key_2 = check_for_time_group(load_monit_requirement_fields_2, yop)
    if "group_group" not in load_moni_dataframe_key_1 and "group_group" not in load_moni_dataframe_key_2  :
        date_wise_max_load_monit.append(get_load_monitoring(yop, date_, load_moni_dataframe_key_1, load_moni_dataframe_key_2, load_monit_requirement_fields_1, load_monit_requirement_fields_2 ))
    return date_wise_max_load_monit

def load_monitoring_dataframe(date_wise_max_load_monit) :
    dates = []
    RteCom_EngineCalculatedLoadDGS = []
    TrqReq_FltRawTorque = []
    DgsClv_load = []
    ApfCrk_EngSpeed_LM = []
    
    for data in date_wise_max_load_monit :
        for dt in data.keys() :
            dates.append(dt)
            RteCom_EngineCalculatedLoadDGS.append(data[dt]["RteCom_EngineCalculatedLoadDGS"])
            TrqReq_FltRawTorque.append(data[dt]["TrqReq_FltRawTorque"])
            DgsClv_load.append(data[dt]["DgsClv_load"])
            ApfCrk_EngSpeed_LM.append(data[dt]["ApfCrk_EngSpeed_LM"])
            
    df_load_monit = pd.DataFrame({"LM_Date" : dates, "RteCom_EngineCalculatedLoadDGS" : RteCom_EngineCalculatedLoadDGS, "TrqReq_FltRawTorque" : TrqReq_FltRawTorque
                                    , "DgsClv_load" : DgsClv_load, "ApfCrk_EngSpeed_LM" : ApfCrk_EngSpeed_LM})
    df_load_monit = df_load_monit.groupby("LM_Date").max()
    df_load_monit.reset_index(inplace=True)
    return df_load_monit


# ### Dashboard style data formation

# In[24]:


def update_dates_and_alarming_values(all_dfs, alarming_values) :

    #### Append alarming rate 
    all_dfs["Catalyst Monitoring IUPR"]["Max_Alarming_Value_IUPR_CM"] = alarming_values.T["Catalyst Monitoring IUPR"]["Alarming_Value"]
    all_dfs["Catalyst Monitoring IUPR"]["Min_Alarming_Value_IUPR_CM"] = alarming_values.T["Catalyst Monitoring IUPR"]["Alarming_Value"]
    
    all_dfs["Crcme Monitoring IUPR"]["Max_Alarming_Value_IUPR_CrCme"] = alarming_values.T["CrCme Monitoring IUPR"]["Alarming_Value"]
    all_dfs["Crcme Monitoring IUPR"]["Min_Alarming_Value_IUPR_CrCme"] = alarming_values.T["CrCme Monitoring IUPR"]["Alarming_Value"]
    
    all_dfs["CrCmi Monitoring IUPR"]["Max_Alarming_Value_IUPR_CrCmi"] = alarming_values.T["CrCmi Monitoring IUPR"]["Alarming_Value"]
    all_dfs["CrCmi Monitoring IUPR"]["Min_Alarming_Value_IUPR_CrCmi"] = alarming_values.T["CrCmi Monitoring IUPR"]["Alarming_Value"]
    
    all_dfs["EGR Monitoring IUPR"]["Max_Alarming_Value_IUPR_EGR"] = alarming_values.T["EGR Monitoring IUPR"]["Alarming_Value"]
    all_dfs["EGR Monitoring IUPR"]["Min_Alarming_Value_IUPR_EGR"] = alarming_values.T["EGR Monitoring IUPR"]["Alarming_Value"]
    
    all_dfs["FO2 Monitoring IUPR"]["Max_Alarming_Value_IUPR_FO2"] = alarming_values.T["FO2 Monitoring IUPR"]["Alarming_Value"]
    all_dfs["FO2 Monitoring IUPR"]["Min_Alarming_Value_IUPR_FO2"] = alarming_values.T["FO2 Monitoring IUPR"]["Alarming_Value"]
    
    
    all_dfs["General IUPR"]["Max_Alarming_Value_IUPR_GEN"] = alarming_values.T["General IUPR"]["Alarming_Value"]
    all_dfs["General IUPR"]["Min_Alarming_Value_IUPR_GEN"] = alarming_values.T["General IUPR"]["Alarming_Value"]
    
    all_dfs["RO2 Monitoring IUPR"]["Max_Alarming_Value_IUPR_RO2"] = alarming_values.T["RO2 Monitoring IUPR"]["Alarming_Value"]
    all_dfs["RO2 Monitoring IUPR"]["Min_Alarming_Value_IUPR_RO2"] = alarming_values.T["RO2 Monitoring IUPR"]["Alarming_Value"]
    
    all_dfs["VVTe Monitoring IUPR"]["Max_Alarming_Value_IUPR_VVTe"] = alarming_values.T["VVTe Monitoring IUPR"]["Alarming_Value"]
    all_dfs["VVTe Monitoring IUPR"]["Min_Alarming_Value_IUPR_VVTe"] = alarming_values.T["VVTe Monitoring IUPR"]["Alarming_Value"]
    
    all_dfs["VVTi Monitoring IUPR"]["Max_Alarming_Value_IUPR_VVTi"] = alarming_values.T["VVTi Monitoring IUPR"]["Alarming_Value"]
    all_dfs["VVTi Monitoring IUPR"]["Min_Alarming_Value_IUPR_VVTi"] = alarming_values.T["VVTi Monitoring IUPR"]["Alarming_Value"]
    

    all_dfs["Catalyst Monitoring"]["Max_Alarming_Value_CM"] = alarming_values.T["Catalyst Monitoring"]["Alarming_Value"]
    all_dfs["Catalyst Monitoring"]["Min_Alarming_Value_CM"] = alarming_values.T["Catalyst Monitoring"]["Alarming_Value"]
    
    all_dfs["Catalyst Temperature"]["Min_Alarming_Value_CT"] = alarming_values.T["Catalyst Temperature"]["Alarming_Value"]
    all_dfs["Catalyst Temperature"]["Max_Alarming_Value_CT"] = alarming_values.T["Catalyst Temperature"]["Alarming_Value"]
    
    all_dfs["Misfire Monitoring"]["Min_Alarming_Value_MFM"] = alarming_values.T["Misfire Monitoring"]["Alarming_Value"]
    all_dfs["Misfire Monitoring"]["Max_Alarming_Value_MFM"] = alarming_values.T["Misfire Monitoring"]["Alarming_Value"]
    
    
    all_dfs["FO2 Monitoring"]["Max_Alarming_Value_FO2"] = alarming_values.T["FO2 Monitoring"]["Alarming_Value"]
    all_dfs["FO2 Monitoring"]["Min_Alarming_Value_FO2"] = alarming_values.T["FO2 Monitoring"]["Alarming_Value"]
    
    all_dfs["RO2 Monitoring"]["Min_Alarming_Value_RO2"] = alarming_values.T["RO2 Monitoring"]["Alarming_Value"]
    all_dfs["RO2 Monitoring"]["Max_Alarming_Value_RO2"] = alarming_values.T["RO2 Monitoring"]["Alarming_Value"]
    
    all_dfs["VVTe Monitoring"]["Max_Alarming_Value_VVTe"] = alarming_values.T["VVTe Monitoring"]["Alarming_Value"]
    all_dfs["VVTe Monitoring"]["Max_Alarming_Value_VVTe"] = alarming_values.T["VVTe Monitoring"]["Alarming_Value"]
    
    all_dfs["VVTi Monitoring"]["Max_Alarming_Value_VVTi"] = alarming_values.T["VVTi Monitoring"]["Alarming_Value"]
    all_dfs["VVTi Monitoring"]["Min_Alarming_Value_VVTi"] = alarming_values.T["VVTi Monitoring"]["Alarming_Value"]
    

    all_dfs["Fuel System"]["Min_Alarming_Value_FS"] = alarming_values.T["Fuel System"]["Alarming_Value"]
    all_dfs["Fuel System"]["Max_Alarming_Value_FS"] = alarming_values.T["Fuel System"]["Alarming_Value"]
    
    all_dfs["CrCmi Monitoring"]["Min_Alarming_Value_CrCmi"] = -(alarming_values.T["CrCmi Monitoring"]["Alarming_Value"])
    all_dfs["CrCmi Monitoring"]["Max_Alarming_Value_CrCmi"] = alarming_values.T["CrCmi Monitoring"]["Alarming_Value"]
    
    all_dfs["CrCme Monitoring"]["Min_Alarming_Value_CrCme"] = -(alarming_values.T["CrCme Monitoring"]["Alarming_Value"])
    all_dfs["CrCme Monitoring"]["Max_Alarming_Value_CrCme"] = alarming_values.T["CrCme Monitoring"]["Alarming_Value"]
    
    all_dfs["EGR Monitoring"]["Min_Alarming_Value_EGR"] = alarming_values.T["EGR Monitoring"]["Alarming_Value"]
    all_dfs["EGR Monitoring"]["Max_Alarming_Value_EGR"] = alarming_values.T["EGR Monitoring"]["Alarming_Value"]
    
    all_dfs["Throttle Adaptation"]["Min_Alarming_Value_th_adap"] = alarming_values.T["Throttle Adaptation"]["Alarming_Value"]
    all_dfs["Throttle Adaptation"]["Max_Alarming_Value_th_adap"] = alarming_values.T["Throttle Adaptation"]["Alarming_Value"]
    
    all_dfs["Fuel Adaptation"]["Min_Alarming_Value_fuel_adap"] = alarming_values.T["Fuel Adaptation"]["Alarming_Value"]
    all_dfs["Fuel Adaptation"]["Max_Alarming_Value_fuel_adap"] = alarming_values.T["Fuel Adaptation"]["Alarming_Value"]
    
#     all_dfs["Fuel Adaptation OnIdle"]["Min_Alarming_Value_fuel_adap"] = alarming_values.T["Fuel Adaptation"]["Alarming_Value"]
#     all_dfs["Fuel Adaptation OnIdle"]["Max_Alarming_Value_fuel_adap"] = alarming_values.T["Fuel Adaptation"]["Alarming_Value"]
    
    
#     all_dfs["Fuel Adaptation 3 sec cons"]["Min_Alarming_Value_fuel_adap"] = alarming_values.T["Fuel Adaptation"]["Alarming_Value"]
#     all_dfs["Fuel Adaptation 3 sec cons"]["Max_Alarming_Value_fuel_adap"] = alarming_values.T["Fuel Adaptation"]["Alarming_Value"]
    
    
    all_dfs["Fuel Adaptation Zone Wise"]["Min_Alarming_Value_fuel_adap_zw"] = alarming_values.T["Fuel Adaptation Zone Wise"]["Alarming_Value"]
    all_dfs["Fuel Adaptation Zone Wise"]["Max_Alarming_Value_fuel_adap_zw"] = alarming_values.T["Fuel Adaptation Zone Wise"]["Alarming_Value"]
    
    all_dfs["Temperature Monitoring"]["Alarming_Value_temp_monit"] = alarming_values.T["Temperature Monitoring"]["Alarming_Value"]
    all_dfs["Torque Adaptation"]["Alarming_Value_torque_adap"] = alarming_values.T["Torque Adaptation"]["Alarming_Value"]
    all_dfs["Load Monitoring"]["Alarming_Value_load_monitoring"] = alarming_values.T["Load Monitoring"]["Alarming_Value"]
    
    return all_dfs

def get_required_fields(all_dfs) :
    required_fields = {}
    for dfs in all_dfs.keys() :
        required_fields[dfs] = all_dfs[dfs].columns.to_list()
    return required_fields


def check_for_empty_frames(dict_data) :
    check = False
    for keys in dict_data :
        if len(dict_data[keys]) != 0 :
            check = True
            break
    return check


# ### Clubbed data 
# 

# In[25]:


def sort_all_value(all_dfs) :
    df_cata_monitoring_file = all_dfs["Catalyst Monitoring"]
    df_cata_temp_file = all_dfs["Catalyst Temperature"]
    df_misfire_monit_file = all_dfs["Misfire Monitoring"]
    df_monit_iupr_cata_file = all_dfs["Catalyst Monitoring IUPR"]
    df_monit_iupr_crcme_file = all_dfs["Crcme Monitoring IUPR"]
    df_monit_iupr_crcmi_file = all_dfs["CrCmi Monitoring IUPR"]
    df_monit_iupr_egr_file = all_dfs["EGR Monitoring IUPR"]
    df_monit_iupr_fo2_file = all_dfs["FO2 Monitoring IUPR"]
    df_monit_iupr_general_file = all_dfs["General IUPR"]
    df_monit_iupr_ro2_file = all_dfs["RO2 Monitoring IUPR"]
    df_monit_iupr_vvte_file = all_dfs["VVTe Monitoring IUPR"]
    df_monit_iupr_vvti_file = all_dfs["VVTi Monitoring IUPR"]
    df_monit_fo2_file = all_dfs["FO2 Monitoring"]
    df_monit_ro2_file = all_dfs["RO2 Monitoring"]
    df_monit_fuel_system_file = all_dfs["Fuel System"]
    df_vvte_monitoring_file = all_dfs["VVTe Monitoring"]
    df_vvti_monitoring_file = all_dfs["VVTi Monitoring"]
    df_crcmi_monitoring_file = all_dfs["CrCmi Monitoring"]
    df_crcme_monitoring_file = all_dfs["CrCme Monitoring"]
    df_egr_monitoring_file = all_dfs["EGR Monitoring"]
    df_throttle_adap_file = all_dfs["Throttle Adaptation"]
    df_fuel_adap_file = all_dfs["Fuel Adaptation"]
    df_fuel_adap_onidle_file = all_dfs["Fuel Adaptation OnIdle"]
    df_fuel_adap_3sec_cos_file = all_dfs["Fuel Adaptation 3 sec cons"]
    df_fuel_adap_zone_wise_file = all_dfs["Fuel Adaptation Zone Wise"]
    df_temp_monit_file = all_dfs["Temperature Monitoring"]
    df_torque_adap_file = all_dfs["Torque Adaptation"]
    df_load_monit_file = all_dfs["Load Monitoring"]
    
    df_cata_monitoring_file['CM_date'] = pd.to_datetime(df_cata_monitoring_file['CM_date'], format="%d-%m-%Y")
    df_monit_iupr_cata_file['IUPR_CM_Date'] = pd.to_datetime(df_monit_iupr_cata_file['IUPR_CM_Date'], format="%d-%m-%Y")
    df_monit_iupr_crcme_file['IUPR_CrCme_Date'] = pd.to_datetime(df_monit_iupr_crcme_file['IUPR_CrCme_Date'], format="%d-%m-%Y")
    df_monit_iupr_crcmi_file['IUPR_CrCmi_Date'] = pd.to_datetime(df_monit_iupr_crcmi_file['IUPR_CrCmi_Date'], format="%d-%m-%Y")
    df_monit_iupr_egr_file['IUPR_EGR_Date'] = pd.to_datetime(df_monit_iupr_egr_file['IUPR_EGR_Date'], format="%d-%m-%Y")
    df_monit_iupr_fo2_file['IUPR_FO2_Date'] = pd.to_datetime(df_monit_iupr_fo2_file['IUPR_FO2_Date'], format="%d-%m-%Y")
    df_monit_iupr_general_file['IUPR_GEN_Date'] = pd.to_datetime(df_monit_iupr_general_file['IUPR_GEN_Date'], format="%d-%m-%Y")
    df_monit_iupr_ro2_file['IUPR_RO2_Date'] = pd.to_datetime(df_monit_iupr_ro2_file['IUPR_RO2_Date'], format="%d-%m-%Y")
    df_monit_iupr_vvte_file['IUPR_VVTe_Date'] = pd.to_datetime(df_monit_iupr_vvte_file['IUPR_VVTe_Date'], format="%d-%m-%Y")
    df_monit_iupr_vvti_file['IUPR_VVTi_Date'] = pd.to_datetime(df_monit_iupr_vvti_file['IUPR_VVTi_Date'], format="%d-%m-%Y")
    df_cata_temp_file['CT_date'] = pd.to_datetime(df_cata_temp_file['CT_date'], format="%d-%m-%Y")
    df_misfire_monit_file['MFM_Date'] = pd.to_datetime(df_misfire_monit_file['MFM_Date'], format="%d-%m-%Y")
    df_monit_fo2_file['FO2_Date'] = pd.to_datetime(df_monit_fo2_file['FO2_Date'], format="%d-%m-%Y")
    df_monit_ro2_file['RO2_Date'] = pd.to_datetime(df_monit_ro2_file['RO2_Date'], format="%d-%m-%Y")
    df_monit_fuel_system_file['FS_date'] = pd.to_datetime(df_monit_fuel_system_file['FS_date'], format="%d-%m-%Y")
    df_vvte_monitoring_file['VVTe_Date'] = pd.to_datetime(df_vvte_monitoring_file['VVTe_Date'], format="%d-%m-%Y")
    df_vvti_monitoring_file['VVTi_Date'] = pd.to_datetime(df_vvti_monitoring_file['VVTi_Date'], format="%d-%m-%Y")
    df_crcmi_monitoring_file['CrCmi_Date'] = pd.to_datetime(df_crcmi_monitoring_file['CrCmi_Date'], format="%d-%m-%Y")
    df_crcme_monitoring_file['CrCme_Date'] = pd.to_datetime(df_crcme_monitoring_file['CrCme_Date'], format="%d-%m-%Y")
    df_egr_monitoring_file['EGR_Dates'] = pd.to_datetime(df_egr_monitoring_file['EGR_Dates'], format="%d-%m-%Y")
    df_throttle_adap_file['Throttle_Adap_Dates'] = pd.to_datetime(df_throttle_adap_file['Throttle_Adap_Dates'], format="%d-%m-%Y")
    df_fuel_adap_file['Fuel_Adap_Date'] = pd.to_datetime(df_fuel_adap_file['Fuel_Adap_Date'], format="%d-%m-%Y")
    df_fuel_adap_onidle_file['Fuel_Adap_Date_onidle'] = pd.to_datetime(df_fuel_adap_onidle_file['Fuel_Adap_Date_onidle'], format="%d-%m-%Y")
    df_fuel_adap_3sec_cos_file['Date_Fuel_AD_3_Sec'] = pd.to_datetime(df_fuel_adap_3sec_cos_file['Date_Fuel_AD_3_Sec'], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio_CNG_[0]_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio_CNG_[0]_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio_PET_[0]_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio_PET_[0]_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio00_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio00_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio00_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio00_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio01_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio01_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio01_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio01_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio02_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio02_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio02_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio02_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio10_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio10_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio10_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio10_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio11_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio11_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio11_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio11_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio12_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio12_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatioIdl_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatioIdl_BU_Date"], format="%d-%m-%Y")
    
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio12_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio12_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatioIdl_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatioIdl_CNG_BU_Date"], format="%d-%m-%Y")
    
    
    df_temp_monit_file['TM_date'] = pd.to_datetime(df_temp_monit_file['TM_date'], format="%d-%m-%Y")
    df_torque_adap_file['Date_Torque_Adap'] = pd.to_datetime(df_torque_adap_file['Date_Torque_Adap'], format="%d-%m-%Y")
    df_load_monit_file['LM_Date'] = pd.to_datetime(df_load_monit_file['LM_Date'], format="%d-%m-%Y")

    
    
    df_cata_monitoring_file = df_cata_monitoring_file.sort_values("CM_date")
    df_monit_iupr_cata_file = df_monit_iupr_cata_file.sort_values("IUPR_CM_Date")
    df_monit_iupr_crcme_file = df_monit_iupr_crcme_file.sort_values("IUPR_CrCme_Date")
    df_monit_iupr_crcmi_file = df_monit_iupr_crcmi_file.sort_values("IUPR_CrCmi_Date")
    df_monit_iupr_egr_file = df_monit_iupr_egr_file.sort_values("IUPR_EGR_Date")
    df_monit_iupr_fo2_file = df_monit_iupr_fo2_file.sort_values("IUPR_FO2_Date")
    df_monit_iupr_general_file = df_monit_iupr_general_file.sort_values("IUPR_GEN_Date")
    df_monit_iupr_ro2_file = df_monit_iupr_ro2_file.sort_values("IUPR_RO2_Date")
    df_monit_iupr_vvte_file = df_monit_iupr_vvte_file.sort_values("IUPR_VVTe_Date")
    df_monit_iupr_vvti_file = df_monit_iupr_vvti_file.sort_values("IUPR_VVTi_Date")
    df_cata_temp_file = df_cata_temp_file.sort_values("CT_date")
    df_misfire_monit_file = df_misfire_monit_file.sort_values("MFM_Date")
    df_monit_fo2_file = df_monit_fo2_file.sort_values("FO2_Date")
    df_monit_ro2_file =df_monit_ro2_file.sort_values("RO2_Date")
    df_monit_fuel_system_file = df_monit_fuel_system_file.sort_values("FS_date")
    df_vvte_monitoring_file = df_vvte_monitoring_file.sort_values("VVTe_Date")
    df_vvti_monitoring_file = df_vvti_monitoring_file.sort_values("VVTi_Date")
    df_crcmi_monitoring_file = df_crcmi_monitoring_file.sort_values("CrCmi_Date")
    df_crcme_monitoring_file = df_crcme_monitoring_file.sort_values("CrCme_Date")
    df_egr_monitoring_file = df_egr_monitoring_file.sort_values("EGR_Dates")
    df_throttle_adap_file = df_throttle_adap_file.sort_values("Throttle_Adap_Dates")
    df_fuel_adap_file = df_fuel_adap_file.sort_values("Fuel_Adap_Date")
    df_fuel_adap_onidle_file = df_fuel_adap_onidle_file.sort_values("Fuel_Adap_Date_onidle")
    df_fuel_adap_3sec_cos_file = df_fuel_adap_3sec_cos_file.sort_values("Date_Fuel_AD_3_Sec")
    df_fuel_adap_zone_wise_file = df_fuel_adap_zone_wise_file.sort_values(["LmdInj_AdpRatio_CNG_[0]_Date","LmdInj_AdpRatio_PET_[0]_Date"
                                                                           ,"LmdInj_AdpRatio00_BU_Date","LmdInj_AdpRatio00_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio01_BU_Date","LmdInj_AdpRatio01_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio02_BU_Date","LmdInj_AdpRatio02_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio10_BU_Date","LmdInj_AdpRatio10_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio11_BU_Date","LmdInj_AdpRatio11_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio12_BU_Date","LmdInj_AdpRatio12_CNG_BU_Date","LmdInj_AdpRatioIdl_BU_Date",
                                                                           "LmdInj_AdpRatioIdl_CNG_BU_Date"])
    
    
    df_temp_monit_file = df_temp_monit_file.sort_values("TM_date")
    df_torque_adap_file = df_torque_adap_file.sort_values("Date_Torque_Adap")
    df_load_monit_file = df_load_monit_file.sort_values("LM_Date")
    

    df_cata_monitoring_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_cata_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_crcme_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_crcmi_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_egr_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_fo2_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_general_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_ro2_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_vvte_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_vvti_file.reset_index(drop=True, inplace=True)
    df_cata_temp_file.reset_index(drop=True, inplace=True)
    df_misfire_monit_file.reset_index(drop=True, inplace=True)
    df_monit_fo2_file.reset_index(drop=True, inplace=True)
    df_monit_ro2_file.reset_index(drop=True, inplace=True)
    df_monit_fuel_system_file.reset_index(drop=True, inplace=True)
    df_vvte_monitoring_file.reset_index(drop=True, inplace=True)
    df_vvti_monitoring_file.reset_index(drop=True, inplace=True)
    df_crcmi_monitoring_file.reset_index(drop=True, inplace=True)
    df_crcme_monitoring_file.reset_index(drop=True, inplace=True)
    df_egr_monitoring_file.reset_index(drop=True, inplace=True)
    df_throttle_adap_file.reset_index(drop=True, inplace=True)
    df_fuel_adap_file.reset_index(drop=True, inplace=True)
    df_fuel_adap_onidle_file.reset_index(drop=True, inplace=True)
    df_fuel_adap_3sec_cos_file.reset_index(drop=True, inplace=True)
    df_fuel_adap_zone_wise_file.reset_index(drop=True, inplace=True)
    
    df_temp_monit_file.reset_index(drop=True, inplace=True)
    df_torque_adap_file.reset_index(drop=True, inplace=True)
    df_load_monit_file.reset_index(drop=True, inplace=True)
    
    all_dfs["Catalyst Monitoring"] = df_cata_monitoring_file
    all_dfs["Catalyst Temperature"] = df_cata_temp_file
    all_dfs["Misfire Monitoring"] = df_misfire_monit_file
    all_dfs["Catalyst Monitoring IUPR"] = df_monit_iupr_cata_file
    all_dfs["Crcme Monitoring IUPR"] = df_monit_iupr_crcme_file
    all_dfs["CrCmi Monitoring IUPR"] = df_monit_iupr_crcmi_file
    all_dfs["EGR Monitoring IUPR"] = df_monit_iupr_egr_file
    all_dfs["FO2 Monitoring IUPR"] = df_monit_iupr_fo2_file
    all_dfs["General IUPR"] = df_monit_iupr_general_file
    all_dfs["RO2 Monitoring IUPR"] = df_monit_iupr_ro2_file
    all_dfs["VVTe Monitoring IUPR"] = df_monit_iupr_vvte_file
    all_dfs["VVTi Monitoring IUPR"] = df_monit_iupr_vvti_file
    all_dfs["FO2 Monitoring"] = df_monit_fo2_file
    all_dfs["RO2 Monitoring"] = df_monit_ro2_file
    all_dfs["Fuel System"] = df_monit_fuel_system_file
    all_dfs["VVTe Monitoring"] = df_vvte_monitoring_file
    all_dfs["VVTi Monitoring"] = df_vvti_monitoring_file
    all_dfs["CrCmi Monitoring"] = df_crcmi_monitoring_file
    all_dfs["CrCme Monitoring"] = df_crcme_monitoring_file
    all_dfs["EGR Monitoring"] = df_egr_monitoring_file
    all_dfs["Throttle Adaptation"] = df_throttle_adap_file
    all_dfs["Fuel Adaptation"] = df_fuel_adap_file
    all_dfs["Fuel Adaptation OnIdle"] = df_fuel_adap_onidle_file
    all_dfs["Fuel Adaptation 3 sec cons"] = df_fuel_adap_3sec_cos_file
    all_dfs["Fuel Adaptation Zone Wise"] = df_fuel_adap_zone_wise_file
    all_dfs["Temperature Monitoring"] = df_temp_monit_file
    all_dfs["Torque Adaptation"] = df_torque_adap_file
    all_dfs["Load Monitoring"] = df_load_monit_file
    
    return all_dfs

def create_df_clubbed(all_dfs, vehicle_name):
    len_dfs = {}
    for keys in all_dfs :
        len_dfs[keys] = len(all_dfs[keys])
    max_key = max(len_dfs.items(), key=operator.itemgetter(1))[0]
#     print(max_key)
    max_length = len_dfs[max_key]
    for keys in all_dfs :
        if keys != max_key :
            copy_key_df = all_dfs[keys]
            for i in range(len(copy_key_df) , max_length + 1) :
                copy_key_df.append(pd.Series(), ignore_index=True)
            
            all_dfs[keys] = copy_key_df
    list_vehicle_name = []
    for i in range(0, max_length) :
        list_vehicle_name.append(vehicle_name)
        
    df_clubbed = pd.DataFrame({"Vehicle_Name" : list_vehicle_name})
    
    for keys in all_dfs :
        df_clubbed = pd.concat([df_clubbed, all_dfs[keys]], axis=1)
        
    return df_clubbed

def get_clubbed_dfs(df_clubbed_vehicle, all_dfs, vehicle_name) :
    all_required_fields = get_required_fields(all_dfs)
    df_monit_iupr_cata_file = df_clubbed_vehicle[all_required_fields["Catalyst Monitoring IUPR"]]
    df_monit_iupr_crcme_file = df_clubbed_vehicle[all_required_fields["Crcme Monitoring IUPR"]]
    df_monit_iupr_crcmi_file = df_clubbed_vehicle[all_required_fields["CrCmi Monitoring IUPR"]]
    df_monit_iupr_egr_file = df_clubbed_vehicle[all_required_fields["EGR Monitoring IUPR"]]
    df_monit_iupr_fo2_file = df_clubbed_vehicle[all_required_fields["FO2 Monitoring IUPR"]]
    df_monit_iupr_general_file = df_clubbed_vehicle[all_required_fields["General IUPR"]]
    df_monit_iupr_ro2_file = df_clubbed_vehicle[all_required_fields["RO2 Monitoring IUPR"]]
    df_monit_iupr_vvte_file = df_clubbed_vehicle[all_required_fields["VVTe Monitoring IUPR"]]
    df_monit_iupr_vvti_file = df_clubbed_vehicle[all_required_fields["VVTi Monitoring IUPR"]]
    df_cata_monitoring_file = df_clubbed_vehicle[all_required_fields["Catalyst Monitoring"]]
    df_cata_temp_file = df_clubbed_vehicle[all_required_fields["Catalyst Temperature"]]
    df_misfire_monit_file = df_clubbed_vehicle[all_required_fields["Misfire Monitoring"]]
    df_monit_fo2_file = df_clubbed_vehicle[all_required_fields["FO2 Monitoring"]]
    df_monit_ro2_file = df_clubbed_vehicle[all_required_fields["RO2 Monitoring"]]
    df_monit_fuel_system_file = df_clubbed_vehicle[all_required_fields["Fuel System"]]
    df_vvte_monitoring_file = df_clubbed_vehicle[all_required_fields["VVTe Monitoring"]]
    df_vvti_monitoring_file = df_clubbed_vehicle[all_required_fields["VVTi Monitoring"]]
    df_crcmi_monitoring_file = df_clubbed_vehicle[all_required_fields["CrCmi Monitoring"]]
    df_crcme_monitoring_file = df_clubbed_vehicle[all_required_fields["CrCme Monitoring"]]
    df_egr_monitoring_file = df_clubbed_vehicle[all_required_fields["EGR Monitoring"]]
    df_throttle_adap_file = df_clubbed_vehicle[all_required_fields["Throttle Adaptation"]]
    df_fuel_adap_file = df_clubbed_vehicle[all_required_fields["Fuel Adaptation"]]
    df_fuel_adap_onidle_file = df_clubbed_vehicle[all_required_fields["Fuel Adaptation OnIdle"]]
    df_fuel_adap_3sec_cos_file = df_clubbed_vehicle[all_required_fields["Fuel Adaptation 3 sec cons"]]
    df_fuel_adap_zone_wise_file = df_clubbed_vehicle[all_required_fields["Fuel Adaptation Zone Wise"]]
    df_temp_monit_file = df_clubbed_vehicle[all_required_fields["Temperature Monitoring"]]
    df_torque_adap_file = df_clubbed_vehicle[all_required_fields["Torque Adaptation"]]
    df_load_monit_file = df_clubbed_vehicle[all_required_fields["Load Monitoring"]]
    
#     print("test")
    df_monit_iupr_cata_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_iupr_crcme_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_iupr_crcmi_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_iupr_egr_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_iupr_fo2_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_iupr_general_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_iupr_ro2_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_iupr_vvte_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_iupr_vvti_file.dropna(axis = 0, how = 'all', inplace = True)
    df_cata_monitoring_file.dropna(axis = 0, how = 'all', inplace = True)
    df_cata_temp_file.dropna(axis = 0, how = 'all', inplace = True)
    df_misfire_monit_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_fo2_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_ro2_file.dropna(axis = 0, how = 'all', inplace = True)
    df_monit_fuel_system_file.dropna(axis = 0, how = 'all', inplace = True)
    df_vvte_monitoring_file.dropna(axis = 0, how = 'all', inplace = True)
    df_vvti_monitoring_file.dropna(axis = 0, how = 'all', inplace = True)
    df_crcmi_monitoring_file.dropna(axis = 0, how = 'all', inplace = True)
    df_crcme_monitoring_file.dropna(axis = 0, how = 'all', inplace = True)
    df_egr_monitoring_file.dropna(axis = 0, how = 'all', inplace = True)
    df_throttle_adap_file.dropna(axis = 0, how = 'all', inplace = True)
    df_fuel_adap_file.dropna(axis = 0, how = 'all', inplace = True)
    df_fuel_adap_onidle_file.dropna(axis = 0, how = 'all', inplace = True)
    df_fuel_adap_3sec_cos_file.dropna(axis = 0, how = 'all', inplace = True)
    df_fuel_adap_zone_wise_file.dropna(axis = 0, how = 'all', inplace = True)
    df_temp_monit_file.dropna(axis = 0, how = 'all', inplace = True)
    df_torque_adap_file.dropna(axis = 0, how = 'all', inplace = True)
    df_load_monit_file.dropna(axis = 0, how = 'all', inplace = True)
#     print("***************************", df_monit_iupr_cata_file)
    
    df_cata_monitoring_file = pd.concat([all_dfs["Catalyst Monitoring"],df_cata_monitoring_file]).drop_duplicates().reset_index(drop=True)
    df_monit_iupr_cata_file = pd.concat([all_dfs["Catalyst Monitoring IUPR"],df_monit_iupr_cata_file]).drop_duplicates().reset_index(drop=True)
    df_monit_iupr_crcme_file = pd.concat([all_dfs["Crcme Monitoring IUPR"],df_monit_iupr_crcme_file]).drop_duplicates().reset_index(drop=True)
    df_monit_iupr_crcmi_file = pd.concat([all_dfs["CrCmi Monitoring IUPR"],df_monit_iupr_crcmi_file]).drop_duplicates().reset_index(drop=True)
    df_monit_iupr_egr_file = pd.concat([all_dfs["EGR Monitoring IUPR"],df_monit_iupr_egr_file]).drop_duplicates().reset_index(drop=True)
    df_monit_iupr_fo2_file = pd.concat([all_dfs["FO2 Monitoring IUPR"],df_monit_iupr_fo2_file]).drop_duplicates().reset_index(drop=True)
    df_monit_iupr_general_file = pd.concat([all_dfs["General IUPR"],df_monit_iupr_general_file]).drop_duplicates().reset_index(drop=True)
    df_monit_iupr_ro2_file = pd.concat([all_dfs["RO2 Monitoring IUPR"],df_monit_iupr_ro2_file]).drop_duplicates().reset_index(drop=True)
    df_monit_iupr_vvte_file = pd.concat([all_dfs["VVTe Monitoring IUPR"],df_monit_iupr_vvte_file]).drop_duplicates().reset_index(drop=True)
    df_monit_iupr_vvti_file = pd.concat([all_dfs["VVTi Monitoring IUPR"],df_monit_iupr_vvti_file]).drop_duplicates().reset_index(drop=True)
    df_cata_temp_file = pd.concat([all_dfs["Catalyst Temperature"],df_cata_temp_file]).drop_duplicates().reset_index(drop=True)
    df_misfire_monit_file = pd.concat([all_dfs["Misfire Monitoring"],df_misfire_monit_file]).drop_duplicates().reset_index(drop=True)
    df_monit_fo2_file = pd.concat([all_dfs["FO2 Monitoring"],df_monit_fo2_file]).drop_duplicates().reset_index(drop=True)
    df_monit_ro2_file = pd.concat([all_dfs["RO2 Monitoring"],df_monit_ro2_file]).drop_duplicates().reset_index(drop=True)
    df_monit_fuel_system_file = pd.concat([all_dfs["Fuel System"],df_monit_fuel_system_file]).drop_duplicates().reset_index(drop=True)
    df_vvte_monitoring_file = pd.concat([all_dfs["VVTe Monitoring"],df_vvte_monitoring_file]).drop_duplicates().reset_index(drop=True)
    df_vvti_monitoring_file = pd.concat([all_dfs["VVTi Monitoring"],df_vvti_monitoring_file]).drop_duplicates().reset_index(drop=True)
    df_crcmi_monitoring_file = pd.concat([all_dfs["CrCmi Monitoring"],df_crcmi_monitoring_file]).drop_duplicates().reset_index(drop=True)
    df_crcme_monitoring_file = pd.concat([all_dfs["CrCme Monitoring"],df_crcme_monitoring_file]).drop_duplicates().reset_index(drop=True)
    df_egr_monitoring_file = pd.concat([all_dfs["EGR Monitoring"],df_egr_monitoring_file]).drop_duplicates().reset_index(drop=True)
    df_throttle_adap_file = pd.concat([all_dfs["Throttle Adaptation"],df_throttle_adap_file]).drop_duplicates().reset_index(drop=True)
    df_fuel_adap_file = pd.concat([all_dfs["Fuel Adaptation"],df_fuel_adap_file]).drop_duplicates().reset_index(drop=True)
    df_fuel_adap_onidle_file = pd.concat([all_dfs["Fuel Adaptation OnIdle"],df_fuel_adap_onidle_file]).drop_duplicates().reset_index(drop=True)
    df_fuel_adap_3sec_cos_file = pd.concat([all_dfs["Fuel Adaptation 3 sec cons"],df_fuel_adap_3sec_cos_file]).drop_duplicates().reset_index(drop=True)
    
    df_fuel_adap_zone_wise_file = pd.concat([all_dfs["Fuel Adaptation Zone Wise"],df_fuel_adap_zone_wise_file]).drop_duplicates().reset_index(drop=True)
    df_temp_monit_file = pd.concat([all_dfs["Temperature Monitoring"],df_temp_monit_file]).drop_duplicates().reset_index(drop=True)
    df_torque_adap_file = pd.concat([all_dfs["Torque Adaptation"],df_torque_adap_file]).drop_duplicates().reset_index(drop=True)
    df_load_monit_file = pd.concat([all_dfs["Load Monitoring"],df_load_monit_file]).drop_duplicates().reset_index(drop=True)

    #     print("***************************", df_monit_iupr_cata_file)
    ##### Convert Date from string to datetime
    df_cata_monitoring_file['CM_date'] = pd.to_datetime(df_cata_monitoring_file['CM_date'], format="%d-%m-%Y")
    df_monit_iupr_cata_file['IUPR_CM_Date'] = pd.to_datetime(df_monit_iupr_cata_file['IUPR_CM_Date'], format="%d-%m-%Y")
    df_monit_iupr_crcme_file['IUPR_CrCme_Date'] = pd.to_datetime(df_monit_iupr_crcme_file['IUPR_CrCme_Date'], format="%d-%m-%Y")
    df_monit_iupr_crcmi_file['IUPR_CrCmi_Date'] = pd.to_datetime(df_monit_iupr_crcmi_file['IUPR_CrCmi_Date'], format="%d-%m-%Y")
    df_monit_iupr_egr_file['IUPR_EGR_Date'] = pd.to_datetime(df_monit_iupr_egr_file['IUPR_EGR_Date'], format="%d-%m-%Y")
    df_monit_iupr_fo2_file['IUPR_FO2_Date'] = pd.to_datetime(df_monit_iupr_fo2_file['IUPR_FO2_Date'], format="%d-%m-%Y")
    df_monit_iupr_general_file['IUPR_GEN_Date'] = pd.to_datetime(df_monit_iupr_general_file['IUPR_GEN_Date'], format="%d-%m-%Y")
    df_monit_iupr_ro2_file['IUPR_RO2_Date'] = pd.to_datetime(df_monit_iupr_ro2_file['IUPR_RO2_Date'], format="%d-%m-%Y")
    df_monit_iupr_vvte_file['IUPR_VVTe_Date'] = pd.to_datetime(df_monit_iupr_vvte_file['IUPR_VVTe_Date'], format="%d-%m-%Y")
    df_monit_iupr_vvti_file['IUPR_VVTi_Date'] = pd.to_datetime(df_monit_iupr_vvti_file['IUPR_VVTi_Date'], format="%d-%m-%Y")
    df_cata_temp_file['CT_date'] = pd.to_datetime(df_cata_temp_file['CT_date'], format="%d-%m-%Y")
    df_misfire_monit_file['MFM_Date'] = pd.to_datetime(df_misfire_monit_file['MFM_Date'], format="%d-%m-%Y")
    df_monit_fo2_file['FO2_Date'] = pd.to_datetime(df_monit_fo2_file['FO2_Date'], format="%d-%m-%Y")
    df_monit_ro2_file['RO2_Date'] = pd.to_datetime(df_monit_ro2_file['RO2_Date'], format="%d-%m-%Y")
    df_monit_fuel_system_file['FS_date'] = pd.to_datetime(df_monit_fuel_system_file['FS_date'], format="%d-%m-%Y")
    df_vvte_monitoring_file['VVTe_Date'] = pd.to_datetime(df_vvte_monitoring_file['VVTe_Date'], format="%d-%m-%Y")
    df_vvti_monitoring_file['VVTi_Date'] = pd.to_datetime(df_vvti_monitoring_file['VVTi_Date'], format="%d-%m-%Y")
    df_crcmi_monitoring_file['CrCmi_Date'] = pd.to_datetime(df_crcmi_monitoring_file['CrCmi_Date'], format="%d-%m-%Y")
    df_crcme_monitoring_file['CrCme_Date'] = pd.to_datetime(df_crcme_monitoring_file['CrCme_Date'], format="%d-%m-%Y")
    df_egr_monitoring_file['EGR_Dates'] = pd.to_datetime(df_egr_monitoring_file['EGR_Dates'], format="%d-%m-%Y")
    df_throttle_adap_file['Throttle_Adap_Dates'] = pd.to_datetime(df_throttle_adap_file['Throttle_Adap_Dates'], format="%d-%m-%Y")
    df_fuel_adap_file['Fuel_Adap_Date'] = pd.to_datetime(df_fuel_adap_file['Fuel_Adap_Date'], format="%d-%m-%Y")
    df_fuel_adap_onidle_file['Fuel_Adap_Date_onidle'] = pd.to_datetime(df_fuel_adap_onidle_file['Fuel_Adap_Date_onidle'], format="%d-%m-%Y")
    df_fuel_adap_3sec_cos_file['Date_Fuel_AD_3_Sec'] = pd.to_datetime(df_fuel_adap_3sec_cos_file['Date_Fuel_AD_3_Sec'], format="%d-%m-%Y")
    
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio_CNG_[0]_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio_CNG_[0]_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio_PET_[0]_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio_PET_[0]_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio00_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio00_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio00_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio00_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio01_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio01_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio01_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio01_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio02_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio02_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio02_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio02_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio10_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio10_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio10_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio10_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio11_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio11_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio11_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio11_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio12_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio12_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatioIdl_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatioIdl_BU_Date"], format="%d-%m-%Y")
    
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatio12_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatio12_CNG_BU_Date"], format="%d-%m-%Y")
    df_fuel_adap_zone_wise_file["LmdInj_AdpRatioIdl_CNG_BU_Date"] = pd.to_datetime(df_fuel_adap_zone_wise_file["LmdInj_AdpRatioIdl_CNG_BU_Date"], format="%d-%m-%Y")
    
    df_temp_monit_file['TM_date'] = pd.to_datetime(df_temp_monit_file['TM_date'], format="%d-%m-%Y")
    df_torque_adap_file['Date_Torque_Adap'] = pd.to_datetime(df_torque_adap_file['Date_Torque_Adap'], format="%d-%m-%Y")
    df_load_monit_file['LM_Date'] = pd.to_datetime(df_load_monit_file['LM_Date'], format="%d-%m-%Y")

    #### Sorting Data
    
    df_cata_monitoring_file = df_cata_monitoring_file.sort_values("CM_date")
    df_monit_iupr_cata_file = df_monit_iupr_cata_file.sort_values("IUPR_CM_Date")
    df_monit_iupr_crcme_file = df_monit_iupr_crcme_file.sort_values("IUPR_CrCme_Date")
    df_monit_iupr_crcmi_file = df_monit_iupr_crcmi_file.sort_values("IUPR_CrCmi_Date")
    df_monit_iupr_egr_file = df_monit_iupr_egr_file.sort_values("IUPR_EGR_Date")
    df_monit_iupr_fo2_file = df_monit_iupr_fo2_file.sort_values("IUPR_FO2_Date")
    df_monit_iupr_general_file = df_monit_iupr_general_file.sort_values("IUPR_GEN_Date")
    df_monit_iupr_ro2_file = df_monit_iupr_ro2_file.sort_values("IUPR_RO2_Date")
    df_monit_iupr_vvte_file = df_monit_iupr_vvte_file.sort_values("IUPR_VVTe_Date")
    df_monit_iupr_vvti_file = df_monit_iupr_vvti_file.sort_values("IUPR_VVTi_Date")
    df_cata_temp_file = df_cata_temp_file.sort_values("CT_date")
    df_misfire_monit_file = df_misfire_monit_file.sort_values("MFM_Date")
    df_monit_fo2_file = df_monit_fo2_file.sort_values("FO2_Date")
    df_monit_ro2_file =df_monit_ro2_file.sort_values("RO2_Date")
    df_monit_fuel_system_file = df_monit_fuel_system_file.sort_values("FS_date")
    df_vvte_monitoring_file = df_vvte_monitoring_file.sort_values("VVTe_Date")
    df_vvti_monitoring_file = df_vvti_monitoring_file.sort_values("VVTi_Date")
    df_crcmi_monitoring_file = df_crcmi_monitoring_file.sort_values("CrCmi_Date")
    df_crcme_monitoring_file = df_crcme_monitoring_file.sort_values("CrCme_Date")
    df_egr_monitoring_file = df_egr_monitoring_file.sort_values("EGR_Dates")
    df_throttle_adap_file = df_throttle_adap_file.sort_values("Throttle_Adap_Dates")
    df_fuel_adap_file = df_fuel_adap_file.sort_values("Fuel_Adap_Date")
    df_fuel_adap_onidle_file = df_fuel_adap_onidle_file.sort_values("Fuel_Adap_Date_onidle")
    df_fuel_adap_3sec_cos_file = df_fuel_adap_3sec_cos_file.sort_values("Date_Fuel_AD_3_Sec")
    df_fuel_adap_zone_wise_file = df_fuel_adap_zone_wise_file.sort_values(["LmdInj_AdpRatio_CNG_[0]_Date","LmdInj_AdpRatio_PET_[0]_Date"
                                                                           ,"LmdInj_AdpRatio00_BU_Date","LmdInj_AdpRatio00_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio01_BU_Date","LmdInj_AdpRatio01_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio02_BU_Date","LmdInj_AdpRatio02_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio10_BU_Date","LmdInj_AdpRatio10_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio11_BU_Date","LmdInj_AdpRatio11_CNG_BU_Date",
                                                                           "LmdInj_AdpRatio12_BU_Date","LmdInj_AdpRatio12_CNG_BU_Date","LmdInj_AdpRatio12_CNG_BU_Date","LmdInj_AdpRatioIdl_BU_Date",
                                                                           "LmdInj_AdpRatioIdl_CNG_BU_Date"])
    
    
    df_temp_monit_file = df_temp_monit_file.sort_values("TM_date")
    df_torque_adap_file = df_torque_adap_file.sort_values("Date_Torque_Adap")
    df_load_monit_file = df_load_monit_file.sort_values("LM_Date")
    
    
    df_cata_monitoring_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_cata_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_crcme_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_crcmi_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_egr_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_fo2_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_general_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_ro2_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_vvte_file.reset_index(drop=True, inplace=True)
    df_monit_iupr_vvti_file.reset_index(drop=True, inplace=True)
    df_cata_temp_file.reset_index(drop=True, inplace=True)
    df_misfire_monit_file.reset_index(drop=True, inplace=True)
    df_monit_fo2_file.reset_index(drop=True, inplace=True)
    df_monit_ro2_file.reset_index(drop=True, inplace=True)
    df_monit_fuel_system_file.reset_index(drop=True, inplace=True)
    df_vvte_monitoring_file.reset_index(drop=True, inplace=True)
    df_vvti_monitoring_file.reset_index(drop=True, inplace=True)
    df_crcmi_monitoring_file.reset_index(drop=True, inplace=True)
    df_crcme_monitoring_file.reset_index(drop=True, inplace=True)
    df_egr_monitoring_file.reset_index(drop=True, inplace=True)
    df_throttle_adap_file.reset_index(drop=True, inplace=True)
    df_fuel_adap_file.reset_index(drop=True, inplace=True)
    df_fuel_adap_onidle_file.reset_index(drop=True, inplace=True)
    df_fuel_adap_3sec_cos_file.reset_index(drop=True, inplace=True)
    df_fuel_adap_zone_wise_file.reset_index(drop=True, inplace=True)
    
    df_temp_monit_file.reset_index(drop=True, inplace=True)
    df_torque_adap_file.reset_index(drop=True, inplace=True)
    df_load_monit_file.reset_index(drop=True, inplace=True)
    ##### Merging data
    
    
    
    all_dfs = {}
    
    all_dfs["Catalyst Monitoring"] = df_cata_monitoring_file
    all_dfs["Catalyst Temperature"] = df_cata_temp_file
    all_dfs["Misfire Monitoring"] = df_misfire_monit_file
    all_dfs["Catalyst Monitoring IUPR"] = df_monit_iupr_cata_file
    all_dfs["Crcme Monitoring IUPR"] = df_monit_iupr_crcme_file
    all_dfs["CrCmi Monitoring IUPR"] = df_monit_iupr_crcmi_file
    all_dfs["EGR Monitoring IUPR"] = df_monit_iupr_egr_file
    all_dfs["FO2 Monitoring IUPR"] = df_monit_iupr_fo2_file
    all_dfs["General IUPR"] = df_monit_iupr_general_file
    all_dfs["RO2 Monitoring IUPR"] = df_monit_iupr_ro2_file
    all_dfs["VVTe Monitoring IUPR"] = df_monit_iupr_vvte_file
    all_dfs["VVTi Monitoring IUPR"] = df_monit_iupr_vvti_file
    all_dfs["FO2 Monitoring"] = df_monit_fo2_file
    all_dfs["RO2 Monitoring"] = df_monit_ro2_file
    all_dfs["Fuel System"] = df_monit_fuel_system_file
    all_dfs["VVTe Monitoring"] = df_vvte_monitoring_file
    all_dfs["VVTi Monitoring"] = df_vvti_monitoring_file
    all_dfs["CrCmi Monitoring"] = df_crcmi_monitoring_file
    all_dfs["CrCme Monitoring"] = df_crcme_monitoring_file
    all_dfs["EGR Monitoring"] = df_egr_monitoring_file
    all_dfs["Throttle Adaptation"] = df_throttle_adap_file
    all_dfs["Fuel Adaptation"] = df_fuel_adap_file
    all_dfs["Fuel Adaptation OnIdle"] = df_fuel_adap_onidle_file
    all_dfs["Fuel Adaptation 3 sec cons"] = df_fuel_adap_3sec_cos_file
    all_dfs["Fuel Adaptation Zone Wise"] = df_fuel_adap_zone_wise_file
    all_dfs["Temperature Monitoring"] = df_temp_monit_file
    all_dfs["Torque Adaptation"] = df_torque_adap_file
    all_dfs["Load Monitoring"] = df_load_monit_file
#     print("test 3")
    
    
    df_clubbed_vehicle = create_df_clubbed(all_dfs, vehicle_name)
#     print("test 4")
    return df_clubbed_vehicle


def fetch_all_dataframes(df_clubbed, all_dfs) :
    count = 0
    for keys in df_clubbed.groupby("Vehicle_Name").groups :
        df_clubbed_vehicle = df_clubbed.groupby("Vehicle_Name").get_group(keys)
        if keys in all_dfs and check_for_empty_frames(all_dfs[keys]):
            
#             print(df_clubbed_vehicle)
            
            df_clubbed_vehicle = get_clubbed_dfs(df_clubbed_vehicle, all_dfs[keys], keys)
#             print(df_clubbed_vehicle)
            if count == 0 :
                count = count + 1
                all_df_clubbed_vehicle = df_clubbed_vehicle
            else :
                all_df_clubbed_vehicle  = pd.concat([all_df_clubbed_vehicle, df_clubbed_vehicle]).drop_duplicates()

#             print(all_df_clubbed_vehicle)
        else :
#             print("came here" , df_clubbed_vehicle)
            if count == 0 :
                count = count + 1
                all_df_clubbed_vehicle = df_clubbed_vehicle
            all_df_clubbed_vehicle  = pd.concat([all_df_clubbed_vehicle, df_clubbed_vehicle]).drop_duplicates()
#     all_df_clubbed_vehicle = pd.concat([all_df_clubbed_vehicle, create_df_clubbed_data(all_dfs)]).drop_duplicates()
#     print(all_df_clubbed_vehicle["Vehicle_Name"])
    return all_df_clubbed_vehicle
    
    
def create_df_clubbed_data_all(vehicle_wise_triggers_mapping) :
    count = 0
    for keys in vehicle_wise_triggers_mapping :
        df_clubbed_vehicle = vehicle_wise_triggers_mapping[keys]
        df_clubbed_vehicle = create_df_clubbed(vehicle_wise_triggers_mapping[keys], keys)
        if count == 0 :
            count = count + 1
            all_df_clubbed_vehicle = df_clubbed_vehicle
        else :
            all_df_clubbed_vehicle  = pd.concat([all_df_clubbed_vehicle, df_clubbed_vehicle])
    return all_df_clubbed_vehicle

def create_df_clubbed_data(vehicle_wise_triggers_mapping, keys) :
    vehicle_wise_triggers_mapping = sort_all_value(vehicle_wise_triggers_mapping)
#     print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$",vehicle_wise_triggers_mapping)
    all_df_clubbed_vehicle = create_df_clubbed(vehicle_wise_triggers_mapping, keys)
#     print("-----------------------------------------", all_df_clubbed_vehicle)
    return all_df_clubbed_vehicle
 
 


# In[26]:


def merge_with_final_dataframe(df_clubbed) :
    df_clubbed_all = pd.DataFrame({"Vehicle_Name" :[],"CT_date":[],"DC1_Th1_Max" :[],"DC1_Th2_Max" :[],"DC1_Th3_Max" :[],"DC1_Th4_Max" :[],"Min_Alarming_Value_CT" :[],"Max_Alarming_Value_CT" :[],"CM_date":[],"FldCat_catgav2" :[],"FldCat_cattdlyb" :[],"DlgCat_soxvcat" :[],"Max_Alarming_Value_CM" :[],"Min_Alarming_Value_CM" :[],"MFM_Date":[],"Max_FldMsf_cmiswa2" :[],"FldMsf_crough2_corrosp" :[],"FldMsf_cmis21":[],"FldMsf_cmis22":[], 
                                   "FldMsf_cmis23":[],"FldMsf_cmis24":[], "Min_Alarming_Value_MFM" :[],"Max_Alarming_Value_MFM" :[],"IUPR_CM_Date":[],"DgsRate_cntdencat" :[],"DgsRate_cntnumcat" :[],"IUPR_CM_Ratio" :[],"Max_Alarming_Value_IUPR_CM" :[],"Min_Alarming_Value_IUPR_CM" :[],"IUPR_FO2_Date":[],"DgsRate_cntdenoxcs" :[],"DgsRate_cntnumoxcs" :[],"IUPR_FO2_Ratio" :[],"Max_Alarming_Value_IUPR_FO2" :[],"Min_Alarming_Value_IUPR_FO2" :[],
                                   "IUPR_VVTi_Date":[],"DgsRate_cntdenvvtr" :[],"DgsRate_cntnumvvtr" :[],"IUPR_VVTi_Ratio" :[],"Max_Alarming_Value_IUPR_VVTi" :[],"Min_Alarming_Value_IUPR_VVTi" :[],"IUPR_RO2_Date":[],"DgsRate_cntdensoxcr" :[],"DgsRate_cntnumsoxcr" :[],"IUPR_RO2_Ratio" :[],"Max_Alarming_Value_IUPR_RO2" :[],"Min_Alarming_Value_IUPR_RO2" :[],"IUPR_VVTe_Date":[],"DgsRate_cntdenexvvta" :[],"DgsRate_cntnumexvvta" :[],"IUPR_VVTe_Ratio" :[],"Max_Alarming_Value_IUPR_VVTe" :[],
                                   "Min_Alarming_Value_IUPR_VVTe" :[],"IUPR_CrCmi_Date":[],"DgsRate_cntdencrcm" :[],"DgsRate_cntnumcrcm" :[],"IUPR_CrCmi_Ratio" :[],"Max_Alarming_Value_IUPR_CrCmi" :[],"Min_Alarming_Value_IUPR_CrCmi" :[],"IUPR_CrCme_Date":[],"DgsRate_cntdenexcrcm" :[],"DgsRate_cntnumexcrcm" :[],"IUPR_CrCme_Ratio" :[],"Max_Alarming_Value_IUPR_CrCme" :[],"Min_Alarming_Value_IUPR_CrCme" :[],"IUPR_EGR_Date":[],"DgsRate_cntdenegrcl" :[],
                                   "DgsRate_cntnumegrcl" :[],"IUPR_EGR_Ratio" :[],"Max_Alarming_Value_IUPR_EGR" :[],"Min_Alarming_Value_IUPR_EGR" :[],"IUPR_GEN_Date":[],"Max_DgsRate_rateigcnt" :[],"Max_DgsRate_rategenden" :[],"Max_DgsRate_cntnumvvtr" :[],"Max_DgsRate_cntnumexvvta" :[],"Max_DgsRate_cntnumegrcl" :[],"Max_Alarming_Value_IUPR_GEN" :[],"Min_Alarming_Value_IUPR_GEN" :[],"FO2_Date":[],"FldOxpc_tfboxsavsdl" :[],"Max_Alarming_Value_FO2" :[],
                                   "Min_Alarming_Value_FO2" :[],"RO2_Date":[],"FldOxscr_cosocr1d" :[],"Min_Alarming_Value_RO2" :[],"Max_Alarming_Value_RO2" :[],"FS_date":[],"FldFue_cofslean" :[],"FldFue_cofsrich" :[],"FldFue_cofsleangas" :[],"FldFue_cofsrichgas" :[],"Min_Alarming_Value_FS" :[],"Max_Alarming_Value_FS" :[],"VVTi_Date":[],"FldVti_vtdobdsm" :[],"Max_Alarming_Value_VVTi" :[],"Min_Alarming_Value_VVTi" :[],"VVTe_Date":[],"FldVte_vtdobdsm_ex" :[],
                                   "Max_Alarming_Value_VVTe" :[],"CrCmi_Date":[],"FldInf_VtiOfsAdvance_bank_min_value" :[],"FldInf_VtiOfsAdvance_bank_max_value" :[],"Min_Alarming_Value_CrCmi" :[],"Max_Alarming_Value_CrCmi" :[],"CrCme_Date":[],"FldInf_VteOfsAdvance_bank_min_value" :[],"FldInf_VteOfsAdvance_bank_max_value" :[],"Min_Alarming_Value_CrCme" :[],"Max_Alarming_Value_CrCme" :[],"EGR_Dates":[],"FldEgrc_odpmegr" :[],"FldEgrc_oegrcdpml" :[],"Min_Alarming_Value_EGR" :[],
                                   "Max_Alarming_Value_EGR" :[],"Throttle_Adap_Dates":[],"AirFbk_AdpFlow00_BU_Max" :[],"AirFbk_AdpFlow01_BU_Max" :[],"AirFbk_AdpFlow02_BU_Max" :[],"AirFbk_AdpFlow03_BU_Max" :[],"AirFbk_AdpFlow04_BU_Max" :[],"AirFbk_AdpFlow05_BU_Max" :[],"AirFbk_AdpFlow06_BU_Max" :[],"Min_Alarming_Value_th_adap" :[],"Max_Alarming_Value_th_adap" :[],"Fuel_Adap_Date":[],"LmdInj_CmpRatio" :[],"Fuel_Adap_Date_onidle" : [], "LmdInj_CmpRatio_onidle" : [], 
                                   "Date_Fuel_AD_3_Sec" : [], "LmdInj_CmpRatio_3_sec" : [], "PdlAcp_Position" :[], "ItkAir_AirChgRatio" : [], "ApfCrk_EngSpeed" : [],"Min_Alarming_Value_fuel_adap" :[],"Max_Alarming_Value_fuel_adap" :[],"LmdInj_AdpRatio_CNG_[0]_Date":[],"LmdInj_AdpRatio_CNG_[0]_Min" :[],"LmdInj_AdpRatio_CNG_[0]_Max" :[],"LmdInj_AdpRatio_PET_[0]_Date":[],"LmdInj_AdpRatio_PET_[0]_Min" :[],"LmdInj_AdpRatio_PET_[0]_Max" :[],"LmdInj_AdpRatio00_BU_Date":[],"LmdInj_AdpRatio00_BU_Min" :[],
                                   "LmdInj_AdpRatio00_BU_Max" :[],"LmdInj_AdpRatio00_CNG_BU_Date":[],"LmdInj_AdpRatio00_CNG_BU_Min" :[],"LmdInj_AdpRatio00_CNG_BU_Max" :[],"LmdInj_AdpRatio01_BU_Date":[],"LmdInj_AdpRatio01_BU_Min" :[],"LmdInj_AdpRatio01_BU_Max" :[],"LmdInj_AdpRatio01_CNG_BU_Date":[],"LmdInj_AdpRatio01_CNG_BU_Min" :[],"LmdInj_AdpRatio01_CNG_BU_Max" :[],"LmdInj_AdpRatio02_BU_Date":[],"LmdInj_AdpRatio02_BU_Min" :[],"LmdInj_AdpRatio02_BU_Max" :[],"LmdInj_AdpRatio02_CNG_BU_Date":[],
                                   "LmdInj_AdpRatio02_CNG_BU_Min" :[],"LmdInj_AdpRatio02_CNG_BU_Max" :[],"LmdInj_AdpRatio10_BU_Date":[],"LmdInj_AdpRatio10_BU_Min" :[],"LmdInj_AdpRatio10_BU_Max" :[],"LmdInj_AdpRatio10_CNG_BU_Min" :[],"LmdInj_AdpRatio10_CNG_BU_Date":[], "LmdInj_AdpRatio10_CNG_BU_Max" :[],"LmdInj_AdpRatio11_BU_Date":[],"LmdInj_AdpRatio11_BU_Min" :[],"LmdInj_AdpRatio11_BU_Max" :[],"LmdInj_AdpRatio11_CNG_BU_Date":[],
                                   "LmdInj_AdpRatio11_CNG_BU_Min" :[],"LmdInj_AdpRatio11_CNG_BU_Max" :[],"LmdInj_AdpRatio12_BU_Date":[],"LmdInj_AdpRatio12_BU_Min" :[],"LmdInj_AdpRatio12_BU_Max" :[],"LmdInj_AdpRatio12_CNG_BU_Date":[],
                                   "LmdInj_AdpRatio12_CNG_BU_Min" :[],"LmdInj_AdpRatio12_CNG_BU_Max" :[],"LmdInj_AdpRatioIdl_BU_Date":[],"LmdInj_AdpRatioIdl_BU_Min" :[],"LmdInj_AdpRatioIdl_BU_Max" :[], "LmdInj_AdpRatioIdl_CNG_BU_Date":[],"LmdInj_AdpRatioIdl_CNG_BU_Min" :[],"LmdInj_AdpRatioIdl_CNG_BU_Max" :[],"Min_Alarming_Value_fuel_adap_zw" :[],
                                   "Max_Alarming_Value_fuel_adap_zw" :[], "TM_date" : [], "ExhTmp_CatTemperature" : [], "ApfOil_EngOilTemperature" : [], "EngClt_Temperature" : [],"TxcSts_VehSpeed":[], "Alarming_Value_temp_monit":[], "Date_Torque_Adap" : [], "TxcLos_LosTorque" : [], "Alarming_Value_torque_adap" : [],
                                  "LM_Date" : [], "RteCom_EngineCalculatedLoadDGS" : [], "TrqReq_FltRawTorque" : [], "DgsClv_load" : [], "ApfCrk_EngSpeed_LM" : [], "Alarming_Value_load_monitoring" :[]})
    df_final_clubbed = pd.DataFrame()
#     print("***********came here", df_clubbed.columns.to_list())
    
    for cols in df_clubbed_all.columns :
#         print(cols)
        
        if cols in df_clubbed.columns.to_list() :
        
            df_final_clubbed[cols] = df_clubbed[cols]
        else :
            df_final_clubbed[cols] = [np.nan for i in range(0, len(df_clubbed))]
    return df_final_clubbed



# In[27]:


def run_final_algorithm(vehicle_mapping, vehicle_name, alarming_values) :
    filter_fields = ["ApfCrk_EngSpeed"]
    cata_temp_required_fields = ["DC1_Th1","DC1_Th2", "DC1_Th3", "DC1_Th4", "DC1_Th5"]
    cata_monit_required_fields = ["FldCat_cattdlyb", "FldCat_catgav2", "DlgCat_soxvcat" , "FldCat_xocat1s", "FldCat_xocat1f"]
    misfire_monit_requirement_fields =["FldMsf_crough2","FldMsf_cmis21","FldMsf_cmis22","FldMsf_cmis23", "FldMsf_cmiswa2", "FldMsf_cmis24"]
    catalyst_monitoring_iupr_required_fields = ["DgsRate_cntdencat", "DgsRate_cntnumcat"]
    fo2_monitoring_iupr_required_fields = ["DgsRate_cntdenoxcs", "DgsRate_cntnumoxcs"]
    vvti_monitoring_iupr_required_fields = ["DgsRate_cntdenvvtr" ,"DgsRate_cntnumvvtr"]
    ro2_monitoring_iupr_required_fields = ["DgsRate_cntdensoxcr", "DgsRate_cntnumsoxcr"]
    vvte_monitoring_iupr_required_fields = ["DgsRate_cntdenexvvta", "DgsRate_cntnumexvvta"]
    crcmi_monitoring_iupr_required_fields = ["DgsRate_cntdencrcm", "DgsRate_cntnumcrcm"]
    crcme_monitoring_iupr_required_fields = ["DgsRate_cntdenexcrcm", "DgsRate_cntnumexcrcm"]
    egr_monitoring_iupr_required_fields = ["DgsRate_cntdenegrcl", "DgsRate_cntnumegrcl"]
    general_monitoring_iupr_required_fields = ["DgsRate_rateigcnt",  "DgsRate_rategenden","DgsRate_cntnumvvtr","DgsRate_cntnumexvvta","DgsRate_cntnumegrcl" ]
    ro2_monit_required_fields_1 = ['FldOxscr_xosoxcr1f', 'FldOxscr_xosoxcr1s']
    ro2_monit_required_fields_2 = ['FldOxscr_xosoxcr1e', 'FldOxscr_cosocr1d']
    fo2_monit_required_fields = ["FldOxpc_xooxcss", "FldOxpc_xooxcsf", "FldOxpc_tfboxsavsdl"]
    fuel_monit_required_fields_1 = ['FldFue_cofslean', 'FldFue_cofsrich']
    fuel_monit_required_fields_2 = ['FldFue_cofsleangas', 'FldFue_cofsrichgas']
    vvti_monit_required_fields_1 = ["FldVti_xovvtrs", "FldVti_vtdobdsm"]
    vvte_monit_required_fields_1 = ["FldVte_xovvtas_ex", "FldVte_vtdobdsm_ex"]
    crcmi_monitoring_fields = ["FldCrcmi_xocrcmex", "FldCrcmi_xVtiClnExp", "FldInf_VtiOfsAdvance_bank_[0]"]
    crcme_monitoring_fields = ["FldCrcme_xocrcmex_ex", "FldCrcme_xVteClnExp", "FldInf_VteOfsAdvance_bank_[0]"]
    egr_monitoring_fields = ["FldEgrc_xoegrcls", "FldEgrc_xoegrclf", "FldEgrc_odpmegr", "FldEgrc_oegrcdpml"]
    
    throttle_adaption_monitoring_fields = ["AirFbk_AdpFlow00_BU", "AirFbk_AdpFlow01_BU", "AirFbk_AdpFlow02_BU",
                                          "AirFbk_AdpFlow03_BU", "AirFbk_AdpFlow04_BU", "AirFbk_AdpFlow05_BU",
                                          "AirFbk_AdpFlow06_BU"]
    fuel_adaption_monitoring_fields = ["LmdInj_CmpRatio"]
    fuel_adaption_monitoring_onidle_fields =  ["LmdInj_CmpRatio", "CmbReq_EngCtrStatus", "TxcSts_VehSpeed"]

    fuel_adaption_monitoring_3_sec_cons_fields = ["ApfCrk_EngSpeed","PdlAcp_Position", "LmdInj_CmpRatio","ItkAir_AirChgRatio"]


    temp_monit_fields_1 = ["ExhTmp_CatTemperature"]
    temp_monit_fields_2 = ["ApfOil_EngOilTemperature"]
    temp_monit_fields_3 = ["EngClt_Temperature"]
    temp_monit_fields_4 = ["TxcSts_VehSpeed"]
    torque_adap_required_fields_1 = ["SymEcu_Status"]
    torque_adap_required_fields_2 = ["TxcLos_LosTorque"]
    
    load_monit_requirement_fields_1 = ["RteCom_EngineCalculatedLoadDGS", "TrqReq_FltRawTorque", "DgsClv_load"]
    load_monit_requirement_fields_2 = ["ApfCrk_EngSpeed"]

    vehicle_wise_triggers_mapping = {}
    skipped_files = []
    
    try :
        vehicle_wise_triggers_mapping[vehicle_name] = {}
        f = vehicle_mapping[vehicle_name]
#         print(vehicle_name, len(f))
        date_wise_max_cat_temp = []
        FldCat_catgav2_x1 = []
        FldCat_cattdlyb_y1 = []
        DlgCat_soxvcat_y2 = []
        Dates_mapping_cm = []
        date_wise_max_misfire_monit = []
        date_wise_iupr_monit_cata= []
        date_wise_iupr_monit_fo2 = []
        date_wise_iupr_monit_vvti = []
        date_wise_iupr_monit_ro2 = []
        date_wise_iupr_monit_vvte = []
        date_wise_iupr_monit_crcmi = []
        date_wise_iupr_monit_crcme = []
        date_wise_iupr_monit_egr = []
        date_wise_iupr_monit_general = []
        index_mapping_x_fo2 = []
        FldOxpc_tfboxsavsdl_y = []
        Dates_mapping_fo2 = []
        index_mapping_x_ro2 = []
        FldOxscr_cosocr1d_y = []
        Dates_mapping_ro2 = []
        index_mapping_x_fs =[]
        FldFue_cofslean = []
        FldFue_cofsrich = []
        FldFue_cofsleangas = []
        FldFue_cofsrichgas = []
        index_mapping_x_vvti = []
        FldVti_vtdobdsm_y = []
        Dates_mapping_vvti = []
        index_mapping_x_vvte = []
        FldVte_vtdobdsm_ex_y = []
        Dates_mapping_vvte = []
        FldInf_VtiOfsAdvance_bank_min_value_y = []
        FldInf_VtiOfsAdvance_bank_max_value_y = []
        Dates_mapping_crcmi = []
        FldInf_VteOfsAdvance_bank_min_value_y = []
        FldInf_VteOfsAdvance_bank_max_value_y = []
        Dates_mapping_crcme = []
        LmdInj_CmpRatio_max_value = []
        Dates_mapping_fuel_adap = []
        Dates_mapping_egr = []
        FldEgrc_odpmegr = []
        FldEgrc_oegrcdpml = []
        date_wise_max_throttle_adaptation = []
        LmdInj_CmpRatio_max_value = []
        Dates_mapping_fuel_adap = []
        fields_dataframes = {}
        OnIdle_LmdInj_CmpRatio_max_value = []
        OnIdle_Dates_mapping_fuel_adap = []
        fuel_adaptation_3_sec_cons_data = []
        date_wise_max_temp_monit = []
        index_mapping_x_torque_adap = []
        TxcLos_LosTorque_y_torque_adap = []
        Dates_mapping_torque_adap = []
        date_wise_max_load_monit = []
        for cata_file in f :
            try :
                if cata_file.endswith(".dat") :
                    yop=mdfreader.Mdf(cata_file)
                    yop.convert_to_pandas()
                    date_ = time.strftime("%d-%m-%Y", time.gmtime(os.path.getmtime(cata_file)))
                    print(date_)
                    if filter_data(filter_fields, yop) :
                        print("File Skipped")
                        skipped_files.append(cata_file)
                        continue
                    try :
                        date_wise_max_cat_temp = catalyst_temperature_data(yop, date_, cata_temp_required_fields, date_wise_max_cat_temp)
                    except :
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                        
                    try :
                        FldCat_catgav2_x1, FldCat_cattdlyb_y1, DlgCat_soxvcat_y2, Dates_mapping_cm = catalyst_monitoring_data(yop, date_, cata_monit_required_fields, FldCat_catgav2_x1, FldCat_cattdlyb_y1, DlgCat_soxvcat_y2, Dates_mapping_cm)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                        
                    try:
                        date_wise_max_misfire_monit = misfire_monitoring_data(yop , date_, misfire_monit_requirement_fields , date_wise_max_misfire_monit)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_iupr_monit_cata = get_date_wise_monitoring_from_files(yop, date_, catalyst_monitoring_iupr_required_fields, date_wise_iupr_monit_cata)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_iupr_monit_fo2 = get_date_wise_monitoring_from_files(yop, date_, fo2_monitoring_iupr_required_fields, date_wise_iupr_monit_fo2)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_iupr_monit_vvti = get_date_wise_monitoring_from_files(yop, date_, vvti_monitoring_iupr_required_fields, date_wise_iupr_monit_vvti) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_iupr_monit_ro2 = get_date_wise_monitoring_from_files(yop,date_, ro2_monitoring_iupr_required_fields, date_wise_iupr_monit_ro2) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_iupr_monit_vvte = get_date_wise_monitoring_from_files(yop,  date_,vvte_monitoring_iupr_required_fields, date_wise_iupr_monit_vvte) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_iupr_monit_crcmi = get_date_wise_monitoring_from_files(yop, date_, crcmi_monitoring_iupr_required_fields, date_wise_iupr_monit_crcmi) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_iupr_monit_crcme = get_date_wise_monitoring_from_files(yop, date_, crcme_monitoring_iupr_required_fields, date_wise_iupr_monit_crcme) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_iupr_monit_egr = get_date_wise_monitoring_from_files(yop, date_, egr_monitoring_iupr_required_fields, date_wise_iupr_monit_egr) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_iupr_monit_general = gen_iupr_monitoring_data(yop , date_, general_monitoring_iupr_required_fields, date_wise_iupr_monit_general)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        index_mapping_x_fo2, FldOxpc_tfboxsavsdl_y, Dates_mapping_fo2 = fo2_monitoring_data(yop, date_, fo2_monit_required_fields, index_mapping_x_fo2, FldOxpc_tfboxsavsdl_y, Dates_mapping_fo2)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        index_mapping_x_ro2, FldOxscr_cosocr1d_y, Dates_mapping_ro2 = ro2_monitoring_data(yop, date_,  ro2_monit_required_fields_1, ro2_monit_required_fields_2, index_mapping_x_ro2, FldOxscr_cosocr1d_y, Dates_mapping_ro2) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        index_mapping_x_fs, FldFue_cofslean, FldFue_cofsrich, FldFue_cofsleangas, FldFue_cofsrichgas = fuel_system_monitoring(yop, date_, fuel_monit_required_fields_1, fuel_monit_required_fields_2, index_mapping_x_fs, FldFue_cofslean, FldFue_cofsrich, FldFue_cofsleangas, FldFue_cofsrichgas)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        index_mapping_x_vvti, FldVti_vtdobdsm_y, Dates_mapping_vvti = vvti_monitoring_data(yop,date_, vvti_monit_required_fields_1,index_mapping_x_vvti, FldVti_vtdobdsm_y, Dates_mapping_vvti)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        index_mapping_x_vvte, FldVte_vtdobdsm_ex_y, Dates_mapping_vvte = vvte_monitoring_data(yop, date_, vvte_monit_required_fields_1, index_mapping_x_vvte, FldVte_vtdobdsm_ex_y, Dates_mapping_vvte) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        FldInf_VtiOfsAdvance_bank_min_value_y, FldInf_VtiOfsAdvance_bank_max_value_y,  Dates_mapping_crcmi = crcmi_monitoring_data(yop,date_, crcmi_monitoring_fields, FldInf_VtiOfsAdvance_bank_min_value_y, FldInf_VtiOfsAdvance_bank_max_value_y,  Dates_mapping_crcmi) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        FldInf_VteOfsAdvance_bank_min_value_y, FldInf_VteOfsAdvance_bank_max_value_y, Dates_mapping_crcme = crcme_monitoring_data(yop, date_, crcme_monitoring_fields, FldInf_VteOfsAdvance_bank_min_value_y, FldInf_VteOfsAdvance_bank_max_value_y, Dates_mapping_crcme) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        FldEgrc_odpmegr, FldEgrc_oegrcdpml, Dates_mapping_egr = egr_monitoring_data(yop, date_, egr_monitoring_fields, FldEgrc_odpmegr, FldEgrc_oegrcdpml, Dates_mapping_egr)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_max_throttle_adaptation = throttle_adaptation_data_monitoring(yop,date_,  throttle_adaption_monitoring_fields, date_wise_max_throttle_adaptation)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        LmdInj_CmpRatio_max_value,Dates_mapping_fuel_adap = fuel_adaptation_monitoring_data(yop, date_, fuel_adaption_monitoring_fields, LmdInj_CmpRatio_max_value,Dates_mapping_fuel_adap)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        fields_dataframes = fuel_adap_zone_wise_monitoring(yop, date_, all_fuel_zone_fields, fields_dataframes )
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        OnIdle_LmdInj_CmpRatio_max_value,OnIdle_Dates_mapping_fuel_adap = fuel_adaptation_monitoring_data_onidle_3sec(yop, date_, fuel_adaption_monitoring_onidle_fields, OnIdle_LmdInj_CmpRatio_max_value,OnIdle_Dates_mapping_fuel_adap)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        fuel_adaptation_3_sec_cons_data = fuel_adaptation_monitoring_data_3sec_cons(yop, date_, fuel_adaption_monitoring_3_sec_cons_fields, fuel_adaptation_3_sec_cons_data)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_max_temp_monit = temperature_monitoring_data(yop, date_, temp_monit_fields_1, temp_monit_fields_2, temp_monit_fields_3, temp_monit_fields_4, date_wise_max_temp_monit) 
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        index_mapping_x_torque_adap, TxcLos_LosTorque_y_torque_adap, Dates_mapping_torque_adap = torque_adaptation_data(yop, date_, torque_adap_required_fields_1, torque_adap_required_fields_2, index_mapping_x_torque_adap, TxcLos_LosTorque_y_torque_adap, Dates_mapping_torque_adap)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    try :
                        date_wise_max_load_monit = load_monitoring_data(yop , date_, load_monit_requirement_fields_1, load_monit_requirement_fields_2, date_wise_max_load_monit)
                    except:
                        traceback.print_exc()
                        skipped_files.append(cata_file)
                    
            except :
                traceback.print_exc()
                skipped_files.append(cata_file)
                continue

        ### Catalyst Temperature
        df_cata_temp = catalyst_temperature_dataframe(date_wise_max_cat_temp)
        vehicle_wise_triggers_mapping[vehicle_name]["Catalyst Temperature"] = df_cata_temp

        ## Catalyst Monitoring
        df_cata_monitoring = pd.DataFrame({"CM_date" : Dates_mapping_cm,"FldCat_catgav2" : FldCat_catgav2_x1, "FldCat_cattdlyb" :FldCat_cattdlyb_y1 , 
                                         "DlgCat_soxvcat" : DlgCat_soxvcat_y2 })
        vehicle_wise_triggers_mapping[vehicle_name]["Catalyst Monitoring"] = df_cata_monitoring

        ### Misfire Monitoring
        df_misfire_monit = misfire_monitoring_dataframe(date_wise_max_misfire_monit)
        vehicle_wise_triggers_mapping[vehicle_name]["Misfire Monitoring"] = df_misfire_monit


        ### IUPR Catalyst Monitoring
        df_monit_iupr_cata = get_dataframe_from_date_wise_monit(date_wise_iupr_monit_cata, "IUPR_CM_Date")
        df_monit_iupr_cata["IUPR_CM_Ratio"] = df_monit_iupr_cata.apply(lambda x:get_ratio(x["DgsRate_cntnumcat"],x["DgsRate_cntdencat"]), axis=1)
        vehicle_wise_triggers_mapping[vehicle_name]["Catalyst Monitoring IUPR"] = df_monit_iupr_cata


        ### IUPR FO2 Monitoring
        df_monit_iupr_fo2 = get_dataframe_from_date_wise_monit(date_wise_iupr_monit_fo2, "IUPR_FO2_Date")
        df_monit_iupr_fo2["IUPR_FO2_Ratio"] = df_monit_iupr_fo2.apply(lambda x:get_ratio(x["DgsRate_cntnumoxcs"],x["DgsRate_cntdenoxcs"]), axis=1)
        vehicle_wise_triggers_mapping[vehicle_name]["FO2 Monitoring IUPR"] = df_monit_iupr_fo2

        ### IUPR VVTi Monitoring
        df_monit_iupr_vvti = get_dataframe_from_date_wise_monit(date_wise_iupr_monit_vvti, "IUPR_VVTi_Date")
        df_monit_iupr_vvti["IUPR_VVTi_Ratio"] = df_monit_iupr_vvti.apply(lambda x:get_ratio(x["DgsRate_cntnumvvtr"],x["DgsRate_cntdenvvtr"]), axis=1)
        vehicle_wise_triggers_mapping[vehicle_name]["VVTi Monitoring IUPR"] = df_monit_iupr_vvti

        ### IUPR RO2 Monitoring
        df_monit_iupr_ro2 = get_dataframe_from_date_wise_monit(date_wise_iupr_monit_ro2, "IUPR_RO2_Date")
        df_monit_iupr_ro2["IUPR_RO2_Ratio"] = df_monit_iupr_ro2.apply(lambda x:get_ratio(x["DgsRate_cntnumsoxcr"],x["DgsRate_cntdensoxcr"]), axis=1)
        vehicle_wise_triggers_mapping[vehicle_name]["RO2 Monitoring IUPR"] = df_monit_iupr_ro2

        ### IUPR VVTe Monitoring
        df_monit_iupr_vvte = get_dataframe_from_date_wise_monit(date_wise_iupr_monit_vvte, "IUPR_VVTe_Date")
        df_monit_iupr_vvte["IUPR_VVTe_Ratio"] = df_monit_iupr_vvte.apply(lambda x:get_ratio(x["DgsRate_cntnumexvvta"],x["DgsRate_cntdenexvvta"]), axis=1)
        vehicle_wise_triggers_mapping[vehicle_name]["VVTe Monitoring IUPR"] = df_monit_iupr_vvte

        ### IUPR CrCmi Monitoring
        df_monit_iupr_crcmi = get_dataframe_from_date_wise_monit(date_wise_iupr_monit_crcmi, "IUPR_CrCmi_Date")
        df_monit_iupr_crcmi["IUPR_CrCmi_Ratio"] = df_monit_iupr_crcmi.apply(lambda x:get_ratio(x["DgsRate_cntnumcrcm"],x["DgsRate_cntdencrcm"]), axis=1)
        vehicle_wise_triggers_mapping[vehicle_name]["CrCmi Monitoring IUPR"] = df_monit_iupr_crcmi

        ### IUPR CrCme Monitoring 
        df_monit_iupr_crcme = get_dataframe_from_date_wise_monit(date_wise_iupr_monit_crcme, "IUPR_CrCme_Date")
        df_monit_iupr_crcme["IUPR_CrCme_Ratio"] = df_monit_iupr_crcme.apply(lambda x:get_ratio(x["DgsRate_cntnumexcrcm"],x["DgsRate_cntdenexcrcm"]), axis=1)
        vehicle_wise_triggers_mapping[vehicle_name]["Crcme Monitoring IUPR"] = df_monit_iupr_crcme

        ### IUPR EGR Monitoring
        df_monit_iupr_egr = get_dataframe_from_date_wise_monit(date_wise_iupr_monit_egr, "IUPR_EGR_Date")
        df_monit_iupr_egr["IUPR_EGR_Ratio"] = df_monit_iupr_egr.apply(lambda x:get_ratio(x["DgsRate_cntnumegrcl"],x["DgsRate_cntdenegrcl"]), axis=1)
        vehicle_wise_triggers_mapping[vehicle_name]["EGR Monitoring IUPR"] = df_monit_iupr_egr

        ### IUPR General 
        df_monit_iupr_general = gen_iupr_monitoring_dataframe(date_wise_iupr_monit_general)
        vehicle_wise_triggers_mapping[vehicle_name]["General IUPR"] = df_monit_iupr_general

        ## FO2 Monitoring
        df_fo2_monitoring = pd.DataFrame({"FO2_Date" : Dates_mapping_fo2, "FldOxpc_tfboxsavsdl" :FldOxpc_tfboxsavsdl_y })
        vehicle_wise_triggers_mapping[vehicle_name]["FO2 Monitoring"] = df_fo2_monitoring

        ## RO2 Monitoring
        df_ro2_monitoring = pd.DataFrame({"RO2_Date" : Dates_mapping_ro2, "FldOxscr_cosocr1d" :FldOxscr_cosocr1d_y })
        vehicle_wise_triggers_mapping[vehicle_name]["RO2 Monitoring"] = df_ro2_monitoring

        ### Fuel System
        df_fuel_system = pd.DataFrame({"FS_date": index_mapping_x_fs, "FldFue_cofslean" : FldFue_cofslean, 
                                      "FldFue_cofsrich": FldFue_cofsrich, "FldFue_cofsleangas": FldFue_cofsleangas,
                                      "FldFue_cofsrichgas": FldFue_cofsrichgas})
        vehicle_wise_triggers_mapping[vehicle_name]["Fuel System"] = df_fuel_system

        ## VVTi Monitoring
        df_vvti_monitoring = pd.DataFrame({"VVTi_Date" : Dates_mapping_vvti, "FldVti_vtdobdsm" :FldVti_vtdobdsm_y })
        vehicle_wise_triggers_mapping[vehicle_name]["VVTi Monitoring"] = df_vvti_monitoring

        ## VVTe Monitoring
        df_vvte_monitoring = pd.DataFrame({"VVTe_Date" : Dates_mapping_vvte, "FldVte_vtdobdsm_ex" :FldVte_vtdobdsm_ex_y })
        vehicle_wise_triggers_mapping[vehicle_name]["VVTe Monitoring"] = df_vvte_monitoring

        ### CrCmi Monitoring
        df_crcmi_monitoring = pd.DataFrame({"CrCmi_Date" : Dates_mapping_crcmi, "FldInf_VtiOfsAdvance_bank_min_value" :FldInf_VtiOfsAdvance_bank_min_value_y,
                                           "FldInf_VtiOfsAdvance_bank_max_value" : FldInf_VtiOfsAdvance_bank_max_value_y})
        vehicle_wise_triggers_mapping[vehicle_name]["CrCmi Monitoring"] = df_crcmi_monitoring

        ### CrCme Monitoring
        df_crcme_monitoring = pd.DataFrame({"CrCme_Date" : Dates_mapping_crcme, "FldInf_VteOfsAdvance_bank_min_value" :FldInf_VteOfsAdvance_bank_min_value_y,
                                           "FldInf_VteOfsAdvance_bank_max_value" : FldInf_VteOfsAdvance_bank_max_value_y})
        vehicle_wise_triggers_mapping[vehicle_name]["CrCme Monitoring"] = df_crcme_monitoring

        ### EGR Monitoring
        df_egr_monitoring = pd.DataFrame({"EGR_Dates" : Dates_mapping_egr, "FldEgrc_odpmegr" :FldEgrc_odpmegr,
                                       "FldEgrc_oegrcdpml" : FldEgrc_oegrcdpml})
        vehicle_wise_triggers_mapping[vehicle_name]["EGR Monitoring"] = df_egr_monitoring


        ### Throttle Adaptation Monitoring
        df_throttle_adap_monitoring = throttle_adaptiation_clubbed_data(date_wise_max_throttle_adaptation)
        vehicle_wise_triggers_mapping[vehicle_name]["Throttle Adaptation"] = df_throttle_adap_monitoring


        ### Fuel Adaptation Monitoring
        df_fuel_adaptation = fetch_required_df_fuel_adaptation(Dates_mapping_fuel_adap,LmdInj_CmpRatio_max_value )
        vehicle_wise_triggers_mapping[vehicle_name]["Fuel Adaptation"] = df_fuel_adaptation

        ### Fuel Adaptation Zone wise
        df_fuel_adaptation_zone_wise = fuel_adap_zone_wise_monitoring_frames(fields_dataframes)
        vehicle_wise_triggers_mapping[vehicle_name]["Fuel Adaptation Zone Wise"] = df_fuel_adaptation_zone_wise
            
        ## Fuel Adaptation Monitoring OnIdle
        df_fuel_adaptation_onidle = fetch_required_df_fuel_adaptation_onidle(OnIdle_Dates_mapping_fuel_adap,OnIdle_LmdInj_CmpRatio_max_value)
        vehicle_wise_triggers_mapping[vehicle_name]["Fuel Adaptation OnIdle"] = df_fuel_adaptation_onidle

        ### Fuel Adaptation Monitoring 3 sec cons
        df_fuel_adaptation_3_sec = fetch_required_fuel_adaptation_3_sec_data(fuel_adaptation_3_sec_cons_data)
        vehicle_wise_triggers_mapping[vehicle_name]["Fuel Adaptation 3 sec cons"] = df_fuel_adaptation_3_sec

        ### Temperature Monitoring
        df_cata_temp = temperature_monitoring_dataframe(date_wise_max_temp_monit)
        vehicle_wise_triggers_mapping[vehicle_name]["Temperature Monitoring"] = df_cata_temp
            
        ### Torque Adaptation
        df_torque_adap = torque_adaptation_frames(Dates_mapping_torque_adap , TxcLos_LosTorque_y_torque_adap)
        vehicle_wise_triggers_mapping[vehicle_name]["Torque Adaptation"] = df_torque_adap
            
        ### Load Monitoring
        df_load_monit = load_monitoring_dataframe(date_wise_max_load_monit)
        vehicle_wise_triggers_mapping[vehicle_name]["Load Monitoring"] = df_load_monit
            
            
        print(vehicle_wise_triggers_mapping)
        vehicle_wise_triggers_mapping[vehicle_name] = update_dates_and_alarming_values(vehicle_wise_triggers_mapping[vehicle_name], alarming_values) 
            
    except :
        traceback.print_exc()
        
    return vehicle_wise_triggers_mapping, skipped_files



# In[28]:


def get_vehicle_mapping(df_template) :
    vehicle_mapping = {}
    total_count = 0
    for row in df_template.itertuples() :
        vehicle_name = row.Vehicle_Model_Name
        
        for path, subdirs, files in os.walk(row.Input_folder_location + str("\\")):
            for name in files:
#                 print(row.Input_folder_location)
                total_count = total_count + 1
                if row.Input_folder_location in vehicle_mapping :
                    if vehicle_name in vehicle_mapping[row.Input_folder_location]: 
                    
                        vehicle_mapping[row.Input_folder_location][vehicle_name].append((os.path.join(path, name)))
                    else :
                        vehicle_mapping[row.Input_folder_location][vehicle_name] = [(os.path.join(path, name))]
                else :
                    vehicle_mapping[row.Input_folder_location] = {}
                    vehicle_mapping[row.Input_folder_location][vehicle_name] =  [(os.path.join(path, name))]
                
    return vehicle_mapping, total_count



def relocation_from_one_to_another(vehicle_mapping, relocation_loc, output_folder_loc): 
    for file in vehicle_mapping : 
        try :
            file_name = file.split("\\")[-1]
            print(file_name)
            date_ = time.strftime("%d-%m-%Y", time.gmtime(os.path.getmtime(file)))
            current_datetime = datetime.strptime(date_, "%d-%m-%Y")
    #         print(current_datetime.strftime("%B"))
            directory_name = str(current_datetime.strftime("%B")) + str("_") + str(current_datetime.strftime("%Y"))
            path_relocation = os.path.join(relocation_loc, directory_name)
    #         print(not(os.path.exists(path_relocation)))
            if not(os.path.exists(path_relocation)) :
                os.mkdir(path_relocation)
            sub_path_relocation = os.path.join(path_relocation, date_)
            if not os.path.exists(sub_path_relocation) :
                os.mkdir(sub_path_relocation)
    #         print(sub_path_relocation + "\\" + file_name)
            if os.path.exists(sub_path_relocation + "\\" + file_name) :
                continue
            shutil.copy2(file, sub_path_relocation + "\\" + file_name)
        except :
            continue
            

def save_to_excel(df_clubbed, output_file_name, sheet_name_) : 
    try :
        ExcelWorkbook = load_workbook(output_file_name)
        writer = pd.ExcelWriter(output_file_name, engine = 'openpyxl')
        writer.book = ExcelWorkbook
        writer.sheets = dict((ws.title, ws) for ws in ExcelWorkbook.worksheets)
        df_clubbed.to_excel(writer, sheet_name = sheet_name_, index=False)
        writer.save()
        writer.close()
    except :
        traceback.print_exc()
        df_clubbed.to_excel( output_file_name, sheet_name = sheet_name_, index=False)


# In[29]:


app = Tk()

input_text = Text(app, height=2, width=30)
input_text.grid(column=1, row=0, sticky='nsew')


def get_input_folder():
    # file type
    filetypes = (
        ('text files', '*.xlsx'),
        ('All files', '*.*')
    )
    # show the open file dialog
    f = fd.askopenfile(filetypes=filetypes)
    # read the text file and show its content on the Text
    input_text.delete('1.0', END)
    input_text.insert('1.0', f.name)


input_label = Button(app, text="Select Input Template File", width=30,command = get_input_folder)
input_label.grid(row=0, column=0)




## Buttons

def start_program() :
    input_template_file = input_text.get(1.0, "end-1c")
    
#     print(input_template_file)
    skipped_files = []
    try :
        df_template = pd.read_excel(input_template_file)
        print(df_template.columns.to_list())
        test_list = df_template.columns.to_list()
        sub_list = ["S_No","Vehicle_Model_Name","Input_folder_location","Thresold_values","Output_folder","Location_for_relocation_of_input_data","Remark"]
        flag = 0
        if(all(x in test_list for x in sub_list)):
            flag = 1
        if  flag == 0 :
            raise Exception("Input template is not correct please check")
        
        df_template = df_template[df_template['Vehicle_Model_Name'].notna()]
        
        vehicle_mapping, total_file_count = get_vehicle_mapping(df_template)
#         print(len(vehicle_mapping))
        count = 0
        for key1 in vehicle_mapping :
#             print(key1)
#             print(len(vehicle_mapping[key1]))
            for sub_key in vehicle_mapping[key1] : 
                
                df_selected = df_template.loc[(df_template["Input_folder_location"] == key1 ) & (df_template["Vehicle_Model_Name"] == sub_key)]
#                 print(df_selected)
                output_folder_loc = df_selected["Output_folder"].to_list()[0]
                alarming_value_loc = df_selected["Thresold_values"].to_list()[0]
                
                ## Relocation from one folder to another
                if len(df_selected["Location_for_relocation_of_input_data"].to_list()) > 0 and not df_selected["Location_for_relocation_of_input_data"].to_list()[0].isspace():
                    relocation_loc = df_selected["Location_for_relocation_of_input_data"].to_list()[0]
                    
                    relocation_from_one_to_another(vehicle_mapping[key1][sub_key], relocation_loc, output_folder_loc)
                else : 
                    continue
                
                try :

                    alarming_values = pd.read_excel(alarming_value_loc)
                    alarming_values.set_index("Monitoring Name", inplace=True)

                    vehicle_wise_triggers_mapping, skipped_files = run_final_algorithm(vehicle_mapping[key1], sub_key, alarming_values)
#                     print("***********",vehicle_wise_triggers_mapping)
                    if len(vehicle_wise_triggers_mapping) > 0 :
                        for keys in vehicle_wise_triggers_mapping :
                            try :
                                df_clubbed = pd.read_excel(output_folder_loc + "\\" + keys+ ".xlsx", sheet_name="Data")
                                
                                if len(df_clubbed)> 0:
                                    df_clubbed = fetch_all_dataframes(df_clubbed, vehicle_wise_triggers_mapping) 
                                else: 
                                    df_clubbed = create_df_clubbed_data(vehicle_wise_triggers_mapping[keys], keys)
                            except Exception as e :
                                print("exception occur", e)
                                traceback.print_exc()
                                df_clubbed = create_df_clubbed_data(vehicle_wise_triggers_mapping[keys], keys)

                            df_clubbed = merge_with_final_dataframe(df_clubbed)
                            print("*******************************____________________________",df_clubbed)
                            save_to_excel(df_clubbed, output_folder_loc +"\\" + keys+ ".xlsx", "Data")
    #                             df_clubbed.to_excel( output_folder_loc +"\\" + keys+ ".xlsx", index=False)
                    
                    df_skipped_files = pd.DataFrame({"Skipped Files" : skipped_files})
                    df_skipped_files.drop_duplicates(inplace=True)
                    print("_____________________ skipped")
                    try :
                        df_skipped = pd.read_csv(output_folder_loc + "\\" + sub_key+ "_skipped_files.csv")
                        df_skipped_files.to_csv(output_folder_loc +"\\" + sub_key+ "_skipped_files.csv", index=False, mode='a', header=False)
                    except :
                        df_skipped_files.to_csv(output_folder_loc + "\\" + sub_key+ "_skipped_files.csv", index=False)
                except:
                    traceback.print_exc()
                    df_skipped_files = pd.DataFrame({"Skipped Files" : skipped_files})
                    df_skipped_files.drop_duplicates(inplace=True)
                    print("_____________________ skipped")
                    try :
                        df_skipped = pd.read_csv(output_folder_loc + "\\" + sub_key+ "_skipped_files.csv")
                        df_skipped_files.to_csv(output_folder_loc +"\\" + sub_key+ "_skipped_files.csv", index=False, mode='a', header=False)
                    except :
                        traceback.print_exc()
                        df_skipped_files.to_csv(output_folder_loc + "\\" + sub_key+ "_skipped_files.csv", index=False)
        print("done")
            # final_df
        messagebox.showinfo("Status", "Executed Successfully." )
    except Exception as e:
        print(e)
        traceback.print_exc()
        messagebox.showinfo("Status", "Input template is not correct please check")
        
    



start_btn = Button(app, text="Run Model", width=15,command = start_program)
start_btn.grid(row=4, column=0, pady=20)



def update_progress_label():
    return f"Current Progress: {pb['value']}%"


def progress(val, total_val):
    if pb['value'] < 100:
        pb['value'] = (val/total_val)*100
        value_label['text'] = update_progress_label()


app.title("Fleet Data Compilation")
app.geometry('700x400')


# 


# Code to add widgets will go here...
app.mainloop()


# In[ ]:




