#!/usr/bin/env python
# coding: utf-8

# In[18]:


# !jupyter nbconvert --to script Flag_For_Dates_Prod_140425.ipynb


# In[2]:


import pandas as pd
import numpy as np
from datetime import datetime
from itertools import count
from tqdm import tqdm
import Levenshtein
import re
import ast
import networkx as nx
from sqlalchemy import create_engine
import urllib
import sys
tqdm.pandas()
a=pd.DataFrame()
from itertools import combinations
import gc


# In[ ]:


def SAME_DATES_FLAG():
    def getDataFromDatabase():
        user = "semen"
        pwd = "Gpohealth!#!"
        password = urllib.parse.quote_plus(pwd)
        schema = "anomaly"
        ip = "prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com"
        engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, ip, schema))
        query = "SELECT * FROM anomaly.duplicate_ap_invoice" #Source data
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    combined = getDataFromDatabase()
    combined = combined.reset_index()
    combined.shape
    # combined = combined[combined['index'].isin(range(6217, 6219))]
    def extract_dates(text):
        if pd.isna(text):
            return None
        text = text.lower()
        # print(text)
        if "e+" in text or "e-" in text:
            return None
        # month_patterns=r"(?:^|[^a-zA-Z])((?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(\d{4})?(?=\b|[^a-zA-Z]|\d{4}))"
        month_patterns=r"((?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|sept(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"\
        r"(\d{4})?(?=\b|[^a-zA-Z]|\d{4}))"
        date_patterns = [
            r"(?<!\d)[^\d]?\(?\{?\[?"
            r"(\d{1,2}[-_/\\.\\]\d{1,2}[-_/\\.\\]\d{2,4}|\d{2,4}[-_/\\.\\]\d{1,2}[-_/\\.\\]\d{1,2})"
            r"\)?\}?\]?[^0-9]?(?!\d)",

            r"(?<!\d)[^\d]?\(?\{?\[?"
            r"(\d{1,2}[-_/\\.\\]\d{2,4}|\d{2,4}[-_/\\.\\]\d{1,2})"
            r"\)?\}?\]?[^0-9]?(?!\d)",
        ]
        for pattern in date_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # print(match)
                date_formats = [
                    "%d-%m-%Y", "%Y-%m-%d","%Y-%d-%m","%m-%d-%Y",
                    "%m-%Y", "%Y-%m", "%m-%d-%y","%d-%m-%y", "%y-%m-%d","%y-%d-%m", "%m-%y", "%y-%m"
                ]
                # match = fixedmatch.replace("(", "").replace(")", "").replace("/","-").replace(".","-")
                match = re.sub(r"[\(\)\[\]\{\}\\\/\.]", "-", match)
                # print(match)
                parsed_date = None
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(match, fmt)
                        # print(parsed_date)
                        return parsed_date
                    except:
                        continue
        found_months = re.search(month_patterns, text, re.IGNORECASE)
        # print(found_months)
        month_mapping = {"jan": 1, "january": 1,
                                     "feb":2, "february":2,
                                     "mar":3, "march":3,
                                     "apr":4, "april":4,
                                     "may":5,
                                     "jun":6, "june":6,
                                     "jul":7,"july":7,
                                     "aug":8, "august":8,
                                     "sep":9,"september":9,
                                     "oct":10,"october":10,
                                     "nov":11,"november":11,
                                     "dec":12, "december":12}



        if found_months:
            month_key = re.sub(r"[^a-zA-Z]", "", found_months.group(0))[:3].lower()
            if month_key in month_mapping:
                return month_mapping[month_key]

        found_number_month = re.search(r"(?<![\d])(\d{2})(\d{4})?(?!\d)", text)
        if found_number_month:

            if len(found_number_month.group(0))>6:
                return 0
            else:
                year = int(found_number_month.group(2)) if found_number_month.group(2) else 0
                if year is not None and (year <2020 or year>2025):
                    return None
                month_number = int(found_number_month.group(1))
                if 1<=month_number<=12:
                    return month_number

            return None

    # def process_dates(combined):
    #     grouped = combined.groupby(["Matched_Record_Number", 'Reason_Grouped', 'file_name'])
    #     print(len(grouped))
    #     combined['date_in_invoice_number_flag'] = None

    #     for matching_number, group in grouped:
    #         if len(group) != 2:
    #             continue
    #         group = group.copy()
    #         group['Extracted_Month'] = group['Supplier_Invoice_Number'].apply(extract_dates)
    #         if group['Extracted_Month'].notna().sum() ==2:
    #             # months = group['Extracted_Month'].tolist()
    #             months = [x.month if isinstance(x, datetime) else x for x in group['Extracted_Month'].tolist()]
    #             if months[0] == months[1]:
    #                 combined.loc[group.index, 'date_in_invoice_number_flag'] =1
    #             else:
    #                 combined.loc[group.index, 'date_in_invoice_number_flag'] =-1
    #     return combined
    # combined = process_dates(combined)
    # combined['date_in_invoice_number_flag'].value_counts()
    combined['date_in_invoice_number_flag'] = None
    def process_dates(combined):
        grouped = combined.groupby(["Matched_Record_Number", 'Reason_Grouped', 'file_name'])
        print(len(grouped))
        combined['date_in_invoice_number_flag'] = None

        for matching_number, group in grouped:
            if len(group) != 2:
                if group['Supplier_Invoice_Number'].nunique()==2:
                    group1 = group.copy()
                    group1 = group1.drop_duplicates(subset=['Supplier_Invoice_Number'])
                    group1['Extracted_Month'] = group1['Supplier_Invoice_Number'].apply(extract_dates)
                    if group1['Extracted_Month'].notna().sum() ==2:
                        # months = group['Extracted_Month'].tolist()
                        months = [x.month if isinstance(x, datetime) else x for x in group1['Extracted_Month'].tolist()]
                        if months[0] == months[1]:
                            combined.loc[group.index, 'date_in_invoice_number_flag'] =1
                        else:
                            combined.loc[group.index, 'date_in_invoice_number_flag'] =-1
                else:
                    continue
            group = group.copy()
            group['Extracted_Month'] = group['Supplier_Invoice_Number'].apply(extract_dates)
            if group['Extracted_Month'].notna().sum() ==2:
                # months = group['Extracted_Month'].tolist()
                months = [x.month if isinstance(x, datetime) else x for x in group['Extracted_Month'].tolist()]
                if months[0] == months[1]:
                    combined.loc[group.index, 'date_in_invoice_number_flag'] =1
                else:
                    combined.loc[group.index, 'date_in_invoice_number_flag'] =-1
        return combined
    combined = process_dates(combined)
    #combined['date_in_invoice_number_flag'].value_counts()
    #combined['date_in_invoice_number_flag'] = combined['date_in_invoice_number_flag'].replace(1,None)
    #combined['date_in_invoice_number_flag'] = combined['date_in_invoice_number_flag'].replace(-1,0)
    combined['date_in_invoice_number_flag'] = combined['date_in_invoice_number_flag'].fillna(0)
    print(combined['date_in_invoice_number_flag'].value_counts())
    combined.sort_values(by="index", ascending =True, inplace=True)
    combined.drop(columns=['index'], inplace=True)
    def uploadOutput(output):
        user = "semen"
        pwd = "Gpohealth!#!"
        password = urllib.parse.quote_plus(pwd)
        schema = "anomaly"
        ip = "prod-db.c969yoyq9cyy.us-east-1.rds.amazonaws.com"
        engine = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user, password, ip, schema))
        table_name = 'duplicate_ap_invoice' #output
        output.to_sql(table_name, con = engine, if_exists='replace', index=False, chunksize=5000)

    uploadOutput(combined)


# In[ ]:





# In[7]:


def main():

    #if len(sys.argv) != 4:
    #    print ("Incorrect parm. Program needs ONLY 3 parameters to execute")
    #    sys.exit(1)

    #user_name = sys.argv[1]
    #execution_id = sys.argv[2]
    #file_name = sys.argv[1]
    SAME_DATES_FLAG()


    return


# In[8]:


if __name__ == "__main__":

    main()
    exit()


# In[ ]:

