#!/usr/bin/python
#########################################################################
### PYTHON PROGRAMMING SKILLS ASSESMENT TEST WITH Dynatron for Data Engineer.
### https://github.com/dtdataplatform/data-challenges/blob/main/data-engineer/README.md
###  The script design version of aper tips suggested in the assigmnet
#########################################################################
### REQUIREMENT UNKNOWS AND QUESTIONS :
###    1) There is no grouping criteria grain specified in the assignemt.
###    2) The assigment does not describe the relational structe of the SQL Lite (TABLES)
###      where the hierarchical (XML) data is to be loaded to.
###    3) In TIPS section, whats is the purpose of keeping the same version of the in different shape and forms?
#########################################################################
###  REVISIONS:
###     2024-05-11 sergey doubov  initial
#########################################################################
import os
import glob
import xml.etree.ElementTree as ET
import pandas as pd
from typing import List, Dict
import datetime

class RepairOrder:
    def __init__(self, order_id, date_time, status, cost, technician, repair_parts,agg_window_date):
        self.order_id = order_id
        self.date_time = date_time
        self.status = status
        self.cost = cost
        self.technician = technician
        self.repair_parts = repair_parts
        self.agg_window_date = agg_window_date

    def __str__(self):
        parts_str = ', '.join([f"{part[1]} {part[0]}" for part in self.repair_parts])

        return "AGGDATE: {}\tORDER ID: {}\tDATE TIME: {}\tSTATUS:{}\tCOST: {}\tTECHNICIAN: {}\tREPAIR PARTS: {}".format(self.agg_window_date,self.order_id,self.date_time,self.status,self.cost,self.technician,parts_str)


def read_files_from_dir(directory: str) -> List[str]:
    files = glob.glob(os.path.join(directory, '*.xml'))
    contents = []
    for file in files:
        with open(file, 'r') as f:
            contents.append(f.read())
    return contents

def parse_xml(files: List[str]) -> pd.DataFrame:
    try:
        data = {
            'order_id': [],
            'date_time': [],
            'status': [],
            'cost': [],
            'technician': [],
            'repair_parts': []
        }

        for file_content in files:
            root = ET.fromstring(file_content)
            for event in root.findall('.'):
                order_id = event.find('order_id').text
                date_time = pd.to_datetime(event.find('date_time').text)
                status = event.find('status').text
                cost = float(event.find('cost').text)
                technician = event.find('repair_details/technician').text
                repair_parts = [(part.attrib['name'], int(part.attrib['quantity'])) for part in event.findall('repair_details/repair_parts/part')]
                data['order_id'].append(order_id)
                data['date_time'].append(date_time)
                data['status'].append(status)
                data['cost'].append(cost)
                data['technician'].append(technician)
                data['repair_parts'].append(repair_parts)
        df = pd.DataFrame(data)

        return df
    except Exception as e:
        print(f"Error parsing XML files: {e} " )
        return pd.DataFrame()


def window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame]:
    try:
        windowed_data = {}
        ## for name, group in data.groupby(pd.Grouper(key='date_time', freq='ME')):
        for name, group in data.groupby(pd.Grouper(key='date_time', freq=window)):
            windowed_data[name.strftime('%Y-%m-%d')] = group
        return windowed_data

    except Exception as e:
        print(f"Error windowing data: {e}")
        return {}


def process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RepairOrder]:
    ro_list = []
    for agg_day, df in data.items():
        for index, row in df.iterrows():
            ro = RepairOrder(
                order_id=row['order_id'],
                date_time=row['date_time'],
                status=row['status'],
                cost=row['cost'],
                technician=row['technician'],
                repair_parts=row['repair_parts'],
                agg_window_date=agg_day
            )
            ro_list.append(ro)
    return ro_list

# Main pipeline
def main(directory: str, window: str):

    ct = datetime.datetime.now()
    print("current time:-", ct)

    # Step 1: Read files
    files = read_files_from_dir(directory)
    ##print (files)

    # Step 2: Parse XML
    data = parse_xml(files)

    # Step 3: Window by DateTime
    windowed_data = window_by_datetime(data, window)

    # Step 4: Process to RO
    ro_list = process_to_RO(windowed_data)

    # Step 5: Write to SQLite database (not implemented in this script)

    print('=================================')
    for ro1 in ro_list:
        print(ro1)


if __name__ == "__main__":
    directory = r'C:\Users\flant\Downloads\test2drop'
    window = "D"
    main(directory, window)



