import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt" # stores all the logs
target_file = "transformed_data.csv" # store the final output which can be loaded into a database

def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process): 
    log_progress(f"Extracting data from {file_to_process}")
    try:
        dataframe = pd.read_json(file_to_process, lines=True)
        return dataframe
    except ValueError as e:
        log_progress(f"Error reading {file_to_process}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True)
    return dataframe

def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    # to process all files:

    # process all .csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index = True)


    # process all .json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index = True)

    
    # process all .xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index = True)
    
    return extracted_data

# to process the data
def transform(data):

    data["height"] = round(data.height * 0.0254, 2)
    data["weight"] = round(data.weight * 0.453592, 2)

    return data

# to load the data into a single csv file
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

# optional but quite helpful, logging function
def log_progress(message):
    timestamp_format = "%Y-%h-%d->%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open (log_file,"a") as file:
        file.write(f"{timestamp}, {message}\n")

# testing
# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 