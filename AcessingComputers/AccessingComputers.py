

from doctest import REPORT_UDIFF
from elasticsearch import Elasticsearch
import tkinter as tk
from tkinter import messagebox
from tkinter import filedialog
import csv
from PIL import Image, ImageTk
import requests
from io import BytesIO
import calendar
import time
from datetime import datetime, timedelta
import pandas as pd 
import numpy as np 
import threading

# Global variables
global server
computer = ""
time_interval = 0  # in seconds
gui_shown = False
selected_computers = []
threads = []
script_running = True  # Flag to indicate if the script is running

#########################################################
#          CONVERT ALL DATA TO CSV FOR OUTPUT           #
#########################################################

def flatten_dict(dictionary, parent_key='', sep='.'):
    items = []
    for k, v in dictionary.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def hits_to_csv(hits, csv_file):
    # Extract the field names from the hits
    fieldnames = set()
    flattened_hits = []
    for hit in hits:
        if isinstance(hit, dict):
            flattened_hit = flatten_dict(hit['_source'])
            fieldnames.update(flattened_hit.keys())
            flattened_hits.append(flattened_hit)

    # Write hits to the CSV file
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=list(fieldnames))
        writer.writeheader()
        writer.writerows(flattened_hits)

#########################################################
#                    ELASTIC SEARCH                     #
#########################################################

def extract_elastic_database(server_name, source_ip, selected, duration):
    # Connect to the Elasticsearch server
    es = Elasticsearch(server_name)
    larger_list = []
    
    # Verify the connection
    if not es.ping():
        ConnectionError("failed to connect to elasticsearch")

    # Set up the search query with filter conditions
    current_time = datetime.now().strftime("%Y-%m-%d___%H-%M-%S")

    for reporting_computer in selected: 
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"reporting_computer": reporting_computer}},
                        {"term": {"source_ip": source_ip}}
                    ]
                }
            }
        }

        # Execute the search query using the "scroll" API to retrieve all matching documents
        scroll_size = 1000
        response = es.search(index="*", body=query, size=scroll_size, scroll="1m")
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']

        while len(hits) > 0:
            # Process the hits
            larger_list.extend(hits)
        
            # Scroll to the next batch of results
            response = es.scroll(scroll_id=scroll_id, scroll="1m")
            hits = response['hits']['hits']

        # Clear the scroll
        es.clear_scroll(scroll_id=scroll_id)

    # Get the current time and date
    current_time = datetime.now().strftime("%Y-%m-%d___%H-%M-%S")
    csv_file = f"result_{current_time}_{str(selected)}.csv"

    
    # Write hits to the CSV file
    hits_to_csv(larger_list, csv_file)

    #return csv_file

def generate_calendar(year, month):
    _, num_days = calendar.monthrange(year, month)
    cal = ""
    for day in range(1, num_days + 1):
        cal += f"{year}-{month:02d}-{day:02d}\n"
    return cal

def write_calendar_to_file(calendar_data, calendar_file):
    with open(calendar_file, 'w') as file:
        file.write(calendar_data)

def get_timestamp():
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d___%H-%M-%S")
    return timestamp

def update_calendar(calendar_file, timestamp, output_file):
    # Read existing calendar data or create new if file doesn't exist
    try:
        with open(calendar_file, 'r') as file:
            calendar_data = file.read()
    except FileNotFoundError:
        calendar_data = ""

    # Append new entry to the existing calendar data
    updated_calendar_data = f"{calendar_data}\n{timestamp}: {output_file}"

    # Write updated calendar data to the calendar file
    with open(calendar_file, 'w') as file:
        file.write(updated_calendar_data)

def get_reporting_computers(server_name):
    # Connect to the Elasticsearch server
    es = Elasticsearch(server_name)

    # Verify the connection
    if not es.ping():
        ConnectionError("failed to connect to elasticsearch")

    # Set up the search query to get unique reporting computers
    query = {
        "size": 0,
        "aggs": {
            "unique_computers": {
                "terms": {
                    "field": "reporting_computer.keyword",
                    "size": 10000
                }
            }
        }
    }

    # Execute the search query
    search_results = es.search(allow_partial_search_results=True, body=query)

    reporting_computers = [bucket['key'] for bucket in search_results['aggregations']['unique_computers']['buckets']]
    return reporting_computers

#########################################################
#                         RUN                           #
#########################################################

# Function to end all running threads
def end_threads():
    global script_running
    script_running = False
    for t in threads:
        t.join()

    end_button_window.destroy()  # Close the "End Threads" GUI

def run_script(server, interval, duration, selected_computers):
    current_time = datetime.now()
    end_time = current_time + timedelta(days=float(duration))

    print(script_running)

    # infinitely loop through the database in the interval the user gives
    while current_time <= end_time and script_running: 
        print("IN HERE")
        # Generate calendar
        now = datetime.now()
        year = now.year
        month = now.month

        # Create output file with timestamp
        timestamp = get_timestamp()

        output_file = extract_elastic_database(server, server, selected_computers, duration)

        time.sleep(float(interval) * 60 * 60)
        current_time = datetime.now()

#########################################################
#          CONVERT GUI INPUT TO SCHEDULE.CSV            #
#########################################################

def convert_to_csv(output_file):
    data = []
    for row in data_entries:
        data.append([entry.get() for entry in row])

    try:
        with open(output_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(column_labels)
            writer.writerows(data)

        result_label.config(text="Conversion successful!", fg="green")
    except Exception as e:
        result_label.config(text=f"Error: {str(e)}", fg="red")
   
    window.destroy()

#########################################################
#                       MAIN GUI                        #
#########################################################

def add_row(server_name, data_frame, data_entries):
    row_entries = []
    for i in range(3):
        if i == 0:  # First field (Computer Name)
            reporting_computers = get_reporting_computers(server_name)
            var = tk.StringVar(window)
            var.set(reporting_computers[0])
            entry = tk.OptionMenu(data_frame, var, *reporting_computers)
            entry.grid(row=len(data_entries) + 1, column=i+1, padx=5, pady=5)
            row_entries.append(var)
        else: 
            entry = tk.Entry(data_frame, width=15)
            entry.grid(row=len(data_entries) + 1, column=i+1, padx=5, pady=5)
            row_entries.append(entry)
    data_entries.append(row_entries)

def next_button_clicked(): 
    window.user_clicked.set(True)

# Create the main window
window = tk.Tk()
window.title("CSV Converter")

window.user_clicked = tk.BooleanVar(window)
# Download and load the image from the URL
image_url = "https://cdn.shopify.com/s/files/1/1852/7897/products/Screen_Shot_2017-08-17_at_6.43.41_PM_2048x.png?v=1503010141"
response = requests.get(image_url)
image_data = response.content
image = Image.open(BytesIO(image_data))
image = image.resize((600, 600))  # Resize the image to 6x6 inches
background_image = ImageTk.PhotoImage(image)

# Create a background label to display the image
background_label = tk.Label(window, image=background_image)
background_label.place(x=0, y=0, relwidth=1, relheight=1)

server_label = tk.Label(window, text="Server:", font=('Arial', 36))
server_label.grid(padx=5, pady=5)

server_entry = tk.Entry(window, width=30)
server_entry.grid(padx=5, pady=5)

window.geometry("400x400")  # Set window size in pixels

next_button = tk.Button(window, text="Next", command=next_button_clicked, width = 20, height = 2) 
next_button.grid(padx=5, pady=5)

window.wait_variable(window.user_clicked)
server_name = server_entry.get()
server_label.grid_forget() 
server_entry.grid_forget() 
next_button.grid_forget() 
# Column labels
column_labels = ["Computer Name", "Interval (Hours)", "Duration (Days)"]

# Create and place the header labels
for i, label in enumerate(column_labels):
    header_label = tk.Label(window, text=label)
    header_label.grid(row=0, column=i, padx=5, pady=5)

# Create and place the data frame
data_frame = tk.Frame(window)
data_frame.grid(row=1, column=0, columnspan=5, padx=5, pady=5)

# Create and place the data entry fields
data_entries = []
for _ in range(1):
    row_entries = []
    for i in range(3):
         if i == 0:  # First field (Computer Name)
            reporting_computers = get_reporting_computers(server_name)
            var = tk.StringVar(window)
            var.set(reporting_computers[0])
            entry = tk.OptionMenu(data_frame, var, *reporting_computers)
            entry.grid(row=len(data_entries) + 1, column=i+1, padx=5, pady=5)
            row_entries.append(var)
         else:
            entry = tk.Entry(data_frame, width=15)
            entry.grid(row=len(data_entries) + 1, column=i+1, padx=5, pady=5)
            row_entries.append(entry)
    data_entries.append(row_entries)

# Create and place the "Add Row" button
add_row_button = tk.Button(window, text="Add Row", command=lambda: add_row(server_name, data_frame, data_entries))
add_row_button.grid(row=2, column=0, padx=5, pady=5)

# Create and place the submit button
convert_button = tk.Button(window, text="Submit", command=lambda: convert_to_csv("schedule.csv"))
convert_button.grid(row=4, column=0, columnspan=3, padx=5, pady=5)

# Create and place the result label
result_label = tk.Label(window, text="")
result_label.grid(row=5, column=0, columnspan=3, padx=5, pady=5)

# Start the main event loop
window.mainloop()

#########################################################
#              CONVERT SCHEDULE TO ARRAY                #
#########################################################

def csv_to_array(csv_file):
    array = []
    with open(csv_file, 'r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            array.append(row)
    return array

arr = csv_to_array("schedule.csv")

#########################################################
#                       THREADS                         #
#########################################################

def worker(i):
    run_script(server_name, arr[i][1], arr[i][2], [arr[i][0]])
    
#Create multiple threads
for i in range(1, len(arr)):
    t = threading.Thread(target=worker, args=(i,))
    threads.append(t)

#Start the threads
for t in threads:
    t.start()

#########################################################
#                       END GUI                         #
#########################################################

# Create the end button window
end_button_window = tk.Tk()
end_button_window.title("End Threads")
end_button_window.geometry("200x100")

# Button to end the threads
end_button = tk.Button(end_button_window, text="End Threads", command=end_threads)
end_button.pack(pady=10)

# Start the end button window event loop
end_button_window.mainloop()

#Wait for all threads to complete
for t in threads:
    t.join()
