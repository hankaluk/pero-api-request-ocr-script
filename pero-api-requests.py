# Based on: https://app.swaggerhub.com/apis-docs/LachubCz/PERO-API/1.0.1#/ by Michal Hradiš.
# For running the script you need to get the api key.
# First enter the link with data.
# Second enter the resulting format (alto, txt, page)

import os
import requests
import json
import time

SERVER_URL = os.environ.get('SERVER_URL')
API_KEY = os.environ.get('API_KEY')
headers1 = {"api-key": API_KEY}  # header for get_engines
headers2 = {"api-key": API_KEY, "Content-Type": "application/json"}  # header for data processing

# data path:
data_path = "OCRs/ocr_data.json"
# format of the result
format1 = "txt"  # or alto or page(returns page xml)

request_names = []
# json for data processing
with open(data_path, "r", encoding="utf-8") as file:
    data = json.load(file)
if data["images"] is not None:
    for key in data["images"].keys():
        request_names.append(key)


# returns list of available engines - not part of processing of an image
def get_engines():
    r = requests.get(f"{SERVER_URL}/get_engines", headers=headers1)
    return r.text


# print(get_engines())


# sends data for processing
def process_request():
    r = requests.post(f"{SERVER_URL}/post_processing_request", json=data, headers=headers2)
    response = json.loads(r.text)
    print(response.get('status'))
    if response.get('status') == 'success':
        response_request_id = response.get('request_id')
        print(response_request_id)
        return response_request_id
    else:
        print("Response was not successful.")


def request_status(request_id, request_name1):
    processed = False
    while not processed:
        r = requests.get(f"{SERVER_URL}/request_status/{request_id}", headers=headers2)
        response = json.loads(r.text)
        if response["request_status"][f"{request_name1}"]["state"] == "PROCESSED":
            processed = True
        else:
            print(f'Request {request_name1} is {response["request_status"][f"{request_name1}"]["state"]}')
        time.sleep(10)
    return True


def get_results(request_id, request_name2, format2):
    url = f"{SERVER_URL}/download_results/{request_id}/{request_name2}/{format2}"
    r = requests.get(url, headers=headers2)
    path = f"OCRs/{request_name2}"
    if format2 == "alto" or format2 == "txt" or format2 == "page":
        with open(path, "w", encoding="utf-8") as file2:
            file2.write(r.text)
        print(f"Request {request_name2} finished.")
    else:
        print("You entered the wrong format.")


def main():
    # Choose one of the options, comment the other one:

    # OPTION 1 (if you already have a request id):
    # result_request_id = ""

    # OPTION 2 (a fresh new request):
    result_request_id = process_request()
    for request_name in request_names:
        if request_status(result_request_id, request_name):
            get_results(result_request_id, request_name, format1)


main()
