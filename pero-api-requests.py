# Based on: https://app.swaggerhub.com/apis-docs/LachubCz/PERO-API/1.0.1#/ by Michal Hradiš.
# For running the script you need to get the api key.
# First enter the link with data.
# Second enter the resulting format (alto, txt, page)

import os
import requests
import json
import time
import logging


SERVER_URL = os.environ.get('SERVER_URL')
API_KEY = os.environ.get('API_KEY')
headers1 = {"api-key": API_KEY}  # header for get_engines
headers2 = {"api-key": API_KEY, "Content-Type": "application/json"}  # header for data processing

# logs settings
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
logger = logging.getLogger("get_results")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('/app/processing_logs.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger_main = logging.getLogger(__name__)
logger_main.setLevel(logging.INFO)
file_handler_main = logging.FileHandler('/app/log_file.log')
file_handler_main.setFormatter(formatter)
logger_main.addHandler(file_handler_main)


# returns list of available engines - not part of the processing of images
def get_engines():
    r = requests.get(f"{SERVER_URL}/get_engines", headers=headers1)
    return r.text


# print(get_engines())


# function for creating processing request
def process_request(data):
    r = requests.post(f"{SERVER_URL}/post_processing_request", json=data, headers=headers2)
    response = r.json()
    if r.status_code == 200:
        response_request_id = response.get('request_id')
        logger_main.info(f"Request ID: {response_request_id}")
        return response_request_id
    else:
        message = response.get('message')
        logger_main.error(f"Processing request error: {r.status_code} {message}")
        return ""


# getting the request status
def request_status(request_id):
    processed = False
    while not processed:
        r = requests.get(f"{SERVER_URL}/request_status/{request_id}", headers=headers2)
        response = r.json().get('request_status').values()
        for value in response:
            if 'PROCESSED' in value.get('state'):
                processed = True
            else:
                # time.sleep(3600)  # 1 hour wait - needs to be tested
                time.sleep(1200)    # 20 minute wait for 100 images?
    return processed


def get_results(request_id, file_name, result_format):
    unprocessed_file = ""
    url = f"{SERVER_URL}/download_results/{request_id}/{file_name}/{result_format}"
    file_path = f"/app/Results/{result_format}/{file_name}"
    if result_format == "alto" or result_format == "txt" or result_format == "page":
        r = requests.get(url, headers=headers2)
        if r.status_code == 200:
            with open(file_path, "w", encoding="utf-8") as result_file:
                result_file.write(r.text)
        else:
            unprocessed_file = file_name
            logger.info(f"File {unprocessed_file} was not processed: {r.status_code} {r.json().get('message')}")
    else:
        logger_main.error("Wrong format entered.")
        exit()
    return unprocessed_file


def main():
    logger_main.info("Script started.")
    request_names = []
    status = False

    with open(data_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    if bool(data):
        for key in data.get('images').keys():
            request_names.append(key)
        logger_main.info("Data load: Successful.")
    else:
        logger_main.error("Data load: Error loading JSON file.")
        exit()

    start = time.time()

    # Choose one of the options, comment the other one:
    # OPTION 1 (new request):
    result_request_id = process_request(data)

    # OPTION 2 (if you already have a request id):
    # result_request_id = "34f6ae37-6eb5-42e8-864a-ef56a0a07e6d"

    if result_request_id != "":
        status = request_status(result_request_id)

    control_group = []
    if status:
        for request_name in request_names:
            control_group_item = get_results(result_request_id, request_name, "txt")
            if control_group_item:
                control_group.append(control_group_item)
            else:
                get_results(result_request_id, request_name, "alto")

    while control_group:    # while control_group list has any items
        for member in control_group:
            control_member = get_results(result_request_id, member, "txt")
            if not control_member:
                control_group.remove(control_member)
                get_results(result_request_id, member, "alto")

    finish = time.time()
    logger_main.info("Script finished.")
    total_time = int(finish-start)
    logger_main.info(f"All files were processed in {total_time//3600} h, {total_time//60} m and {total_time%60} s ({total_time} s).")


# TODO: enter data
data_path = "/app/JSONs/data.json"
# Right now the format is set to create txt and alto
# result_format = "txt"  # or alto or page(returns page xml)
main()
