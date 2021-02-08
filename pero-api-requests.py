import json
import logging
import os
import time
import requests


SERVER_URL = os.environ.get('SERVER_URL')
API_KEY = os.environ.get('API_KEY')
INPUT_FILE = os.environ.get('INPUT_FILE')
headers = {"api-key": API_KEY, "Content-Type": "application/json"}

# logger
file_handler = logging.FileHandler('main_logs.log')
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
file_handler.setFormatter(formatter)
logger_main = logging.getLogger(__name__)
logger_main.setLevel(logging.DEBUG)
logger_main.addHandler(file_handler)


def main():
    format_txt = "txt"
    format_alto = "alto"

    # loading the json file into a dictionary
    file_names = []
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as input_file:
            try:
                data = json.load(input_file)
                for key in data.get("images").keys():
                    file_names.append(key)
            except json.JSONDecodeError as err:
                logger_main.error(f"Error occurred: {err}")
                exit(1)
            except KeyError as err:
                logger_main.error(f"Error occurred: {err}")
                exit(1)
    except FileNotFoundError as err:
        logger_main.error(f"Error occurred: {err}")
        exit(1)
    except Exception as err:
        logger_main.error(f"Error occurred: {err}")
        exit(1)

    # create a destination directory
    output_dir = create_destination()

    # setting timer
    logger_main.info("Processing started.")
    start = time.time()

    # send data for processing and get request id in return
    request_id = ""
    while not request_id:
        request_id = post_processing_request(data)
    # time.sleep(10800) # 3 hour wait for the processing of 1500-2000 images in one request
    time.sleep(60)

    # create session for long processing
    session = requests.Session()

    # request the status of processing
    failed_files = []
    unprocessed_files = []
    processed = False
    while not processed:
        processed = request_status(session, request_id)
        time.sleep(600)

    # download logger
    file_handler_result = logging.FileHandler('result_download.log')
    file_handler_result.setFormatter(formatter)
    result_logger = logging.getLogger("result_logger")
    result_logger.setLevel(logging.INFO)
    result_logger.addHandler(file_handler_result)

    # downloading results
    for name in file_names:
        result = download_results(session, output_dir, request_id, name, format_txt, result_logger)
        if result == "PROCESSED":
            download_results(session, output_dir, request_id, name, format_alto, result_logger)
        else:
            processing = check_status(session, request_id, result)
            if processing == 'PROCESSING_FAILED':
                failed_files.append(name)
            else:
                unprocessed_files.append(name)

    # downloading unprocessed files
    while unprocessed_files:
        result_logger.info(unprocessed_files)
        time.sleep(1800) # wait for processing the files
        for file in unprocessed_files:
            result = download_results(session, output_dir, request_id, file, format_txt, result_logger)
            if result == "PROCESSED":
                download_results(session, output_dir, request_id, file, format_alto, result_logger)
                unprocessed_files.remove(file)

    if not failed_files:
        logger_main.info(f"Processing failed for following files: {failed_files}")
    else:
        logger_main.info(f"None of the files failed to be processed.")

    finish = time.time()
    total_time = int(finish-start)
    logger_main.info(f"Processing finished, "
                     f"total processing time: {total_time//3600} h,"
                     f"{(total_time%3600)//60} m,"
                     f"{total_time%60} s.")


# create the destination directories
def create_destination():
    output_dir = os.path.basename(INPUT_FILE).split(".")[0]
    output_main = "results"
    output_path_txt = os.path.join(output_main, output_dir, "txt")
    output_path_alto = os.path.join(output_main, output_dir, "alto")
    if not os.path.isdir(output_path_txt):
        os.makedirs(output_path_txt)
        logger_main.info(f"{output_path_txt} was successfully created.")
    else:
        logger_main.info(f"{output_path_txt} already exists.")
    if not os.path.isdir(output_path_alto):
        os.makedirs(output_path_alto)
        logger_main.info(f"{output_path_alto} was successfully created.")
    else:
        logger_main.info(f"{output_path_alto} already exists.")
    return os.path.join(output_main, output_dir)


# sends data for processing
def post_processing_request(data):
    url = SERVER_URL + "post_processing_request"
    response = requests.post(url, json=data, headers=headers)
    # logger_main.info(f"Post processing response: {response}: {response.text}")
    response_dict = response.json()
    if response.status_code == 200 and response_dict.get('status') == "success":
        request_id = response_dict.get('request_id')
        logger_main.info(f"Request ID: {request_id}")
        return request_id
    else:
        logger_main.error(f"Processing request ended with code {response.status_code}."
                          f"{response_dict.get('message')}")


def request_status(session, request_id):
    url = SERVER_URL + "request_status/" + request_id
    response = session.get(url, headers=headers)
    response_dict = response.json()
    dict_values = response_dict.get('request_status').values()
    for value in dict_values:
        if 'PROCESSED' in value.get('state'):
            return True
        else:
            logger_main.info(f"{response.status_code} : {response_dict.get('status')}"
                             f" : {response_dict.get('message')}")
            return False


def download_results(session, output_dir, request_id, file_name, result_format, result_logger):
    url = os.path.join(SERVER_URL, "download_results", request_id, file_name, result_format)
    file_path = os.path.join(output_dir, result_format, file_name)
    response = session.get(url, headers=headers)
    if response.status_code == 200:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        result_logger.info(f"{file_name} is processed.")
        return "PROCESSED"
    else:
        response_dict = response.json()
        result_logger.info(f"{file_name} ended with code {response.status_code} : "
                           f"{response_dict.get('status')} : {response_dict.get('message')}")
        return file_name


def check_status(session, request_id, file_name):
    url = SERVER_URL + "request_status/" + request_id
    response = session.get(url, headers=headers)
    file_status = response.json().get('request_status').get(file_name).get('state')
    return file_status


main()
