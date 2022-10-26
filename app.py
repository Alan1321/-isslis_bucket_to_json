import boto3
import json
import os
from dotenv import load_dotenv

load_dotenv()
BUCKET_NAME=os.getenv('BUCKET_NAME')
BUCKET_PATH=os.getenv('BUCKET_PATH')
BUCKET_PATH_FILES=os.getenv('BUCKET_PATH_FILES')

bucket_name = BUCKET_NAME
path = BUCKET_PATH
path_file = BUCKET_PATH_FILES

s3 = boto3.client('s3')
s3_client = boto3.client('s3')

s3r = boto3.resource('s3')
bucket = s3r.Bucket(bucket_name)
files_in_bucket = list(bucket.objects.all())

all_files = []
files = []
date = "202203"

for file in files_in_bucket:
    if(file.key[0:20] == BUCKET_PATH_FILES):
        file_name = file.key[19:]
        if(date == file_name[16:22]):
            files.append(file_name)
        else:
            all_files.append(files)
            files = []
            files.append(file_name)
            date = file_name[16:22]

all_files.append(files)
files = []

isslis_json_array = []

for file in all_files:
    month = file[0][20:22]
    init_date_day = file[0][22:24]
    end_date_day = file[len(file) - 1][22:24]
    length = len(file)

    day = init_date_day
    data_indices = {}
    start_index = 0
    end_index = 0

    for data in file:
        if(day != data[22:24]):
            data_indices[day] = [start_index, end_index-1]
            start_index = end_index
            day = data[22:24]
        end_index = end_index + 1
    data_indices[day] = [start_index, end_index-1]

    dictionary = {
        "month":month,
        "init_day":init_date_day,
        "end_day":end_date_day,
        "length":length,
        "day_indices":data_indices,
        "file":file
    }

    isslis_json_array.append(dictionary)

json_object = json.dumps(isslis_json_array, indent = 4)

with open("iss_lis_endpoints.json", "w") as outfile:
    outfile.write(json_object)