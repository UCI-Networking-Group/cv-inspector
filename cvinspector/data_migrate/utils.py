#  Copyright (c) 2021 Hieu Le and the UCI Networking Group
#  <https://athinagroup.eng.uci.edu>.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os

from pymongo import MongoClient

# change these to connect to correct db
MONGO_CLIENT_HOST = 'localhost'
MONGO_CLIENT_PORT = 27017
ANTICV_MONGO_DB = "anticircumvention"


def get_mongo_client_and_db(client,
                            port,
                            db_name,
                            username=None,
                            password=None,
                            authSource=None):
    if username and password:
        client = MongoClient(client,
                             port,
                             username=username,
                             password=password,
                             authSource=authSource)
    else:
        # default to just regular localhost
        client = MongoClient(client, port)

    db = client[db_name]

    if not client:
        print("No client was found for " + client + " " + port)
    if not db:
        print("No db was found for " + db_name)

    if not client or not db:
        raise Exception("Could find mongo client or database")

    return client, db


def get_anticv_mongo_client_and_db(client, port, username=None, password=None):
    if username and password:
        return get_mongo_client_and_db(client,
                                       port,
                                       ANTICV_MONGO_DB,
                                       username=username,
                                       password=password,
                                       authSource=ANTICV_MONGO_DB)

    return get_mongo_client_and_db(client, port, ANTICV_MONGO_DB)


def get_file_name(file_path):
    file_path_split = file_path.split(os.sep)
    file_name = file_path_split[-1]
    return file_name


def process_url_for_special_cases(url):
    # instartLogic URL
    SPECIAL_CASE = "g00"

    new_url = url
    if SPECIAL_CASE in url:
        new_url = url[:url.index(SPECIAL_CASE)]
        print("Found original URL: " + new_url)

    return new_url
