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

import json
import logging
import os

import pymongo.errors
from pymongo import UpdateOne

from cvinspector.data_migrate.utils import get_anticv_mongo_client_and_db, get_file_name, \
    process_url_for_special_cases

logger = logging.getLogger(__name__)
#logger.setLevel("DEBUG")


def create_crawler_instance(file_path, file_name, crawler_group_name,
                            dommutation_key, control_or_variant):

    file_data = None
    with open(file_path) as f:
        try:
            file_data = json.load(f)
        except:
            # return out of here
            print("Could not load json file: " + file_path)
            return None, None, None

    # extract crawler instance, we ignore the events
    crawler_instance = {
        "crawl_group_name": crawler_group_name,
        "file_name": file_name,
        "file_path": file_path
    }
    for k, v in file_data.items():
        if k != dommutation_key:
            if k == "url":
                # Treat some urls as special like file_names
                new_url = process_url_for_special_cases(v)
                crawler_instance[k] = new_url
            else:
                crawler_instance[k] = v

    # add is_control
    is_control = str(control_or_variant == "control").lower()
    crawler_instance["is_control"] = is_control

    query = {
        "crawl_group_name": crawler_group_name,
        "url": crawler_instance.get("url"),
        "file_name": file_name,
        "is_control": is_control
    }

    return query, crawler_instance, file_data


def migrate_json_to_mongodb_by_file_path(file_path,
                                         dommutation_key,
                                         collection_name,
                                         crawler_group_name,
                                         main_collection,
                                         crawl_collection,
                                         file_name,
                                         control_or_variant,
                                         migrate_dom_events=False):

    query, crawler_instance, file_data = create_crawler_instance(
        file_path, file_name, crawler_group_name, dommutation_key,
        control_or_variant)

    if crawl_collection:
        # Note: we no longer tie control and variant together
        # try to see if crawler_instance is there already
        result = crawl_collection.find_one(query)

        crawl_instance_id = None
        if not result:
            result = crawl_collection.insert_one(crawler_instance)
            crawl_instance_id = result.inserted_id
            logger.debug("Added one crawl instance")
            logger.debug(crawler_instance)
        else:
            logger.debug(
                "Found existing crawl instance, using that instead: " +
                str(result.get("_id")))
            crawl_instance_id = result.get("_id")

        if migrate_dom_events:
            # see if we already imported the events
            # if we have just one, we already did it, return
            result = main_collection.find_one({
                "crawl_group_name":
                crawler_group_name,
                "crawl_instance_id":
                crawl_instance_id
            })
            if result:
                logger.debug(
                    "Already imported events for crawl_group_name %s and instance %s"
                    % (crawler_group_name, crawl_instance_id))
                return

            # extract events using key
            events = file_data[dommutation_key]

            # attach the crawler_group_name and document_id to each request as well
            for event in events:
                event["crawl_group_name"] = crawler_group_name
                event["crawl_instance_id"] = crawl_instance_id

            try:
                main_collection.insert_many(events, ordered=False)
            except pymongo.errors.DocumentTooLarge:
                logger.debug(
                    "Could not add event to collection because of large size")
            logger.debug("Added " + str(len(events)) + " events to " +
                         collection_name)
    else:
        logger.warn("Collection for crawl_instance was not found")


def migrate_json_to_mongodb(file_or_dir_path,
                            main_key,
                            collection_name,
                            crawler_group_name,
                            control_or_variant,
                            mongodb_client,
                            mongodb_port,
                            username=None,
                            password=None):
    client, db = get_anticv_mongo_client_and_db(mongodb_client,
                                                mongodb_port,
                                                username=username,
                                                password=password)

    # some event collection
    collection = db[collection_name]
    crawl_collection = db['crawl_instance']

    if collection:
        if os.path.exists(file_or_dir_path):
            # treat this as a file
            if os.path.isfile(file_or_dir_path):
                logger.debug("Input is  a file")
                file_name = get_file_name(file_or_dir_path)
                logger.debug("found file_name: " + file_name)
                if file_or_dir_path.endswith(".json"):
                    migrate_json_to_mongodb_by_file_path(
                        file_or_dir_path, main_key, collection_name,
                        crawler_group_name, collection, crawl_collection,
                        file_name, control_or_variant)
            elif os.path.isdir(file_or_dir_path):
                logger.debug("Input is a directory")
                operations = []
                # walk only immediate files and parse them
                for root, _, files in os.walk(file_or_dir_path):
                    for data_file_name in files:
                        # ignore MAC OS files
                        if data_file_name != ".DS_Store":
                            data_file_path = root + os.sep + data_file_name
                            if data_file_path.endswith(".json"):
                                query, crawler_instance, file_data = create_crawler_instance(
                                    data_file_path, data_file_name,
                                    crawler_group_name, main_key,
                                    control_or_variant)
                                if crawler_instance:
                                    operations.append(
                                        UpdateOne(
                                            query,
                                            {"$setOnInsert": crawler_instance},
                                            upsert=True))
                                else:
                                    logger.warn(
                                        "Could not add crawl instance for : %s"
                                        % data_file_path)
                                if len(operations) >= 1000:
                                    logger.debug(
                                        "Writing 1000 crawl instances")
                                    result = crawl_collection.bulk_write(
                                        operations, ordered=False)
                                    logger.debug("Upserted count: %d" %
                                                 result.upserted_count)
                                    operations = []
                    break

                if len(operations) > 0:
                    logger.debug("Writing remaining crawl instances")
                    result = crawl_collection.bulk_write(operations,
                                                         ordered=False)
                    logger.debug("Upserted count: %d" % result.upserted_count)

    else:
        logger.warn("Collection " + collection_name + " was not found")

    client.close()
