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

import logging

from pymongo import UpdateOne

from cvinspector.common.utils import CONTROL, VARIANT, DIFF_GROUP_SUFFIX, get_trial_file_name_details
from cvinspector.data_migrate.utils import get_file_name

logger = logging.getLogger(__name__)
#logger.setLevel("DEBUG")

PATH_ENDING_RESOURCES = [".css", ".js", ".png", "jpeg", "gif", "jpg", "webp"]


def contains_important_resource(path):
    for extension in PATH_ENDING_RESOURCES:
        if path.endswith(extension):
            return True
    return False


def get_crawl_groups_by_csv(csv_reader,
                            event_key,
                            crawler_group_name,
                            client,
                            db,
                            crawl_collection,
                            bulk_insert_threshold=500):
    def _create_diff_group_doc(crawl_group_name,
                               ids_mapping,
                               url,
                               crawl_type,
                               discard=False,
                               chunk=None):
        control_ids_str = [str(x) for x in ids_mapping[CONTROL]]
        variant_ids_str = [str(x) for x in ids_mapping[VARIANT]]

        query = {
            "crawl_group_name": crawl_group_name,
            "crawl_type": crawl_type,
            "url": url,
            "discard": str(discard).lower()
        }

        doc = {
            "crawl_group_name": crawl_group_name,
            "control_crawl_instance_ids_str":
            ",".join(control_ids_str),  # string version of objIds
            "control_crawl_instance_ids":
            ids_mapping[CONTROL],  # list of objIds
            "variant_crawl_instance_ids_str":
            ",".join(variant_ids_str),  # string version of objIds
            "variant_crawl_instance_ids":
            ids_mapping[VARIANT],  # list of objIds
            "crawl_type": crawl_type,
            "url": url,
            "discard": str(discard).lower()
        }
        if chunk:
            query["chunk"] = chunk
            doc["chunk"] = chunk

        return query, doc

    crawl_instances_dict = dict()

    collection_name = event_key + DIFF_GROUP_SUFFIX
    diff_group_collection = db[collection_name]

    trials_set = []

    # we do this to get the instances or else the cursor object will stay a cursor object
    for row in csv_reader:
        for col_key in row.keys():
            file_path = row[col_key]
            if "File" in col_key and event_key in file_path:
                file_name = get_file_name(file_path)
                file_key, trial_number, _, control_or_variant, _ = get_trial_file_name_details(
                    file_name, file_path)

                is_control = control_or_variant == CONTROL

                # skip ones that have not trials
                if trial_number is None:
                    logger.debug("skipping due to trial number being None")
                    continue

                if file_key not in crawl_instances_dict:
                    crawl_instances_dict[file_key] = {}
                    crawl_instances_dict[file_key]["url"] = row["URL Crawled"]
                    crawl_instances_dict[file_key]["chunk"] = row["Chunk"]
                if CONTROL not in crawl_instances_dict[file_key]:
                    crawl_instances_dict[file_key][CONTROL] = {}
                if VARIANT not in crawl_instances_dict[file_key]:
                    crawl_instances_dict[file_key][VARIANT] = {}

                crawl_inst = crawl_collection.find_one({
                    "crawl_group_name": crawler_group_name,
                    "file_name": file_name
                })
                if crawl_inst is None:
                    logger.debug("Could not find crawl instance for %s" %
                                 row["URL Crawled"])

                crawl_instances_dict[file_key][control_or_variant][str(
                    trial_number)] = crawl_inst

                if trial_number is not None and trial_number not in trials_set:
                    trials_set.append(str(trial_number))

    discard_crawl_instances = []
    discard_operations = []

    logger.debug(trials_set)
    for key in crawl_instances_dict.keys():
        group_crawl_instances = crawl_instances_dict.get(key)

        url = group_crawl_instances.get("url")
        chunk = group_crawl_instances.get("chunk")
        ids_mapping = dict()
        ids_mapping[CONTROL] = []
        ids_mapping[VARIANT] = []

        discarded = False

        for crawl_type in [CONTROL, VARIANT]:
            ids = []
            crawl_type_discard = False

            # if even one is not there, then we break
            for trial_number in trials_set:
                if trial_number not in group_crawl_instances[crawl_type].keys(
                ):
                    crawl_type_discard = True
                    break

            if crawl_type_discard:
                if key not in discard_crawl_instances:
                    discard_crawl_instances.append(key)
                    discarded = True
                    #.debug(group_crawl_instances[crawl_type])

                for trial_key in group_crawl_instances[crawl_type]:
                    inst = group_crawl_instances[crawl_type][trial_key]
                    #url = inst.get("url")
                    ids.append(inst.get("_id"))

            ids_mapping[crawl_type] = ids

        if discarded:
            query, discard_doc = _create_diff_group_doc(crawler_group_name,
                                                        ids_mapping,
                                                        url,
                                                        crawl_type,
                                                        discard=True,
                                                        chunk=chunk)
            discard_operations.append(
                UpdateOne(query, {"$set": discard_doc}, upsert=True))
            if len(discard_operations) >= bulk_insert_threshold:
                logger.debug("Writing %d discard diff groups instances" %
                             bulk_insert_threshold)
                result = diff_group_collection.bulk_write(discard_operations,
                                                          ordered=False)
                logger.debug("Upserted count: %d" % result.upserted_count)
                discard_operations = []

    # upsert last discard batch
    if len(discard_operations) > 0:
        logger.debug("Writing remaining discard diff groups instances")
        result = diff_group_collection.bulk_write(discard_operations,
                                                  ordered=False)
        logger.debug("Upserted count: %d" % result.upserted_count)
        discard_operations = []

    # keep ones that have complete trials only
    logger.debug("Discarded grouped crawl instances %d" %
                 len(discard_crawl_instances))
    for discarded_key in discard_crawl_instances:
        del crawl_instances_dict[discarded_key]

    # flatten the dict
    crawl_instances_list = []

    logger.debug("Found number of crawl instance keys %d" %
                 len(crawl_instances_dict.keys()))

    diff_group_operations = []
    for key in crawl_instances_dict.keys():
        crawl_group_obj = crawl_instances_dict.get(key)
        crawl_instances_list.append(crawl_group_obj)

        url = crawl_group_obj.get("url")
        chunk = crawl_group_obj.get("chunk")

        ids_mapping = dict()
        ids_mapping[CONTROL] = []
        ids_mapping[VARIANT] = []

        for crawl_type in [CONTROL, VARIANT]:
            trial_crawls = crawl_group_obj.get(crawl_type)

            ids = []

            for trial_crawl in trial_crawls.values():
                ids.append(trial_crawl.get("_id"))

            ids_mapping[crawl_type] = ids

        query, diff_doc = _create_diff_group_doc(crawler_group_name,
                                                 ids_mapping,
                                                 url,
                                                 crawl_type,
                                                 discard=False,
                                                 chunk=chunk)
        diff_group_operations.append(
            UpdateOne(query, {"$setOnInsert": diff_doc}, upsert=True))
        if len(diff_group_operations) >= bulk_insert_threshold:
            logger.debug("Writing %d real diff groups instances" %
                         bulk_insert_threshold)
            result = diff_group_collection.bulk_write(diff_group_operations,
                                                      ordered=False)
            logger.debug("Upserted count: %d" % result.upserted_count)
            diff_group_operations = []
            logger.debug(result)

    # upsert last batch
    if len(diff_group_operations) > 0:
        logger.debug("Writing remaining real diff groups instances %d" %
                     len(diff_group_operations))
        result = diff_group_collection.bulk_write(diff_group_operations,
                                                  ordered=False)
        logger.debug("Upserted count: %d" % result.upserted_count)
        diff_group_operations = []
        logger.debug(result)

    return crawl_instances_list


def create_trial_group(diff_group, crawl_collection):
    crawl_trial_group = dict()
    crawl_trial_group["url"] = diff_group.get("url")
    for crawl_type in [CONTROL, VARIANT]:
        crawl_trial_group[crawl_type] = dict()
        trial_index = 0
        diff_key = crawl_type + "_crawl_instance_ids"

        for mongo_id in diff_group.get(diff_key):
            trial_inst = crawl_collection.find_one({"_id": mongo_id})
            assert (trial_inst
                    is not None), "Cannot find crawl_instance " + str(mongo_id)
            crawl_trial_group[crawl_type][str(trial_index)] = trial_inst
            trial_index = trial_index + 1

    return crawl_trial_group
