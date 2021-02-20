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

import csv
import json
import logging
import math
import random
import statistics
import string
import time
from multiprocessing import Process

from pymongo import MongoClient
from scipy.stats import linregress

logger = logging.getLogger(__name__)

# change these to connect to correct db
MONGO_CLIENT_HOST = 'localhost'
MONGO_CLIENT_PORT = 27017
MONGO_DB = "anticircumvention"

# SIMPLE
CONTROL = "control"
VARIANT = "variant"

# JSON KEYS
JSON_WEBREQUEST_KEY = "cvwebrequests"
JSON_DOMMUTATION_KEY = "dommutation"

# hardcoded collections
MONGODB_COLLECTION_CRAWL_INSTANCE = "crawl_instance"

# holds raw data
MONGODB_COLLECTION_WEBREQUESTS_CONTROL = "vanilla_webrequests"
MONGODB_COLLECTION_WEBREQUESTS_VARIANT = "adb_webrequests"
MONGODB_COLLECTION_DOMMUTATION_CONTROL = "vanilla_dommutation"
MONGODB_COLLECTION_DOMMUTATION_VARIANT = "adb_dommutation"

# for webrequests only (holds differences)
MONGODB_COLLECTION_WR_CONTROL_ONLY = "control_only"
MONGODB_COLLECTION_WR_VARIANT_ONLY = "variant_only"

# for dom mutation (holds differences)
MONGODB_CONTROL_ONLY_DOM_COLLECTION_NAME = "control_dommutation"
MONGODB_VARIANT_ONLY_DOM_COLLECTION_NAME = "variant_dommutation"

# holds the trials together in groups. Holds references to the differences above to
# MONGODB_COLLECTION_WR_CONTROL_ONLY etc
DIFF_GROUP_SUFFIX = "_diff_group"
MONGODB_WR_DIFF_GROUP = JSON_WEBREQUEST_KEY + DIFF_GROUP_SUFFIX
MONGODB_DOM_DIFF_GROUP = JSON_DOMMUTATION_KEY + DIFF_GROUP_SUFFIX

#page source (holds differences)
MONGODB_PGSOURCE_COLLECTION_NAME = "pgsource_diff"

# data file suffix
WEBREQUESTS_DATA_FILE_SUFFIX_CONTROL = "--cvwebrequestsvanilla.json"
WEBREQUESTS_DATA_FILE_SUFFIX_VARIANT = "--cvwebrequests.json"

DOMMUTATION_DATA_FILE_SUFFIX_CONTROL = "--cvdommutationvanilla.json"
DOMMUTATION_DATA_FILE_SUFFIX_VARIANT = "--cvdommutation.json"

PAGE_SOURCE_SUFFIX = "__pagesource.html"

ERR_BLOCKED_BY_CLIENT = "ERR_BLOCKED_BY_CLIENT"
ABP_BLOCKED_ELEMENT = "abp-blocked-element"
ABP_BLOCKED_SNIPPET = "abp-blocked-snippet"
ANTICV_HIDDEN = "anticv-hidden"
ANTICV_OFFSETWIDTH = "anticv-offsetwidth"
ANTICV_OFFSETHEIGHT = "anticv-offsetheight"
ANTICV_ANNOTATION_PREFIX = "anticv-"

TRIAL_PREFIX = "__trial"


def get_trial_label(trial_count):
    return TRIAL_PREFIX + str(trial_count)


def randomword(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


# break list into chunks of n. Meaning each list will contain at most n items
def chunk(some_list, n=4):
    final = [
        some_list[i * n:(i + 1) * n]
        for i in range((len(some_list) + n - 1) // n)
    ]
    return final


def _get_mongo_client_and_db(client_host, client_port, db_name):
    client = MongoClient(client_host, client_port)
    db = client[db_name]

    if not client:
        logger.warn("No client was found for %s %s", 
                    client_host, client_port)
    if not db:
        logger.warn("No db was found for %s", db_name)

    if not client or not db:
        raise Exception("Could find mongo client or database")

    return client, db


# shortcut to get our hardcoded anticv client and db
def get_anticv_client_and_db():
    return _get_mongo_client_and_db(MONGO_CLIENT_HOST, MONGO_CLIENT_PORT,
                                    MONGO_DB)


# returns cursor object
def get_by_crawl_group_name(crawl_group_name,
                            db,
                            collection_name,
                            find_one=False,
                            **kwargs):
    collection = db[collection_name]
    find_dict = {"crawl_group_name": crawl_group_name}
    find_dict.update(kwargs)
    if not find_one:
        return collection.find(find_dict)
    else:
        return collection.find_one(find_dict)


def get_entropy(string, base=2.0):
    #make set with all unrepeatable symbols from string
    dct = dict.fromkeys(list(string))
    #calculate frequencies
    pkvec = [float(string.count(c)) / len(string) for c in dct]

    #calculate Entropy
    H = -sum([pk * math.log(pk, base) for pk in pkvec])
    return H


def _get_common_stats_default():
    stats = {"mean": 0, "variance": 0, "max_val": 0}

    return stats


def _get_common_stats_for_number_list(list_of_numbers):
    stats = _get_common_stats_default()

    if list_of_numbers is not None and len(list_of_numbers) == 0:
        return stats

    items_count = len(list_of_numbers)

    if items_count > 1:
        stats["mean"] = statistics.mean(list_of_numbers)

    if len(list_of_numbers) > 1:
        stats["variance"] = statistics.variance(list_of_numbers)

    stats["max_val"] = max(list_of_numbers)

    for key in stats:
        val = stats.get(key)
        val = round(val, 2)
        stats[key] = val

    return stats


def get_trial_file_name_details(file_name, file_path, other_suffix=None):
    if file_name is not None:
        event_key = None
        if JSON_DOMMUTATION_KEY in file_name:
            event_key = JSON_DOMMUTATION_KEY
        else:
            event_key = JSON_WEBREQUEST_KEY

        control_or_variant = CONTROL
        if VARIANT in file_path:
            control_or_variant = VARIANT

        trial_file_name = file_name
        if event_key:
            trial_file_name = file_name.replace("cv" + event_key, "").replace(
                event_key, "").replace("vanilla.json",
                                       "").replace(".json",
                                                   "").replace("--", "")

        if other_suffix:
            trial_file_name.replace(other_suffix, "")

        file_split = trial_file_name.split("__trial")
        if len(file_split) == 2:
            trial_number = file_split[1]

            # file_key will now be the random part of the name
            file_split = file_split[0].split("_")

            file_key = file_split[-1]

            file_prefix = "_".join(file_split[:-1])

            return file_key, trial_number, event_key, control_or_variant, file_prefix

    return None, None, None, None, None


def get_ground_truth(ground_truth_file_path):
    # read in file with domains labeled as positives
    # if line starts with ! , then it means it is negative label
    positive_label_domains = []
    negative_label_domains = []
    if ground_truth_file_path:
        with open(ground_truth_file_path, 'r') as ground_truth_file:
            for line in ground_truth_file:
                if line.startswith("!"):
                    negative_label_domains.append(
                        line.replace("\n", "").replace("!", ""))
                else:
                    positive_label_domains.append(line.replace("\n", ""))

        logger.info("Found %d positive label domains",
                    len(positive_label_domains))
        logger.info("Found %d negative label domains",
                    len(negative_label_domains))
    else:
        logger.warn(
            "Warning: No ground truth file given. All rows will be considered unlabel (-1)"
        )

    return positive_label_domains, negative_label_domains


def avg_growth_rate(past, present, time_periods):
    if past == 0:
        return 0

    return math.pow(present / past, 1 / time_periods) - 1


def get_linear_regress(x, y):
    #slope, intercept, r_value, p_value, std_err = linregress(x, y)
    return linregress(x, y)


def get_css_dict(css_string):
    split_values = css_string.split(";")
    # we should have even counts of key amd values
    css_dict = dict()
    for split_val in split_values:
        # split on first ocurrence because there may be more the one if the value has http://
        key_val = split_val.split(":", 1)
        # key/val
        if len(key_val) == 2:
            css_dict[key_val[0].strip()] = key_val[1].strip()
    return css_dict


def get_webrequests_from_raw_json(file_path, event_status):
    webrequests = []
    with open(file_path) as f:
        try:
            file_data = json.load(f)
            requests = file_data[JSON_WEBREQUEST_KEY]
            if requests and len(requests) > 0:
                for req in requests:
                    event = req["event"]
                    if event and "status" in event and event[
                            "status"] == event_status:
                        webrequests.append(req)
        except:
            # return out of here
            logger.debug("Could not load json file: %s", file_path)
            return

    return webrequests


def get_blocked_webrequests(file_path):
    events = get_webrequests_from_raw_json(file_path, "onErrorOccurred")
    blocked_urls = []
    for event in events:
        event_inner = event["event"]
        url = event_inner["url"]
        details = event_inner.get("details")
        if details:
            try:
                details_json = json.loads(details)
                if ERR_BLOCKED_BY_CLIENT in details_json.get("error"):
                    blocked_urls.append(url)
                else:
                    continue
            except Exception as e:
                logger.debug("Could not parse details json")
                logger.debug(e)
                continue
        else:
            continue

    return blocked_urls


def get_dom_mutation_from_raw_json(file_path):
    dom_events = []
    with open(file_path) as f:
        try:
            file_data = json.load(f)
            events = file_data[JSON_DOMMUTATION_KEY]
            if events and len(events) > 0:
                for ev in events:
                    dom_events.append(ev)
        except:
            # return out of here
            print("Could not load json file: %s", file_path)
            return

    return dom_events


class OutputCSVBase:
    def __init__(self,
                 id,
                 name,
                 csv_file_path,
                 shutdown_output,
                 output_csv_queue,
                 header_row=None,
                 cache_row_limit=1000):

        self.id = id
        self.name = name
        self.shutdown_output = shutdown_output
        self.csv_file_path = csv_file_path
        self.output_csv_queue = output_csv_queue
        self.header_row = header_row
        self.cached_rows = []
        self.cache_row_limit = cache_row_limit

    def should_shut_down(self):
        return self.shutdown_output

    def run(self):
        with open(self.csv_file_path, 'w') as output_name_file:
            csvwriter = csv.writer(output_name_file)
            if self.header_row:
                csvwriter.writerow(self.header_row)
            while not self.should_shut_down():
                try:
                    row = self.output_csv_queue.get(timeout=10)
                    self.cached_rows.append(row)

                    if len(self.cached_rows) >= self.cache_row_limit:
                        rows_count = len(self.cached_rows)
                        csvwriter.writerows(self.cached_rows)
                        logger.debug("%s - Wrote new rows: %d",
                                     self.name, rows_count)
                        self.cached_rows.clear()

                    self.output_csv_queue.task_done()
                except:
                    pass

            if len(self.cached_rows) >= 0:
                rows_count = len(self.cached_rows)
                csvwriter.writerows(self.cached_rows)
                logger.debug("%s - Wrote new rows: %d",
                             self.name, rows_count)
                self.cached_rows.clear()

            output_name_file.flush()

            time.sleep(10)


class OutputCSVForceHeaderBase:
    def __init__(self,
                 id,
                 name,
                 csv_file_path,
                 shutdown_output,
                 output_csv_queue,
                 header_delimiter,
                 header_row=None,
                 cache_row_limit=1000):

        self.id = id
        self.name = name
        self.shutdown_output = shutdown_output
        self.csv_file_path = csv_file_path
        self.output_csv_queue = output_csv_queue
        self.header_row = header_row
        self.cached_rows = []
        self.cache_row_limit = cache_row_limit
        self.header_delimiter = header_delimiter
        self.done_with_header = False
        assert header_delimiter is not None, "Header Delimiter cannot be None"

    def should_shut_down(self):
        return self.shutdown_output

    def get_header_from_cache(self):
        header_row_match = None

        for row in self.cached_rows:
            if self.header_delimiter in row:
                header_row_match = row
                logger.debug("%s - header row found", self.name)
                break

        if header_row_match:
            self.cached_rows.remove(header_row_match)
            logger.debug("%s - removing header row from cache", self.name)

        return header_row_match

    def run(self):
        with open(self.csv_file_path, 'w') as output_name_file:
            csvwriter = csv.writer(output_name_file)
            if self.header_row:
                csvwriter.writerow(self.header_row)
                self.done_with_header = True
                logger.debug("%s - Wrote header row", self.name)
            while not self.should_shut_down():
                try:
                    row = self.output_csv_queue.get(timeout=10)
                    self.cached_rows.append(row)

                    if len(self.cached_rows) >= self.cache_row_limit:
                        # find the header row first and write it
                        if not self.done_with_header:
                            header_row_match = self.get_header_from_cache()
                            if header_row_match:
                                csvwriter.writerow(header_row_match)
                                self.done_with_header = True
                                logger.debug("%s - Wrote header row",
                                             self.name)

                        # only output once the header row is done
                        if self.done_with_header:
                            rows_count = len(self.cached_rows)
                            csvwriter.writerows(self.cached_rows)
                            logger.debug("%s - Wrote new rows: %d",
                                         self.name, rows_count)
                            self.cached_rows.clear()

                    self.output_csv_queue.task_done()
                except:
                    pass

            if len(self.cached_rows) >= 0:
                if not self.done_with_header:
                    logger.debug("%s - Trying to find header row",
                                 self.name)
                    header_row_match = self.get_header_from_cache()
                    if header_row_match:
                        csvwriter.writerow(header_row_match)
                        self.done_with_header = True
                        logger.debug("%s - Wrote header row", self.name)
                if len(self.cached_rows) >= 0:
                    rows_count = len(self.cached_rows)
                    csvwriter.writerows(self.cached_rows)
                    logger.debug("%s - Wrote new rows: %d",
                                 self.name, rows_count)
                    self.cached_rows.clear()

            if not self.done_with_header:
                logger.warning("%s - Did not find header row by the end",
                               self.name)

            output_name_file.flush()

            time.sleep(10)


class OutputCSVProcess(OutputCSVBase, Process):
    def __init__(self, *args, **kwargs):
        OutputCSVBase.__init__(self, *args, **kwargs)
        Process.__init__(self)

    def should_shut_down(self):
        # here shutdown is an event
        return self.shutdown_output.is_set()


class OutputCSVForceHeaderProcess(OutputCSVForceHeaderBase, Process):
    def __init__(self, *args, **kwargs):
        OutputCSVForceHeaderBase.__init__(self, *args, **kwargs)
        Process.__init__(self)

    def should_shut_down(self):
        # here shutdown is an event
        return self.shutdown_output.is_set()


class OutputDebugBase:
    def __init__(self,
                 id,
                 name,
                 file_path,
                 shutdown_output,
                 output_queue,
                 cache_row_limit=2000):
        self.id = id
        self.name = name
        self.shutdown_output = shutdown_output
        self.file_path = file_path
        self.output_queue = output_queue
        self.cached_rows = []
        self.cache_row_limit = cache_row_limit

    def should_shut_down(self):
        return self.shutdown_output

    def run(self):
        with open(self.file_path, 'w') as output_name_file:
            while not self.should_shut_down():
                try:
                    row = self.output_queue.get(timeout=10)
                    self.cached_rows.append(row)
                    if len(self.cached_rows) >= self.cache_row_limit:
                        output_name_file.write("\n".join(self.cached_rows))
                        logger.debug("%s - Wrote new rows %d",
                                     self.name, len(self.cached_rows))
                        self.cached_rows.clear()

                    self.output_queue.task_done()
                except:
                    pass
                
            if len(self.cached_rows) >= 0:
                output_name_file.write("\n".join(self.cached_rows))
                logger.debug("%s - Wrote new rows %d",
                             self.name, len(self.cached_rows))
                self.cached_rows.clear()

            # force flush
            output_name_file.flush()

            time.sleep(10)


class OutputDebugProcess(OutputDebugBase, Process):
    def __init__(self, *args, **kwargs):
        OutputDebugBase.__init__(self, *args, **kwargs)
        Process.__init__(self)

    def should_shut_down(self):
        # here shutdown is an event
        return self.shutdown_output.is_set()
