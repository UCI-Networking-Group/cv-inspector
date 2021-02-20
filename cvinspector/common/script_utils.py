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
import os
import re
import subprocess
import threading
import time
from json import JSONDecodeError
from multiprocessing import Queue, Process, Event

from cvinspector.common.utils import JSON_WEBREQUEST_KEY, JSON_DOMMUTATION_KEY, \
    MONGODB_COLLECTION_CRAWL_INSTANCE, MONGODB_COLLECTION_WEBREQUESTS_CONTROL, \
    MONGODB_COLLECTION_WEBREQUESTS_VARIANT, MONGODB_COLLECTION_DOMMUTATION_CONTROL, \
    MONGODB_COLLECTION_DOMMUTATION_VARIANT
from cvinspector.common.utils import randomword, CONTROL, VARIANT, get_ground_truth, chunk, OutputCSVProcess, \
    get_trial_file_name_details, get_trial_label
from cvinspector.data_migrate.migrate_dommutation import migrate_json_to_mongodb as migrate_json_to_mongdo_dommutation
from cvinspector.data_migrate.migrate_webrequest import migrate_json_to_mongodb as migrate_json_to_mongdo_webrequest
from cvinspector.data_migrate.utils import MONGO_CLIENT_HOST, MONGO_CLIENT_PORT, get_anticv_mongo_client_and_db
from cvinspector.diff_analysis.utils import get_crawl_groups_by_csv
from cvinspector.ml.output_features_to_csv import CV_DETECT_TARGET_NAME
from cvinspector.ml.plot import auto_bin_time_series_csv

logger = logging.getLogger(__name__)
#logger.setLevel("DEBUG")

leading_4_spaces = re.compile('^    ')

DOMAIN_PREFIX = "||"  # denotes that this is a domain
#denotes end of domain and that it must be a thirdparty request
THIRD_PARTY_SUFFIX = "^$third-party"
NEW_LINE = "\n"
LINES_CHANGED = "lines_changed"

ignore_host_list = ["localhost", "localdomain", "android", "test", "testing"]
URL_CRAWLED = "URL Crawled"


def get_possible_domains(str_value, delimiter=","):
    split_domains = str_value.split(delimiter)
    return split_domains


def get_possible_domains_ignore_simple_rules(rule, delimiter):
    split_lines = rule.split(delimiter)
    possible_domains = get_possible_domains(split_lines[0])
    return possible_domains


# each line is a rule
def find_domain_in_rule(line):
    line_processed_success = False
    domains_found = []

    if len(line.strip()) == 0:
        return line_processed_success, domains_found

    #ignore comment lines
    if line.startswith("!") or line.startswith(
            "@@") or "$popup" in line or "third-party" in line:
        return line_processed_success, domains_found

    simple_rule_found = None
    # for domain=, the rule is really targeting what comes after
    if "domain=" in line:
        split_lines = line.split("domain=")
        if len(split_lines) > 1:
            possible_domains = get_possible_domains(split_lines[1],
                                                    delimiter="|")
            domains_found = [
                x.strip() for x in possible_domains if not x.startswith("~")
            ]
            line_processed_success = True
    elif line.startswith(DOMAIN_PREFIX):
        if "^" in line:
            # example: ||dgnepemukk.com^$script,xmlhttprequest --> extract dgnepemukk.com
            domain = line.replace(DOMAIN_PREFIX, "").split("^")[0]
            if len(domain) > 0:
                domains_found.append(domain.strip())
                line_processed_success = True
        else:
            # skip over these as they are too complicated to parse
            logger.warning("COMPLICATED LINE: Could not parse domains for %s",
                           line)
            return line_processed_success, domains_found
    elif "#$#" in line:
        simple_rule_found = "#$#"
    elif "#?#" in line:
        simple_rule_found = "#?#"
    elif "###" in line:
        simple_rule_found = "###"
    elif "##" in line:
        simple_rule_found = "##"
    # this is an exception rule
    #elif "#@#" in line:
    #    simple_rule_found = "#@#"

    if simple_rule_found:
        #print("Simple rule found: %s" % line)
        domains_found = get_possible_domains_ignore_simple_rules(
            line, simple_rule_found)
        line_processed_success = True

    if not line_processed_success:
        logger.warning("Could not parse domains for %s", line)

    # clean each domain
    domains_found_clean = []
    for domain in domains_found:
        if domain.startswith("-"):
            domain = domain.replace("-", "", 1)
            domains_found_clean.append(domain.strip())
        else:
            domains_found_clean.append(domain.strip())

    return line_processed_success, domains_found_clean


def get_domains_from_commit(commit_hash,
                            commit_date_str,
                            commit_date,
                            git_repo_path,
                            target_file_name=None):
    #git diff 38836e599ed6d523a388bde065a3760a9da7c1e8^!  -U0
    if target_file_name:
        lines = subprocess.check_output(
            ['git', 'diff', "-U0", commit_hash + "^!", target_file_name],
            cwd=git_repo_path,
            stderr=subprocess.STDOUT).decode("utf-8").split('\n')
    else:
        lines = subprocess.check_output(
            ['git', 'diff', "-U0", commit_hash + "^!"],
            cwd=git_repo_path,
            stderr=subprocess.STDOUT).decode("utf-8").split('\n')

    lines_change_only = []
    domain_actions = dict()
    domains_added = []
    domains_removed = []
    domains_modified = []

    ignore_diff_file = False
    for line in lines:
        if line.startswith("diff"):
            if ".md" in line or "LICENSE" in line or "gitignore" in line:
                ignore_diff_file = True
                continue
            else:
                ignore_diff_file = False
        elif ignore_diff_file:
            continue

        if not line.startswith("diff") and not line.startswith("index") and not line.startswith("---") \
            and not line.startswith("+++") and not line.startswith("@@"):
            lines_change_only.append(line)
            line_processed_success, domains_found = find_domain_in_rule(
                line.replace("+", "", 1).replace("-", "", 1))
            if line_processed_success:
                if line.startswith("+"):
                    domains_added += domains_found
                elif line.startswith("-"):
                    domains_removed += domains_found

    domains_added = set(domains_added)
    domains_removed = set(domains_removed)
    modified_set = set([])
    if len(domains_added) > 0 and len(domains_removed) > 0:

        modified_set = domains_added.intersection(domains_removed)
        domains_modified += list(modified_set)

        # remove from other lists
        for domain in modified_set:
            domains_added.remove(domain)
            domains_removed.remove(domain)

    # put found domains into dict
    for domain in domains_added:
        if domain not in domain_actions:
            domain_actions[domain] = []
        domain_actions[domain].append(
            (commit_date, "insertion", commit_date_str))
    for domain in domains_removed:
        if domain not in domain_actions:
            domain_actions[domain] = []
        domain_actions[domain].append(
            (commit_date, "deletion", commit_date_str))

    for domain in domains_modified:
        if domain not in domain_actions:
            domain_actions[domain] = []
        domain_actions[domain].append((commit_date, "modify", commit_date_str))

    return domain_actions


def get_filtertype_from_commit(commit_hash,
                               commit_date_str,
                               commit_date,
                               git_repo_path,
                               target_file_name=None):
    #git diff 38836e599ed6d523a388bde065a3760a9da7c1e8^!  -U0
    if target_file_name:
        lines = subprocess.check_output(
            ['git', 'diff', "-U0", commit_hash + "^!", target_file_name],
            cwd=git_repo_path,
            stderr=subprocess.STDOUT).decode("utf-8").split('\n')
    else:
        lines = subprocess.check_output(
            ['git', 'diff', "-U0", commit_hash + "^!"],
            cwd=git_repo_path,
            stderr=subprocess.STDOUT).decode("utf-8").split('\n')

    lines_change_only = []
    domain_actions = dict()
    domains_added = []
    domains_removed = []
    domains_modified = []

    all_lines_stats = get_default_rule_type_dict()

    ignore_diff_file = False
    for line in lines:
        if line.startswith("diff"):
            if ".md" in line or "LICENSE" in line or "gitignore" in line:
                ignore_diff_file = True
                continue
            else:
                ignore_diff_file = False
        elif ignore_diff_file:
            continue

        if not line.startswith("diff") and not line.startswith("index") and not line.startswith("---") \
            and not line.startswith("+++") and not line.startswith("@@"):
            lines_change_only.append(line)
            # get filter type for line
            line_stats = find_line_stats(line)
            # add it up to the main all_lines_stats
            all_lines_stats = {
                key: all_lines_stats.get(key, 0) + line_stats.get(key, 0)
                for key in set(all_lines_stats) | set(line_stats)
            }

    return all_lines_stats


def get_default_rule_type_dict():
    return {
        "Web Request Blocking": 0,
        "Element Hiding": 0,
        "Whitelisting": 0,
        "Advance Element Hiding": 0,
        "Advance JS aborting": 0,
        "Advance Misc.": 0
    }


# each line is a rule
def find_line_stats(line):
    file_stats = get_default_rule_type_dict()

    #ignore comment lines
    if line.startswith("!") or len(line.strip()) == 0:
        return file_stats

    if line.startswith("@@"):
        file_stats["Whitelisting"] += 1
        return file_stats

    # whitelisting element hiding
    if "#@#" in line:
        file_stats["Whitelisting"] += 1
        return file_stats

    if "#$#" in line:
        if "abort" in line:
            file_stats["Advance JS aborting"] += 1
        elif "hide-if-contains-visible-text" in line or "hide-if-contains-and-matches-style" in line or \
            "hide-if-has-and-matches-style" in line or "hide-if-contains-image" in line or \
            "hide-if-contains-image-hash" in line or "hide-if-shadow-contains" in line or \
            "hide-if-contains" in line:
            file_stats["Advance Element Hiding"] += 1
        else:
            file_stats["Advance Misc."] += 1

        return file_stats

    if "#?#" in line:
        file_stats["Element Hiding"] += 1
        return file_stats

    if "##" in line:
        file_stats["Element Hiding"] += 1
        return file_stats

    file_stats["Web Request Blocking"] += 1
    return file_stats


def group_trials(csvwriter,
                 crawl_group_name,
                 input_directory,
                 crawl_chunk,
                 file_suffix,
                 logger,
                 ground_truth_file=None,
                 trials=4):

    if crawl_chunk:
        crawl_data_output = input_directory + os.sep + crawl_chunk + os.sep + "crawl_data_" + crawl_group_name + os.sep
    else:
        crawl_data_output = input_directory + os.sep + "crawl_data_" + crawl_group_name + os.sep

    crawl_data_output__webrequests_control = crawl_data_output + "control_webrequests" + os.sep
    crawl_data_output__webrequests_variant = crawl_data_output + "variant_webrequests" + os.sep
    crawl_data_output__dom_control = crawl_data_output + "control_dommutation" + os.sep
    crawl_data_output__dom_variant = crawl_data_output + "variant_dommutation" + os.sep

    positive_label_domains = None
    negative_label_domains = None
    if ground_truth_file:
        positive_label_domains, negative_label_domains = get_ground_truth(
            ground_truth_file)

    files_process = dict()
    for main_dir in [
            crawl_data_output__webrequests_control,
            crawl_data_output__webrequests_variant,
            crawl_data_output__dom_control, crawl_data_output__dom_variant
    ]:
        for root, _, files in os.walk(main_dir):
            for file_name in files:
                # ignore MAC OS files
                if file_name != ".DS_Store":
                    # here we leverage the files collected for webrequests vanilla only
                    if file_name.endswith(file_suffix):
                        # clean the url
                        file_path = root + os.sep + file_name

                        if "trial" not in file_name:
                            logger.debug(
                                "Skipping file due to no trial keyword: %s",
                                str(file_name))
                            continue

                        file_key, trial_number, event_key, control_or_variant, _ = get_trial_file_name_details(
                            file_name, file_path)

                        logger.debug("File Key %s" % file_key)
                        if file_key is None:
                            logger.debug("Skipping file due to none: %s",
                                         str(file_name))
                            continue

                        logger.debug(
                            "file_key %s, control_or_variant: %s, event_key: %s, trial_number: %s  "
                            % (file_key, control_or_variant, event_key,
                               str(trial_number)))

                        if file_key not in files_process:
                            files_process[file_key] = dict()
                            files_process[file_key][CONTROL] = dict()
                            files_process[file_key][VARIANT] = dict()

                        if event_key not in files_process[file_key][
                                control_or_variant]:
                            files_process[file_key][control_or_variant][
                                event_key] = dict()

                        files_process[file_key][control_or_variant][event_key][
                            trial_number] = file_path

    logger.debug("Number of possible groups found: %d",
                 len(files_process.keys()))

    for file_key in files_process:
        found_all_trials = True
        url = None
        csv_row = []
        for trial_number in range(trials):
            trial_number = str(trial_number)
            for event_key in [JSON_WEBREQUEST_KEY, JSON_DOMMUTATION_KEY]:
                for control_or_variant in [CONTROL, VARIANT]:
                    if control_or_variant in files_process[file_key] and \
                        event_key in files_process[file_key][control_or_variant] and \
                        trial_number in files_process[file_key][control_or_variant][event_key]:
                        file_path = files_process[file_key][
                            control_or_variant][event_key][str(trial_number)]
                        if not url:
                            # this checks the control wr file and gets url
                            with open(file_path, 'r') as file_opened:
                                try:
                                    data = json.load(file_opened)
                                    url = data["url"]
                                    csv_row = [url, crawl_chunk]
                                    if positive_label_domains and url in positive_label_domains:
                                        csv_row.append(1)
                                    elif negative_label_domains and url in negative_label_domains:
                                        csv_row.append(0)
                                    else:
                                        csv_row.append(-1)

                                except UnicodeDecodeError as e:
                                    print("Could not open " + file_path)
                                    print(e)
                                    continue
                                except JSONDecodeError as e:
                                    print("Could not open " + file_path)
                                    print(e)
                                    continue

                        csv_row.append(file_path)
                    else:
                        found_all_trials = False
                        logger.debug(
                            "Missing file_key %s, control_or_variant: %s, event_key: %s, trial_number: %s  "
                            % (file_key, control_or_variant, event_key,
                               str(trial_number)))

        if found_all_trials:
            csvwriter.writerow(csv_row)
        else:
            logger.debug("Skipping adding row for %s", url)


def process_group_trails(input_directory,
                         output_file_name,
                         crawl_group_name,
                         logger,
                         file_suffix=".json",
                         ground_truth_file=None,
                         trials=4):

    FILE_PATH_WR_CONTROL = "File Path WR Vanilla"
    FILE_PATH_WR_VARIANT = "File Path WR"
    FILE_PATH_DOM_CONTROL = "File Path DOM Vanilla"
    FILE_PATH_DOM_VARIANT = "File Path DOM"

    output_file_path = input_directory + os.sep + output_file_name
    output_file_opened = open(output_file_path, 'w')
    csvwriter = csv.writer(output_file_opened)
    header = ["URL Crawled", "Chunk", CV_DETECT_TARGET_NAME]

    for trial_index in range(trials):
        trial_label = get_trial_label(trial_index)
        header += [
            FILE_PATH_WR_CONTROL + " " + trial_label,
            FILE_PATH_WR_VARIANT + " " + trial_label
        ]
        header += [
            FILE_PATH_DOM_CONTROL + " " + trial_label,
            FILE_PATH_DOM_VARIANT + " " + trial_label
        ]
    csvwriter.writerow(header)

    found_chunk_directories = False
    for root, directories, _ in os.walk(input_directory):
        for directory in directories:
            if "_to_" in directory:
                logger.debug("Processing chunk directory %s", directory)

                found_chunk_directories = True
                crawl_chunk = directory
                group_trials(csvwriter,
                             crawl_group_name,
                             root,
                             crawl_chunk,
                             file_suffix,
                             logger,
                             ground_truth_file=ground_truth_file,
                             trials=trials)

    if not found_chunk_directories:
        logger.debug("No chunk dir found, so using regular directory %s",
                     input_directory)

        group_trials(csvwriter,
                     crawl_group_name,
                     input_directory,
                     None,
                     file_suffix,
                     logger,
                     ground_truth_file=ground_truth_file,
                     trials=trials)

    output_file_opened.close()

    return output_file_path


def transfer_data_to_db(client,
                        port,
                        crawler_group_name,
                        crawl_data_output__webrequests_control,
                        crawl_data_output__webrequests_variant,
                        crawl_data_output__dom_control,
                        crawl_data_output__dom_variant,
                        mongodb_username=None,
                        mongodb_password=None):

    logger.debug("Transfering control webrequests crawl instances")
    # webrequests: transfer control data
    migrate_json_to_mongdo_webrequest(crawl_data_output__webrequests_control,
                                      JSON_WEBREQUEST_KEY,
                                      MONGODB_COLLECTION_WEBREQUESTS_CONTROL,
                                      crawler_group_name,
                                      "control",
                                      client,
                                      port,
                                      username=mongodb_username,
                                      password=mongodb_password)

    logger.debug("Transfering variant webrequests crawl instances")
    # webrequests: transfer variant data
    migrate_json_to_mongdo_webrequest(crawl_data_output__webrequests_variant,
                                      JSON_WEBREQUEST_KEY,
                                      MONGODB_COLLECTION_WEBREQUESTS_VARIANT,
                                      crawler_group_name,
                                      "variant",
                                      client,
                                      port,
                                      username=mongodb_username,
                                      password=mongodb_password)

    logger.debug("Transfering control dommutation crawl instances")
    # dommutation: transfer control data
    migrate_json_to_mongdo_dommutation(crawl_data_output__dom_control,
                                       JSON_DOMMUTATION_KEY,
                                       MONGODB_COLLECTION_DOMMUTATION_CONTROL,
                                       crawler_group_name,
                                       "control",
                                       client,
                                       port,
                                       username=mongodb_username,
                                       password=mongodb_password)

    logger.debug("Transfering variant dommutation crawl instances")
    # dommutation: transfer variant data
    migrate_json_to_mongdo_dommutation(crawl_data_output__dom_variant,
                                       JSON_DOMMUTATION_KEY,
                                       MONGODB_COLLECTION_DOMMUTATION_VARIANT,
                                       crawler_group_name,
                                       "variant",
                                       client,
                                       port,
                                       username=mongodb_username,
                                       password=mongodb_password)


def transfer_prep(output_directory,
                  crawler_group_name,
                  logger,
                  mongodb_client=MONGO_CLIENT_HOST,
                  mongodb_port=MONGO_CLIENT_PORT,
                  mongodb_username=None,
                  mongodb_password=None):

    found_chunks = False
    for root, dirs, _ in os.walk(output_directory):
        for directory in dirs:
            if "_to_" in directory:
                found_chunks = True
                chunk_name = directory
                output_directory_chunk = output_directory + os.sep + chunk_name
                logger.info("chunk directory found: %s",
                            output_directory_chunk)

                # Make crawl data folder using crawler_group_name
                crawl_data_output = output_directory_chunk + os.sep + "crawl_data_" + crawler_group_name + os.sep
                crawl_data_output__webrequests_control = crawl_data_output + "control_webrequests" + os.sep
                crawl_data_output__webrequests_variant = crawl_data_output + "variant_webrequests" + os.sep
                crawl_data_output__dom_control = crawl_data_output + "control_dommutation" + os.sep
                crawl_data_output__dom_variant = crawl_data_output + "variant_dommutation" + os.sep

                # Transfer data to DB
                transfer_data_to_db(mongodb_client,
                                    mongodb_port,
                                    crawler_group_name,
                                    crawl_data_output__webrequests_control,
                                    crawl_data_output__webrequests_variant,
                                    crawl_data_output__dom_control,
                                    crawl_data_output__dom_variant,
                                    mongodb_username=mongodb_username,
                                    mongodb_password=mongodb_password)

        break

    # if no chunks were found, then the output directory has the crawl data folder
    if not found_chunks:
        logger.info(
            "chunk directory NOT found, so relying on output directory: %s",
            output_directory)

        # Make crawl data folder using crawler_group_name
        crawl_data_output = output_directory + os.sep + "crawl_data_" + crawler_group_name + os.sep
        crawl_data_output__webrequests_control = crawl_data_output + "control_webrequests" + os.sep
        crawl_data_output__webrequests_variant = crawl_data_output + "variant_webrequests" + os.sep
        crawl_data_output__dom_control = crawl_data_output + "control_dommutation" + os.sep
        crawl_data_output__dom_variant = crawl_data_output + "variant_dommutation" + os.sep

        # Transfer data to DB
        transfer_data_to_db(mongodb_client,
                            mongodb_port,
                            crawler_group_name,
                            crawl_data_output__webrequests_control,
                            crawl_data_output__webrequests_variant,
                            crawl_data_output__dom_control,
                            crawl_data_output__dom_variant,
                            mongodb_username=mongodb_username,
                            mongodb_password=mongodb_password)


def diff_groups(client,
                port,
                crawler_group_name,
                csv_file_path,
                logger,
                mongodb_username=None,
                mongodb_password=None):

    rows = []
    with open(csv_file_path, 'r') as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')
        rows = list(reader)

    # find diff groups for web requests
    client, db = get_anticv_mongo_client_and_db(client,
                                                port,
                                                username=mongodb_username,
                                                password=mongodb_password)

    crawl_collection = db[MONGODB_COLLECTION_CRAWL_INSTANCE]

    crawl_instances_list_wr = get_crawl_groups_by_csv(rows,
                                                      JSON_WEBREQUEST_KEY,
                                                      crawler_group_name,
                                                      client, db,
                                                      crawl_collection)
    logger.debug("Total valid grouped web requests diff groups %d",
                 len(crawl_instances_list_wr))

    # find diff groups for dommutation
    crawl_instances_list_dom = get_crawl_groups_by_csv(rows,
                                                       JSON_DOMMUTATION_KEY,
                                                       crawler_group_name,
                                                       client, db,
                                                       crawl_collection)

    logger.debug("Total valid grouped dom mutation diff groups %d",
                 len(crawl_instances_list_dom))

    client.close()


class TimeseriesPrepThread(threading.Thread):
    def __init__(self,
                 threadID,
                 name,
                 rows,
                 output_directory,
                 output_csv_queue,
                 chunk_csv=None,
                 positive_label_domains=None,
                 negative_label_domains=None,
                 trials=4):

        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.rows = rows
        self.output_csv_queue = output_csv_queue
        self.chunk_csv = chunk_csv
        self.output_directory = output_directory
        self.positive_label_domains = positive_label_domains
        self.negative_label_domains = negative_label_domains
        self.trials=trials

    def run(self):
        logger.debug("Running thread %s", self.name)

        FILE_PATH_WR_CONTROL = "File Path WR Vanilla"
        FILE_PATH_WR_VARIANT = "File Path WR"
        FILE_PATH_DOM_CONTROL = "File Path DOM Vanilla"
        FILE_PATH_DOM_VARIANT = "File Path DOM"

        for row in self.rows:
            url = row[URL_CRAWLED]
            logger.info("%s - Processing URL %s", self.name, url)

            crawl_chunk = row["Chunk"]

            if self.chunk_csv and self.chunk_csv != crawl_chunk:
                continue

            cv_detect = row[CV_DETECT_TARGET_NAME]

            if self.positive_label_domains or self.negative_label_domains:
                if url not in self.positive_label_domains and url not in self.negative_label_domains:
                    continue

            ts_trials_json = dict()
            for trial_index in range(self.trials):
                trial_label = get_trial_label(trial_index)
                dom_control_name = FILE_PATH_DOM_CONTROL + " " + trial_label
                wr_control_name = FILE_PATH_WR_CONTROL + " " + trial_label

                dom_variant_name = FILE_PATH_DOM_VARIANT + " " + trial_label
                wr_variant_name = FILE_PATH_WR_VARIANT + " " + trial_label

                control_file_dom = row[dom_control_name] or ""
                control_file_wr = row[wr_control_name] or ""
                variant_file_dom = row[dom_variant_name] or ""
                variant_file_wr = row[wr_variant_name] or ""

                # if all file exists
                random_part = randomword(10)
                if len(control_file_dom) > 0 and len(control_file_wr) > 0 and \
                    len(variant_file_dom) > 0 and len(variant_file_wr) > 0:

                    try:

                        chunk_path = self.output_directory + os.sep + crawl_chunk
                        if not os.path.isdir(chunk_path):
                            os.mkdir(chunk_path)

                        control_prep = chunk_path + os.sep + random_part + trial_label + "_control"
                        prep_timeseries_file_json(control_file_dom,
                                                  control_file_wr,
                                                  control_prep)

                        variant_prep = chunk_path + os.sep + random_part + trial_label + "_variant"
                        prep_timeseries_file_json(variant_file_dom,
                                                  variant_file_wr,
                                                  variant_prep)

                        control_prep_csv = control_prep + ".csv"
                        variant_prep_csv = variant_prep + ".csv"

                        if os.path.isfile(control_prep_csv) and os.path.isfile(
                                variant_prep_csv):
                            logger.debug(
                                "%s - Success creating control+variant json for time series %s",
                                self.name, url)
                            ts_trials_json[trial_label] = (control_prep_csv,
                                                           variant_prep_csv)
                            time.sleep(2)
                    except Exception as e:
                        logger.error(e)
                        logger.warning(
                            "%s - Could not create plot for %s, exception plotting",
                            self.name, url + " " + str(trial_label))
                        ts_trials_json[trial_label] = (None, None)

                else:
                    ts_trials_json[trial_label] = (None, None)
                    logger.warning(
                        "%s - Could not create plot for %s, not enough files",
                        self.name, url + " " + str(trial_label))

            # write out rows
            csv_row = [url, crawl_chunk, cv_detect]
            for trial_index in range(self.trials):
                trial_label = get_trial_label(trial_index)
                ctr_prep, var_prep = ts_trials_json.get(trial_label)
                csv_row.append(ctr_prep if ctr_prep else "")
                csv_row.append(var_prep if var_prep else "")

            # add to queue
            self.output_csv_queue.put(csv_row)


def prep_timeseries_file_json(dom_file_path, wr_file_path, output_file_name):
    auto_determine_time = False
    auto_bin_time_series_csv(dom_file_path,
                             wr_file_path,
                             output_file_name,
                             100,
                             max_seconds_later=25,
                             auto_determine_time=auto_determine_time)


def plot_time_series_process(process_index,
                             all_rows,
                             output_csv_queue,
                             output_directory,
                             positive_label_domains=None,
                             negative_label_domains=None,
                             chunk_csv=None,
                             trials=4,
                             thread_limit=5,
                             chunk_size=50):

    logger.debug("Starting process " + str(process_index))

    # chunking
    THREADS_LIMIT = thread_limit  # how many threads can run each thread
    chunks = chunk(all_rows, n=chunk_size)

    chunk_count = len(chunks)
    current_threads = []
    chunk_index = 0
    chunk_completed = 0
    logger.debug("Processing chunk %d out of %d",
                 chunk_index + 1, chunk_count)

    stop = False
    while chunk_completed < chunk_count and not stop:
        if len(current_threads) < THREADS_LIMIT and chunk_index < chunk_count:
            thread_name = "Thread-" + randomword(5)
            logger.debug("Creating " + thread_name)

            some_thread = TimeseriesPrepThread(
                chunk_index,
                thread_name,
                chunks[chunk_index],
                output_directory,
                output_csv_queue,
                chunk_csv=chunk_csv,
                positive_label_domains=positive_label_domains,
                negative_label_domains=negative_label_domains,
                trials=trials)

            # Start new Threads
            some_thread.start()
            logger.debug("Processing chunk %d out of %d with thread %s",
                         chunk_index + 1, chunk_count, some_thread.name)
            current_threads.append(some_thread)
            chunk_index += 1
            time.sleep(1)
        else:
            done_threads = []
            for t in current_threads:
                if not t.is_alive():
                    done_threads.append(t)

            if len(done_threads) == 0:
                logger.debug("Found no done threads")
                time.sleep(2)
            else:
                for done_thread in done_threads:
                    logger.debug(
                        "Done with thread %s and chunk index %d out of %d",
                        done_thread.name, done_thread.threadID + 1,
                         chunk_count)
                    chunk_completed += 1
                    current_threads.remove(done_thread)
                time.sleep(2)


def create_time_series_csvs(csv_file_path,
                            output_directory,
                            ground_truth_file,
                            chunk_csv=None,
                            thread_limit=20,
                            chunk_size=50,
                            csv_file_existing=None,
                            trials=4):

    output_name_mapping = output_directory + os.sep + "filename_mapping.csv"

    positive_label_domains = None
    negative_label_domains = None

    if ground_truth_file:
        positive_label_domains, negative_label_domains = get_ground_truth(
            ground_truth_file)

    if not os.path.isdir(output_directory + os.sep):
        os.makedirs(output_directory + os.sep, exist_ok=True)

    # get already done urls
    already_done = []
    if csv_file_existing:
        with open(csv_file_existing, 'r') as already_done_file:
            already_done_reader = csv.DictReader(already_done_file,
                                                 delimiter=',')
            for row in already_done_reader:
                already_done.append(row[URL_CRAWLED])

    logger.debug("Number of already done %d", len(already_done))
    logger.debug("Number of already done unique %d", len(set(already_done)))

    header_row = ["URL Crawled", "Chunk", CV_DETECT_TARGET_NAME]
    for trial_index in range(trials):
        trial_label = get_trial_label(trial_index)
        header_row.append(CONTROL + " " + trial_label)
        header_row.append(VARIANT + " " + trial_label)

    # start process to write out csv
    output_csv_queue = Queue()
    output_csv_shutdown_event = Event()
    output_csv_process = OutputCSVProcess("1",
                                          "output_csv",
                                          output_name_mapping,
                                          output_csv_shutdown_event,
                                          output_csv_queue,
                                          header_row=header_row)
    output_csv_process.start()

    ts_to_process_list = []
    with open(csv_file_path) as csv_file:
        reader = csv.DictReader(csv_file, delimiter=',')

        for row in reader:
            if csv_file_existing and len(already_done) > 0:
                if row[URL_CRAWLED] in already_done:
                    continue
                else:
                    logger.debug("Still have no done this: %s",
                                 row[URL_CRAWLED])
                if chunk_csv and row["Chunk"] != chunk_csv:
                    logger.debug(
                        "Not doing because chunk does not match: %s, chunk %s != %s",
                        row[URL_CRAWLED], chunk_csv, row["Chunk"])
                    continue

            ts_to_process_list.append(row)

        logger.debug("Number of rows to process %d", len(ts_to_process_list))

    ts_to_process_list_count = len(ts_to_process_list)
    process_limit = 10
    process_chunk_size = int(ts_to_process_list_count / process_limit) + 1
    process_chunks = chunk(ts_to_process_list, n=process_chunk_size)

    # verify the number is the same
    total_count = 0
    for chunk_tmp in process_chunks:
        total_count += len(chunk_tmp)

    assert (
        total_count == ts_to_process_list_count
    ), "Process chunks total count not equal to original size of list: " + str(
        total_count) + ", " + str(ts_to_process_list_count)

    process_list = []
    for process_index in range(0, len(process_chunks)):
        process_chunk_tmp = process_chunks[process_index]

        # first process gets to write the header
        csv_has_header = True
        if process_index == 0:
            csv_has_header = False

        p = Process(target=plot_time_series_process,
                    args=(process_index, process_chunk_tmp, output_csv_queue,
                          output_directory, positive_label_domains,
                          negative_label_domains, chunk_csv, trials))
        p.start()

        process_list.append(p)

    # wait for all to be done
    for p in process_list:
        p.join()

    logger.debug("All work process are done")

    time.sleep(10)
    logger.debug("Cleaning up all output processes")
    output_csv_shutdown_event.set()
    logger.debug("Waiting for output csv processes to complete")
    output_csv_process.join()
    logger.info("DONE")
