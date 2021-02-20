#!/usr/bin/python

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
import logging
import os
import pickle
import threading
import time
from multiprocessing import Event, Process, Queue

import pandas as pd
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler, QuantileTransformer, PowerTransformer

from cvinspector.common.utils import chunk, randomword, get_anticv_client_and_db, get_by_crawl_group_name, \
    MONGODB_WR_DIFF_GROUP, \
    MONGODB_DOM_DIFF_GROUP, get_ground_truth, OutputCSVProcess, \
    OutputDebugProcess, OutputCSVForceHeaderProcess, MONGODB_COLLECTION_CRAWL_INSTANCE, CONTROL, \
    VARIANT, TRIAL_PREFIX
from cvinspector.common.webrequests_utils import extract_tld, find_all_first_and_third_party_webrequests, \
    get_second_level_domain_from_tld
from cvinspector.data_migrate.utils import get_anticv_mongo_client_and_db
from cvinspector.diff_analysis.dommutation_core import get_dom_differences_only
from cvinspector.diff_analysis.webrequests_core import get_wr_differences_only
from cvinspector.ml.feature_constants import BOOLEAN_FEATURES, CRAWL_URL_COLUMN_NAME, TARGET_COLUMN_NAME
from cvinspector.ml.feature_extraction import WebRequestsFeatureExtraction, DOMMutationFeatureExtraction, \
    TimeSeriesDOMFeatureExtraction, PageSourceFeatureNewExtraction, \
    PageSourceCorrespFeatureNewExtraction

logger = logging.getLogger(__name__)
# logger.setLevel("DEBUG")

CV_DETECTION_COLLECTION = "cv_detection"
DEFAULT_CSV_FILE_NAME = "cv_features.csv"
# values for this is [0,1]
CV_DETECT_TARGET_NAME = "cv_detect"
CRAWL_URL = "crawl_url"

RAW_UNLABEL_FILE_KEY = "raw_unlabel"


def filter_features_exclusion(feature_names,
                              features,
                              exclusion=[],
                              exclusion_contains=[]):
    new_feature_names = []
    new_features = []

    if not exclusion or len(exclusion) == 0:
        return feature_names, features

    for feature_name, feature_value in zip(feature_names, features):
        if feature_name not in exclusion:
            if len(exclusion_contains) == 0:
                new_feature_names.append(feature_name)
                new_features.append(feature_value)
            else:
                found = False
                for substr_excl in exclusion_contains:
                    if substr_excl in feature_name:
                        found = True
                if not found:
                    new_feature_names.append(feature_name)
                    new_features.append(feature_value)

    return new_feature_names, new_features


def get_variant_features_only(feature_names, features):
    new_feature_names = []
    new_features = []

    for feature_name, feature_value in zip(feature_names, features):
        if feature_name.startswith("var_") or feature_name.startswith(
                "variant"):
            new_feature_names.append(feature_name)
            new_features.append(feature_value)

    return new_feature_names, new_features


def write_main_row(wr_sorted_keys,
                   wr_features_vector,
                   pgsource_sorted_keys,
                   pgsource_features_vector,
                   dommutation_sorted_keys,
                   dommutation_features_vector,
                   ts_sorted_keys,
                   ts_feature_extractor,
                   csv_queue,
                   crawl_url,
                   cv_detect,
                   csv_has_header,
                   include_control=False):
    if not include_control:
        wr_sorted_keys, wr_features_vector = get_variant_features_only(
            wr_sorted_keys, wr_features_vector)

    pgsource_sorted_keys, pgsource_features_vector = filter_features_exclusion(
        pgsource_sorted_keys,
        pgsource_features_vector,
        exclusion=[
            "pgsource_key_DEPTH_STATS_mean",
            "pgsource_key_DEPTH_STATS_variance", "pgsource_value_VALUE",
            "pgsource_value_EXTRA", "pgsource_value_VALUE_DEPTH_STATS_mean",
            "pgsource_value_ATTRIBUTE",
            "pgsource_value_EXTRA_DEPTH_STATS_variance",
            "pgsource_value_EXTRA_DEPTH_STATS_mean", "pgsource_key_IFRAME"
        ],
        exclusion_contains=[
            "max_val",
            "EXTRA",
        ])

    dommutation_sorted_keys, dommutation_features_vector = filter_features_exclusion(
        dommutation_sorted_keys,
        dommutation_features_vector,
        exclusion=[
            "dom_attribute_changed__src-set",
            "dom_node_changed_added__ad",
            "dom_node_changed_added__audio",
            "dom_node_changed_added__body",
            "dom_node_changed_added__header",
            "dom_node_changed_added__video",
            "dom_node_changed_removed__ad",
            "dom_node_changed_removed__audio",
            "dom_node_changed_removed__body",
            "dom_node_changed_removed__header",
            "dom_node_changed_removed__video",
        ])

    # concentenate the sort keys and features
    sorted_keys = wr_sorted_keys + pgsource_sorted_keys + dommutation_sorted_keys + ts_sorted_keys
    features_vector = wr_features_vector + pgsource_features_vector + dommutation_features_vector + ts_feature_extractor

    # add the crawl url as first feature
    crawl_url = crawl_url.replace(",", "__")
    features_vector = [crawl_url] + features_vector + [cv_detect]

    # write out the header if needed
    if not csv_has_header:
        header = sorted_keys
        header = [CRAWL_URL] + header + [CV_DETECT_TARGET_NAME]
        csv_queue.put(header)
        csv_has_header = True

    # write out the row of data
    csv_queue.put(features_vector)

    return csv_has_header


class WriteURLSThread(threading.Thread):
    def __init__(self,
                 threadID,
                 name,
                 crawl_group_name,
                 diff_groups_wr,
                 output_queue,
                 control_output_queue,
                 mongodb_client,
                 mongodb_port,
                 trials_queue,
                 username=None,
                 password=None,
                 positive_label_domains=None,
                 negative_label_domains=None,
                 adblock_parser=None):

        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.crawl_group_name = crawl_group_name
        self.diff_groups_wr = diff_groups_wr
        self.output_queue = output_queue
        self.control_output_queue = control_output_queue
        self.positive_label_domains = positive_label_domains
        self.negative_label_domains = negative_label_domains
        self.mongodb_client = mongodb_client
        self.mongodb_port = mongodb_port
        self.username = username
        self.password = password
        self.adblock_parser = adblock_parser
        self.trials_queue = trials_queue

    def run_per_diff_group(self, diff_group_wr, crawl_collection, db):
        crawl_url = diff_group_wr.get("url")
        logger.debug("Processing diff group with crawl group name %s, url %s " %
                     (self.crawl_group_name, crawl_url))

        control_only_docs, variant_only_docs = get_wr_differences_only(
            diff_group_wr,
            self.crawl_group_name,
            crawl_collection,
            thread_name=self.name,
            debug_collect_urls=True)

        trials_row = [crawl_url]
        for control_urls in control_only_docs.get("trials_urls_diff"):
            trials_row.append(control_urls)
        for control_urls in control_only_docs.get("trials_domain_diff"):
            trials_row.append(control_urls)
        for variant_urls in variant_only_docs.get("trials_urls_diff"):
            trials_row.append(variant_urls)
        for variant_urls in variant_only_docs.get("trials_domain_diff"):
            trials_row.append(variant_urls)

        self.trials_queue.put(trials_row)

        # print(variant_only_docs)
        wr_feature_extractor = WebRequestsFeatureExtraction(
            crawl_url,
            control_only_docs.get("urls"),
            variant_only_docs.get("urls"),
            blocked_requests=variant_only_docs.get("tracker_blocked"),
            content_type_resources=variant_only_docs.get("content_types"),
            resource_type_resources=variant_only_docs.get("misc_types"),
            mismatch_resources=variant_only_docs.get("mismatch_resources"),
            var_diff_obj=variant_only_docs,
            ctr_blocked_requests=control_only_docs.get("tracker_blocked"),
            ctr_content_type_resources=control_only_docs.get("content_types"),
            ctr_resource_type_resources=control_only_docs.get("misc_types"),
            ctr_mismatch_resources=control_only_docs.get("mismatch_resources"),
            ctr_diff_obj=control_only_docs,
            log_prefix=self.name,
            urls_collector_queue=self.output_queue,
            urls_collector_queue_control=self.control_output_queue,
            adblock_parser=self.adblock_parser)

        wr_sorted_keys, wr_features_vector = wr_feature_extractor.extract_features_vector(
        )

    def run(self):
        client, db = get_anticv_mongo_client_and_db(self.mongodb_client,
                                                    self.mongodb_port,
                                                    username=self.username,
                                                    password=self.password)

        crawl_collection = db[MONGODB_COLLECTION_CRAWL_INSTANCE]

        for index, diff_group_wr in enumerate(self.diff_groups_wr, start=0):
            self.run_per_diff_group(diff_group_wr, crawl_collection, db)


class WriteFeatureCSVThread(threading.Thread):
    def __init__(self,
                 threadID,
                 name,
                 crawl_group_name,
                 diff_groups_wr,
                 time_series_dict,
                 feature_csv_queue,
                 debug_logger_queue,
                 debug_diff_queue,
                 mongodb_client,
                 mongodb_port,
                 tracking_dict,
                 username=None,
                 password=None,
                 positive_label_domains=None,
                 negative_label_domains=None,
                 img_dimension_dict=None,
                 include_control=False,
                 csv_has_header=False,
                 adblock_parser=None,
                 output_external_logs=True,
                 trials=4):

        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.crawl_group_name = crawl_group_name
        self.diff_groups_wr = diff_groups_wr
        self.time_series_dict = time_series_dict
        self.feature_csv_queue = feature_csv_queue
        self.debug_logger_queue = debug_logger_queue
        self.debug_diff_queue = debug_diff_queue
        self.positive_label_domains = positive_label_domains
        self.negative_label_domains = negative_label_domains
        self.mongodb_client = mongodb_client
        self.mongodb_port = mongodb_port
        self.username = username
        self.password = password
        self.include_control = include_control
        self.csv_has_header = csv_has_header
        self.tracking_dict = tracking_dict
        self.adblock_parser = adblock_parser
        self.output_external_logs = output_external_logs
        self.img_dimension_dict = img_dimension_dict
        self.trials = trials

    def run_per_diff_group(self,
                           diff_group_wr,
                           crawl_collection,
                           db,
                           already_has_header=True):
        crawl_url = diff_group_wr.get("url")
        logger.debug("Processing diff group with crawl group name %s, url %s " %
                     (self.crawl_group_name, crawl_url))

        do_wr = True
        do_dom = True
        do_pagesource = True
        do_pagesource_corres = True
        do_timeseries = True

        # webrequest  feature extraction
        wr_sorted_keys = []
        wr_features_vector = []
        wr_feature_extractor = None
        pgsource_sorted_keys = []
        pgsource_features_vector = []
        dommutation_sorted_keys = []
        dommutation_features_vector = []
        ts_sorted_keys = []
        ts_features_vector = []

        variant_only_docs = dict()

        if (do_pagesource and not do_wr) or (do_pagesource_corres
                                             and not do_wr):
            logger.error(
                "Warning!!! You must do both Pagesource and WR together for blocked urls dependency"
            )

        original_time = time.time()
        if do_wr:
            start_time = time.time()

            control_only_docs, variant_only_docs = get_wr_differences_only(
                diff_group_wr,
                self.crawl_group_name,
                crawl_collection,
                self.debug_diff_queue,
                thread_name=self.name,
                output_external_logs=self.output_external_logs)

            logger.debug("%s - WR DIFFING TIME --- %s seconds --- %s" %
                         (self.name, time.time() - start_time, crawl_url))

            _, first_party_requests, third_party_requests = find_all_first_and_third_party_webrequests(
                crawl_url,
                variant_only_docs.get("urls") or [])

            start_time = time.time()

            wr_feature_extractor = WebRequestsFeatureExtraction(
                crawl_url,
                control_only_docs.get("urls"),
                variant_only_docs.get("urls"),
                blocked_requests=variant_only_docs.get("tracker_blocked"),
                content_type_resources=variant_only_docs.get("content_types"),
                resource_type_resources=variant_only_docs.get("misc_types"),
                mismatch_resources=variant_only_docs.get("mismatch_resources"),
                var_diff_obj=variant_only_docs,
                ctr_blocked_requests=control_only_docs.get("tracker_blocked"),
                ctr_content_type_resources=control_only_docs.get(
                    "content_types"),
                ctr_resource_type_resources=control_only_docs.get(
                    "misc_types"),
                ctr_mismatch_resources=control_only_docs.get(
                    "mismatch_resources"),
                ctr_diff_obj=control_only_docs,
                log_prefix=self.name,
                adblock_parser=self.adblock_parser,
                tracking_dict=self.tracking_dict,
                trials=self.trials)

            wr_sorted_keys, wr_features_vector = wr_feature_extractor.extract_features_vector(
            )

            logger.info("%s - DONE: Extraction Type %s - URL %s\n",
                        str(self.name),
                        wr_feature_extractor.__class__.__name__, crawl_url)

            logger.debug(
                "%s - WR FEATURE EXTRACTION TIME --- %s seconds --- %s",
                self.name, time.time() - start_time, crawl_url)

        # pagesource feature extraction
        if do_pagesource and do_wr:
            start_time = time.time()

            pgsource_feature_extractor = PageSourceFeatureNewExtraction(
                self.crawl_group_name,
                crawl_url,
                diff_group_wr,
                crawl_collection,
                self.debug_diff_queue,
                self.name,
                output_external_logs=self.output_external_logs,
                variant_blocked_urls_by_trial=variant_only_docs.get(
                    "variant_blocked_urls_by_trial"),
                img_dimension_dict=self.img_dimension_dict,
                trials=self.trials)
            pgsource_sorted_keys, pgsource_features_vector = pgsource_feature_extractor.extract_features_vector(
            )

            print("DONE: Extraction Type %s - URL %s" %
                  (pgsource_feature_extractor.__class__.__name__, crawl_url))

            logger.debug(
                "%s - PAGESOURCE EXTRACTION TIME --- %s seconds --- %s" %
                (self.name, time.time() - start_time, crawl_url))

        # pagesource feature extraction
        if do_pagesource_corres and do_wr:
            start_time = time.time()

            pgsource_feature_extractor = PageSourceCorrespFeatureNewExtraction(
                self.crawl_group_name,
                crawl_url,
                diff_group_wr,
                crawl_collection,
                self.debug_diff_queue,
                self.name,
                output_external_logs=self.output_external_logs,
                variant_blocked_urls_by_trial=variant_only_docs.get(
                    "variant_blocked_urls_by_trial"),
                img_dimension_dict=self.img_dimension_dict,
                trials=self.trials)
            pgsource_corr_sorted_keys, pgsource_corr_features_vector = pgsource_feature_extractor.extract_features_vector(
            )
            pgsource_sorted_keys += pgsource_corr_sorted_keys
            pgsource_features_vector += pgsource_corr_features_vector

            print("DONE: Extraction Type %s - URL %s" %
                  (pgsource_feature_extractor.__class__.__name__, crawl_url))

            logger.debug(
                "%s - PAGESOURCE CORRES EXTRACTION TIME --- %s seconds --- %s"
                % (self.name, time.time() - start_time, crawl_url))

        # dommutation feature extraction
        if do_dom:
            start_time = time.time()
            dom_diff_group = get_by_crawl_group_name(self.crawl_group_name,
                                                     db,
                                                     MONGODB_DOM_DIFF_GROUP,
                                                     find_one=True,
                                                     url=crawl_url,
                                                     discard="false")
            logger.debug(
                "%s - GET DOM DIFF GROUP (MongoDB) TIME --- %s seconds --- %s"
                % (self.name, time.time() - start_time, crawl_url))

            if not dom_diff_group:
                logger.warning("No DOM diff group, skipping %s", crawl_url)
                # sys.exit(1)
                return

            logger.debug("Found corresponding dom mutation crawl_instance %s",
                         dom_diff_group.get("_id"))

            start_time = time.time()
            _, variant_dommutation_doc = get_dom_differences_only(
                dom_diff_group,
                self.crawl_group_name,
                crawl_collection,
                self.debug_diff_queue,
                thread_name=self.name,
                output_external_logs=self.output_external_logs)
            logger.debug("%s - DOM DIFFING TIME --- %s seconds --- %s" %
                         (self.name, time.time() - start_time, crawl_url))

            start_time = time.time()
            dommutation_feature_extractor = DOMMutationFeatureExtraction(
                crawl_url, variant_dommutation_doc, log_prefix=self.name, trials=self.trials)

            dommutation_sorted_keys, dommutation_features_vector = dommutation_feature_extractor.extract_features_vector(
            )

            logger.info(
                "%s - DONE: Extraction Type %s - URL %s",
                str(self.name),
                dommutation_feature_extractor.__class__.__name__, crawl_url)

            logger.debug(
                "%s - DOM FEATURES EXTRACTION TIME --- %s seconds --- %s",
                self.name, time.time() - start_time, crawl_url)

        # timeseries feature extraction
        if do_timeseries:
            if self.time_series_dict is not None and self.time_series_dict.get(
                    crawl_url) is not None:
                start_time = time.time()

                ts_feature_extractor = TimeSeriesDOMFeatureExtraction(
                    crawl_url, self.time_series_dict.get(crawl_url), trials=self.trials)

                ts_sorted_keys, ts_features_vector = ts_feature_extractor.extract_features_vector(
                )
                if len(ts_sorted_keys) == 0:
                    logger.warn("%s - Skipping row %s because of ts " %
                                (str(self.name), crawl_url))
                    return

                logger.info(
                    "%s - DONE: Extraction Type %s - URL %s\n",
                    str(self.name), ts_feature_extractor.__class__.__name__,
                    crawl_url)
                logger.debug(
                    "%s - TIME SERIES FEATURES EXTRACTION TIME --- %s seconds --- %s",
                    self.name, time.time() - start_time, crawl_url)

            else:
                logger.warn("%s - Skipping row %s because of ts " %
                            (str(self.name), crawl_url))
                logger.warning("%s - Missing time series, %s" %
                               (str(self.name), crawl_url))
                return

        # find out if we know whether this domain is positive label
        crawl_domain = crawl_url  # get_domain_only_from_url(crawl_url)

        ## crawl domain now is the whole url
        # crawl_domain = crawl_domain.replace("www.", "")
        logger.debug("%s - Crawl Domain %s", str(self.name), crawl_domain)

        cv_detect = -1
        if self.positive_label_domains is not None and crawl_domain in self.positive_label_domains:
            cv_detect = 1
            # print("Found positive label for %s" % crawl_domain)
        elif self.negative_label_domains is not None and crawl_domain in self.negative_label_domains:
            cv_detect = 0
            # print("Found negative label for %s" % crawl_domain)

        # write row that has almost everything
        already_has_header = write_main_row(
            wr_sorted_keys,
            wr_features_vector,
            pgsource_sorted_keys,
            pgsource_features_vector,
            dommutation_sorted_keys,
            dommutation_features_vector,
            ts_sorted_keys,
            ts_features_vector,
            self.feature_csv_queue,
            crawl_url,
            cv_detect,
            already_has_header,
            include_control=self.include_control)

        # this file is for debugging purpose to manually check the features we used to do decisions later on
        if self.debug_logger_queue and self.output_external_logs and wr_feature_extractor:
            start_time = time.time()

            url_variant_line = crawl_url
            self.debug_logger_queue.put(
                str(self.name) + " Crawled url: " + url_variant_line + "\n")
            if do_wr:
                self.debug_logger_queue.put(
                    str(self.name) + " control orig: " +
                    str(wr_feature_extractor.control_requests_orig) + "\n")
                self.debug_logger_queue.put(
                    str(self.name) + " variant orig: " +
                    str(wr_feature_extractor.variant_requests_orig) + "\n")
                self.debug_logger_queue.put(
                    str(self.name) + " control fp: " +
                    str(wr_feature_extractor.control_first_party_requests) +
                    "\n")
                self.debug_logger_queue.put(
                    str(self.name) + " control tp: " +
                    str(wr_feature_extractor.control_third_party_requests) +
                    "\n")
                self.debug_logger_queue.put(
                    str(self.name) + " variant fp: " +
                    str(wr_feature_extractor.variant_first_party_requests) +
                    "\n")
                self.debug_logger_queue.put(
                    str(self.name) + " variant tp: " +
                    str(wr_feature_extractor.variant_third_party_requests) +
                    "\n")
                self.debug_logger_queue.put(
                    str(self.name) + " ====Extractor type %s Logs ===\n" %
                    wr_feature_extractor.__class__.__name__)
                self.debug_logger_queue.put(
                    str(self.name) + " " +
                    "\n".join(wr_feature_extractor.logs) + "\n")
            if do_dom:
                self.debug_logger_queue.put(
                    str(self.name) + " ====Extractor type %s Logs ===\n" %
                    dommutation_feature_extractor.__class__.__name__)
                self.debug_logger_queue.put(
                    str(self.name) + " " +
                    "\n".join(dommutation_feature_extractor.logs) + "\n")
            self.debug_logger_queue.put(str(self.name) + " ****DONE****")

            # url_variant_file.write("pgsource diff: " + str(pgsource_feature_extractor.variant_only_docs) + "\n")
            logger.debug(
                "%s - WRITING OUT DEBUG LOG TIME --- %s seconds --- %s" %
                (self.name, time.time() - start_time, crawl_url))

        logger.info("\nDONE: Extracting Features for URL %s", crawl_url)
        if self.debug_diff_queue and self.output_external_logs:
            self.debug_logger_queue.put(
                "\nDONE: Extracting Features for URL %s" % (crawl_url))

        logger.debug(
            "%s - ENTIRE FEATURE EXTRACTION TIME --- %s seconds --- %s",
            self.name, time.time() - original_time, crawl_url)

    def run(self):
        client, db = get_anticv_mongo_client_and_db(self.mongodb_client,
                                                    self.mongodb_port,
                                                    username=self.username,
                                                    password=self.password)

        crawl_collection = db[MONGODB_COLLECTION_CRAWL_INSTANCE]

        for index, diff_group_wr in enumerate(self.diff_groups_wr, start=0):
            if index == 0:
                self.run_per_diff_group(diff_group_wr,
                                        crawl_collection,
                                        db,
                                        already_has_header=self.csv_has_header)
            else:
                self.run_per_diff_group(diff_group_wr, crawl_collection, db)


def _write_feature_csv__process(process_index,
                                crawl_group_name,
                                mongodb_client,
                                mongodb_port,
                                diff_groups_wr,
                                time_series_dict,
                                features_queue,
                                features_debug,
                                diff_debug,
                                tracking_file_path,
                                positive_label_domains=None,
                                negative_label_domains=None,
                                csv_has_header=True,
                                output_external_logs=True,
                                thread_limit=20,
                                chunk_size=30,
                                img_dimension_file_path=None,
                                trials=4):
    logger.debug("Starting process " + str(process_index))

    # chunking
    THREADS_LIMIT = thread_limit  # how many threads can run each thread
    chunks = chunk(diff_groups_wr, n=chunk_size)

    chunk_count = len(chunks)
    current_threads = []
    chunk_index = 0
    chunk_completed = 0
    logger.debug("Processing chunk %d out of %d" %
                 (chunk_index + 1, chunk_count))

    # parser for removing noise

    # read in the tracking file
    logger.debug("reading in tracking file %s" % tracking_file_path)
    tracking_dict = dict()
    tracking_delimiter = ";;"
    with open(tracking_file_path, "r") as tracking_file:
        # each line is: main domain, url, resource type
        for line in tracking_file:
            line_split = line.strip().split(tracking_delimiter)
            if len(line_split) == 3:
                host_page = line_split[0]
                tracking_url = line_split[1]
                tracking_resource = line_split[2]
                if host_page not in tracking_dict:
                    tracking_dict[host_page] = dict()
                if tracking_url not in tracking_dict[host_page]:
                    tracking_dict[host_page][tracking_url] = tracking_resource
    logger.debug("done reading in tracking file %s" % tracking_file_path)

    # read in image dimension file
    img_dimension_dict = None
    if img_dimension_file_path and os.path.isfile(img_dimension_file_path):
        logger.debug("reading in image dimension file %s" %
                     img_dimension_file_path)
        img_dimension_dict = dict()
        img_dimension_delimiter = ";;"
        with open(img_dimension_file_path, "r") as img_dim_file:
            for line in img_dim_file:
                line_split = line.strip().split(img_dimension_delimiter)
                # remove protocol
                img_url = line_split[0].replace("https",
                                                "").replace("http", "")
                # make into float then int because some dimensions are floats
                img_width = int(float(line_split[1]))
                img_height = int(float(line_split[2]))
                img_dimension_dict[img_url] = (img_width, img_height)
        logger.debug("done reading in image dimension file %s" %
                     img_dimension_file_path)
        logger.debug("image dimension file found urls: %d" %
                     len(img_dimension_dict))

    stop = False
    while chunk_completed < chunk_count and not stop:
        if len(current_threads) < THREADS_LIMIT and chunk_index < chunk_count:
            thread_name = "Thread-" + randomword(5)
            logger.debug("Creating " + thread_name)

            # we only allow the header to be written once, then we reset it to True
            if chunk_index == 0 and not csv_has_header:
                csv_has_header = False
            else:
                csv_has_header = True

            some_thread = WriteFeatureCSVThread(
                chunk_index,
                thread_name,
                crawl_group_name,
                chunks[chunk_index],
                time_series_dict,
                features_queue,
                features_debug,
                diff_debug,
                mongodb_client,
                mongodb_port,
                tracking_dict,
                output_external_logs=output_external_logs,
                csv_has_header=csv_has_header,
                positive_label_domains=positive_label_domains,
                negative_label_domains=negative_label_domains,
                img_dimension_dict=img_dimension_dict,
                trials=trials)

            # Start new Threads
            some_thread.start()
            logger.debug("Processing chunk %d out of %d with thread %s" %
                         (chunk_index + 1, chunk_count, some_thread.name))
            current_threads.append(some_thread)
            chunk_index += 1
            time.sleep(1)
        else:
            done_threads = []
            for t in current_threads:
                if not t.is_alive():
                    done_threads.append(t)

            if len(done_threads) == 0:
                logger.debug(
                    "Process %d : Found no done threads, still alive %d" %
                    (process_index, len(current_threads)))
                time.sleep(10)
            else:
                for done_thread in done_threads:
                    logger.debug(
                        "Done with thread %s and chunk index %d out of %d" %
                        (done_thread.name, done_thread.threadID + 1,
                         chunk_count))
                    chunk_completed += 1
                    current_threads.remove(done_thread)
                time.sleep(2)

    features_debug.put("Done with process " + str(process_index))


def _write_urls_csv__process(process_index,
                             crawl_group_name,
                             mongodb_client,
                             mongodb_port,
                             diff_groups_wr,
                             output_queue,
                             output_queue_control,
                             trials_queue,
                             positive_label_domains=None,
                             negative_label_domains=None,
                             thread_limit=40,
                             chunk_size=100):
    logger.debug("Starting process " + str(process_index))

    # chunking
    THREADS_LIMIT = thread_limit  # how many threads can run each thread
    chunks = chunk(diff_groups_wr, n=chunk_size)

    chunk_count = len(chunks)
    current_threads = []
    chunk_index = 0
    chunk_completed = 0
    logger.debug("Processing chunk %d out of %d" %
                 (chunk_index + 1, chunk_count))

    stop = False
    while chunk_completed < chunk_count and not stop:
        if len(current_threads) < THREADS_LIMIT and chunk_index < chunk_count:
            thread_name = "Thread-" + randomword(5)
            logger.debug("Creating " + thread_name)

            some_thread = WriteURLSThread(
                chunk_index,
                thread_name,
                crawl_group_name,
                chunks[chunk_index],
                output_queue,
                output_queue_control,
                mongodb_client,
                mongodb_port,
                trials_queue,
                positive_label_domains=positive_label_domains,
                negative_label_domains=negative_label_domains,
            )

            # Start new Threads
            some_thread.start()
            logger.debug("Processing chunk %d out of %d with thread %s" %
                         (chunk_index + 1, chunk_count, some_thread.name))
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
                        "Done with thread %s and chunk index %d out of %d" %
                        (done_thread.name, done_thread.threadID + 1,
                         chunk_count))
                    chunk_completed += 1
                    current_threads.remove(done_thread)
                time.sleep(2)

    logger.debug("Done with process " + str(process_index))


def write_feature_csv(crawl_group_name,
                      mongodb_client,
                      mongodb_port,
                      tracking_file_path,
                      ground_truth_file_path=None,
                      csv_file_name=DEFAULT_CSV_FILE_NAME,
                      output_directory=None,
                      ground_truth_only=True,
                      time_series_mapping=None,
                      output_external_logs=True,
                      include_control=False,
                      existing_file=None,
                      img_dimension_file_path=None,
                      rank_file=None,
                      rank_start=None,
                      rank_end=None,
                      trials=4):
    # read in file with domains labeled as positives
    # if line starts with ! , then it means it is negative label

    positive_label_domains = None
    negative_label_domains = None
    if ground_truth_file_path:
        positive_label_domains, negative_label_domains = get_ground_truth(
            ground_truth_file_path)

    client, db = get_anticv_client_and_db()

    # We create multiple csvs depending on split_by_party
    raw_features_file_name = output_directory + os.sep + csv_file_name + ".csv"
    features_queue = Queue()
    features_shutdown_event = Event()
    features_process = OutputCSVForceHeaderProcess("1", "main_features_csv",
                                                   raw_features_file_name,
                                                   features_shutdown_event,
                                                   features_queue, "crawl_url")
    features_process.start()

    # file for debug information (features extraction)
    features_debug_file_name = output_directory + os.sep + csv_file_name + "__debug.txt"
    features_debug = Queue()
    features_debug_shutdown_event = Event()
    features_debug_process = OutputDebugProcess("1", "main_features_debug",
                                                features_debug_file_name,
                                                features_debug_shutdown_event,
                                                features_debug)
    features_debug_process.start()

    # file for debug information (diff extraction)
    diff_debug_file_name = output_directory + os.sep + csv_file_name + "__diff_analysis_debug.txt"
    diff_debug = Queue()
    diff_debug_shutdown_event = Event()
    diff_debug_process = OutputDebugProcess("1", "main_diff_debug",
                                            diff_debug_file_name,
                                            diff_debug_shutdown_event,
                                            diff_debug)
    diff_debug_process.start()

    # file for urls not found from ground truth
    url_not_found_file_name = output_directory + os.sep + csv_file_name + "__urls_not_found.txt"
    url_not_found_queue = Queue()
    url_not_found_shutdown_event = Event()
    url_not_found_process = OutputDebugProcess("1", "url_not_found",
                                               url_not_found_file_name,
                                               url_not_found_shutdown_event,
                                               url_not_found_queue)
    url_not_found_process.start()

    # these diff groups are only for webrequests
    wr_crawl_diff_groups = get_by_crawl_group_name(crawl_group_name,
                                                   db,
                                                   MONGODB_WR_DIFF_GROUP,
                                                   discard="false")
    logger.debug("WR DIFF GROUPS FOUND %d" % wr_crawl_diff_groups.count())

    wr_crawl_diff_groups_list = []
    urls_found = []
    if ground_truth_only and (positive_label_domains
                              and len(positive_label_domains) > 0) or (
            negative_label_domains
            and len(negative_label_domains) > 0):
        for x in wr_crawl_diff_groups:
            url = x.get("url")
            if (positive_label_domains and url in positive_label_domains) or (
                    negative_label_domains and url in negative_label_domains):
                if url not in urls_found:
                    wr_crawl_diff_groups_list.append(x)
                    urls_found.append(url)
                else:
                    logger.warning("Possible duplicated ground truth: %s" %
                                   url)

        if positive_label_domains:
            pos_url_list = []
            for pos_url in positive_label_domains:
                if pos_url not in urls_found:
                    logger.warning(
                        "Could not find pos labeled url for processing %s" %
                        pos_url)
                    url_not_found_queue.put(pos_url)
                    pos_url_list.append(pos_url)
            logger.debug("Positive labels not found: %d" % len(pos_url_list))
        if negative_label_domains:
            neg_url_list = []
            for neg_url in negative_label_domains:
                if neg_url not in urls_found:
                    logger.warning(
                        "Could not find neg labeled url for processing %s" %
                        neg_url)
                    url_not_found_queue.put("!" + neg_url)
                    neg_url_list.append(neg_url)
            logger.debug("Negative urls not found: %d" % len(neg_url_list))

        features_debug.put("Feature Extraction only for ground truth: %d" %
                           len(wr_crawl_diff_groups_list))
        logger.debug("Feature Extraction only for ground truth: %d" %
                     len(wr_crawl_diff_groups_list))

    else:
        wr_crawl_diff_groups_list = list(wr_crawl_diff_groups)
        features_debug.put("Feature Extraction for all diff groups: %d" %
                           len(wr_crawl_diff_groups_list))
        logger.debug("Feature Extraction for all diff groups: %d" %
                     len(wr_crawl_diff_groups_list))

    if existing_file:
        if os.path.isfile(existing_file):
            logger.debug("Filtering sites based off existing file: %s" %
                         existing_file)

            existing_file_pd = pd.read_csv(existing_file, index_col=0)
            wr_crawl_diff_groups_list_new = []
            for x in wr_crawl_diff_groups_list:
                url = x.get("url")
                if url not in existing_file_pd.index:
                    wr_crawl_diff_groups_list_new.append(x)

            wr_crawl_diff_groups_list = wr_crawl_diff_groups_list_new
            logger.debug(
                "Feature Extraction only for remaining sites not in existing file - %d"
                % len(wr_crawl_diff_groups_list))
        else:
            logger.warning("Could not find existing file: %s" % existing_file)

    if rank_file is not None and rank_start is not None and rank_end is not None:
        logger.debug("Feature Extraction - only for rank %s to %s " %
                     (rank_start, rank_end))
        rank_dict = dict()
        with open(rank_file, 'r') as rank_file_open:
            reader = csv.DictReader(rank_file_open, delimiter=',')
            for row in reader:
                rank_dict[row["URL Crawled"]] = int(row["Rank"])

        logger.debug("Feature Extraction - found ranks %d" % len(rank_dict))
        wr_crawl_diff_groups_list_new = []
        for x in wr_crawl_diff_groups_list:
            url = x.get("url")
            if url in rank_dict:
                rank_of_url = rank_dict.get(url)
                if rank_start <= rank_of_url <= rank_end:
                    wr_crawl_diff_groups_list_new.append(x)

        wr_crawl_diff_groups_list = wr_crawl_diff_groups_list_new
        logger.debug(
            "Feature Extraction only for remaining sites within rank - %d" %
            len(wr_crawl_diff_groups_list))

    # read in time series file to be reused
    time_series_dict = dict()
    if time_series_mapping:
        logger.debug("Loading time series mapping %s" % time_series_mapping)
        with open(time_series_mapping, 'r') as time_file:
            reader = csv.DictReader(time_file, delimiter=',')
            for row in reader:
                time_series_dict[row["URL Crawled"]] = row

    # set to None if there is no time series to use (to ignore it)
    if len(time_series_dict.keys()) == 0:
        logger.debug("No time series found in %s" % time_series_mapping)
        time_series_dict = None

    diff_groups_count = len(wr_crawl_diff_groups_list)
    process_limit = 10
    process_chunk_size = int(diff_groups_count / process_limit) + 1
    logger.debug("Process Chunk size %d" % process_chunk_size)

    process_chunks = chunk(wr_crawl_diff_groups_list, n=process_chunk_size)

    logger.debug("Process chunks %s" % str(process_chunks))

    # verify the number is the same
    total_count = 0
    for chunk_tmp in process_chunks:
        total_count += len(chunk_tmp)

    assert (
            total_count == diff_groups_count
    ), "Process chunks total count not equal to original size of list: " + str(
        total_count) + ", " + str(diff_groups_count)

    logger.debug("Before Creating Processes %d" % len(process_chunks))

    process_list = []
    for process_index in range(0, len(process_chunks)):
        process_chunk_tmp = process_chunks[process_index]

        # first process gets to write the header
        csv_has_header = True
        if process_index == 0:
            csv_has_header = False

        logger.debug("Creating Processes %d" % process_index)

        p = Process(target=_write_feature_csv__process,
                    args=(process_index, crawl_group_name, mongodb_client,
                          mongodb_port, process_chunk_tmp, time_series_dict,
                          features_queue, features_debug, diff_debug,
                          tracking_file_path, positive_label_domains,
                          negative_label_domains, csv_has_header,
                          output_external_logs, 20, 30,
                          img_dimension_file_path, trials))
        p.start()

        process_list.append(p)

    logger.debug("Created Processes %d" % len(process_list))

    # wait for all to be done
    for p in process_list:
        p.join()

    logger.debug("All work process are done")
    client.close()

    time.sleep(10)
    logger.debug("Cleaning up all output processes")
    features_shutdown_event.set()
    features_debug_shutdown_event.set()
    diff_debug_shutdown_event.set()
    logger.debug("Waiting for output processes to complete")
    features_process.join()
    features_debug_process.join()
    diff_debug_process.join()
    url_not_found_shutdown_event.set()
    url_not_found_process.join()
    logger.info("DONE")

    return raw_features_file_name


def write_urls_txt(crawl_group_name,
                   mongodb_client,
                   mongodb_port,
                   ground_truth_file_path=None,
                   csv_file_name=DEFAULT_CSV_FILE_NAME,
                   output_directory=None,
                   ground_truth_only=True,
                   trial_count=4):
    # read in file with domains labeled as positives
    # if line starts with ! , then it means it is negative label

    positive_label_domains = None
    negative_label_domains = None
    if ground_truth_file_path:
        positive_label_domains, negative_label_domains = get_ground_truth(
            ground_truth_file_path)

    client, db = get_anticv_client_and_db()

    # We create multiple csvs depending on split_by_party
    raw_output_file_name = output_directory + os.sep + csv_file_name + ".txt"
    output_queue = Queue()
    output_queue_shutdown_event = Event()
    output_queue_process = OutputDebugProcess("1", "output_queue_urls",
                                              raw_output_file_name,
                                              output_queue_shutdown_event,
                                              output_queue)
    output_queue_process.start()

    raw_output_file_name_control = output_directory + os.sep + csv_file_name + "_control" + ".txt"
    output_queue_control = Queue()
    output_queue_shutdown_event_control = Event()
    output_queue_process_control = OutputDebugProcess(
        "2", "output_queue_urls_control", raw_output_file_name_control,
        output_queue_shutdown_event_control, output_queue_control)
    output_queue_process_control.start()

    # We create multiple csvs depending on split_by_party
    raw_trials_file_name = output_directory + os.sep + "trials_urls_and_domains" + ".csv"
    trials_queue = Queue()
    trials_shutdown_event = Event()
    trials_header_row = ["URL Crawled"]
    for trial_index in range(trial_count):
        trials_header_row.append(CONTROL + TRIAL_PREFIX + str(trial_index) +
                                 "_urls")
    for trial_index in range(trial_count):
        trials_header_row.append(CONTROL + TRIAL_PREFIX + str(trial_index) +
                                 "_domains")
    for trial_index in range(trial_count):
        trials_header_row.append(VARIANT + TRIAL_PREFIX + str(trial_index) +
                                 "_urls")
    for trial_index in range(trial_count):
        trials_header_row.append(VARIANT + TRIAL_PREFIX + str(trial_index) +
                                 "_domains")

    trials_process = OutputCSVProcess("1",
                                      "main_trials_csv",
                                      raw_trials_file_name,
                                      trials_shutdown_event,
                                      trials_queue,
                                      header_row=trials_header_row)
    trials_process.start()

    # these diff groups are only for webrequests
    wr_crawl_diff_groups = get_by_crawl_group_name(crawl_group_name,
                                                   db,
                                                   MONGODB_WR_DIFF_GROUP,
                                                   discard="false")
    logger.debug("WR DIFF GROUPS FOUND %d" % wr_crawl_diff_groups.count())

    wr_crawl_diff_groups_list = []
    urls_found = []
    if ground_truth_only and (positive_label_domains
                              and len(positive_label_domains) > 0) or (
            negative_label_domains
            and len(negative_label_domains) > 0):
        for x in wr_crawl_diff_groups:
            url = x.get("url")
            if (positive_label_domains and url in positive_label_domains) or (
                    negative_label_domains and url in negative_label_domains):
                if url not in urls_found:
                    wr_crawl_diff_groups_list.append(x)
                    urls_found.append(url)
                else:
                    logger.warning("Possible duplicated ground truth: %s" %
                                   url)

        if positive_label_domains:
            pos_url_list = []
            for pos_url in positive_label_domains:
                if pos_url not in urls_found:
                    logger.warning(
                        "Could not find pos labeled url for processing %s" %
                        pos_url)
                    pos_url_list.append(pos_url)
            logger.debug("Positive labels not found: %d" % len(pos_url_list))
        if negative_label_domains:
            neg_url_list = []
            for neg_url in negative_label_domains:
                if neg_url not in urls_found:
                    logger.warning(
                        "Could not find neg labeled url for processing %s" %
                        neg_url)
                    neg_url_list.append(neg_url)
            logger.debug("Negative urls not found: %d" % len(neg_url_list))

        logger.info("Feature Extraction only for ground truth: %d",
                    len(wr_crawl_diff_groups_list))

    else:
        wr_crawl_diff_groups_list = list(wr_crawl_diff_groups)
        logger.info("Feature Extraction for all diff groups: %d" %
                    len(wr_crawl_diff_groups_list))

    diff_groups_count = len(wr_crawl_diff_groups_list)
    process_limit = 10
    process_chunk_size = int(diff_groups_count / process_limit) + 1
    process_chunks = chunk(wr_crawl_diff_groups_list, n=process_chunk_size)

    # verify the number is the same
    total_count = 0
    for chunk_tmp in process_chunks:
        total_count += len(chunk_tmp)

    assert (
            total_count == diff_groups_count
    ), "Process chunks total count not equal to original size of list: " + str(
        total_count) + ", " + str(diff_groups_count)

    process_list = []
    for process_index in range(0, len(process_chunks)):
        process_chunk_tmp = process_chunks[process_index]

        # first process gets to write the header
        csv_has_header = True
        if process_index == 0:
            csv_has_header = False

        p = Process(target=_write_urls_csv__process,
                    args=(process_index, crawl_group_name, mongodb_client,
                          mongodb_port, process_chunk_tmp, output_queue,
                          output_queue_control, trials_queue,
                          positive_label_domains, negative_label_domains))
        p.start()

        process_list.append(p)

    # wait for all to be done
    for p in process_list:
        p.join()

    logger.debug("All work process are done")
    client.close()

    time.sleep(10)
    logger.debug("Cleaning up all output processes")
    output_queue_shutdown_event.set()
    output_queue_shutdown_event_control.set()
    trials_shutdown_event.set()
    logger.debug("Waiting for output processes to complete")
    output_queue_process.join()
    output_queue_process_control.join()
    trials_process.join()
    logger.info("DONE")

    return ""


def _wrap_features(features):
    return [CRAWL_URL_COLUMN_NAME] + features + [TARGET_COLUMN_NAME]


def _get_columns_in_dataframe(col_list, df):
    col_list_filter = [x for x in col_list if x in df.columns]
    return col_list_filter


def _fillNA(df):
    ts_last_time_diff_col = "ts__last_time_diff"
    if ts_last_time_diff_col in df.columns:
        df[ts_last_time_diff_col] = df[ts_last_time_diff_col].div(100).round(2)

    return df.fillna(0)


def clean_ground_truth(pd_data, features=None, ground_truth_file=None):
    ### GROUND TRUTH FILE
    is_groundtruth = pd_data[TARGET_COLUMN_NAME].isin([0, 1])
    logger.debug(is_groundtruth.head)
    pd_gr_truth = pd_data[is_groundtruth]
    logger.debug(pd_gr_truth.shape)

    # Here we ignore all rows that are all zeroes for ground truth only
    # filter ground truth, (1) find sum rows add up to one, then find the cv_detect is 1, then remove those.
    logger.debug("trying to filter out columns with all zeroes for ground truth cv_detect = 1")
    temp = pd_gr_truth.sum(axis=1) == 1
    logger.debug(temp)
    temp_index = pd_gr_truth[temp][TARGET_COLUMN_NAME].isin([1]).index
    logger.debug(temp_index)
    pd_gr_truth = pd_gr_truth.drop(temp_index)
    logger.debug(pd_gr_truth.shape)

    positive_label_domains = None
    negative_label_domains = None
    if ground_truth_file:
        positive_label_domains, negative_label_domains = get_ground_truth(
            ground_truth_file)
        merged_gr = positive_label_domains + negative_label_domains
        is_groundtruth = pd_data[CRAWL_URL_COLUMN_NAME].isin(merged_gr)
        logger.debug(is_groundtruth.head)
        pd_gr_truth = pd_data[is_groundtruth]

    if features is not None:
        wr_features = _wrap_features(features)
        wr_features = _get_columns_in_dataframe(wr_features, pd_gr_truth)
        # logger.info("Ground Truth: Filtering to some features only")
        pd_gr_truth = pd_gr_truth[wr_features]
        logger.debug(pd_gr_truth.shape)

    return pd_gr_truth


def clean_unlabel_data(pd_data, features=None):
    logger.debug("Finding unlabeled")
    is_unlabeled = pd_data[TARGET_COLUMN_NAME].isin([-1])
    logger.debug(is_unlabeled.head)
    pd_unlabeled = pd_data[is_unlabeled]
    logger.debug(pd_unlabeled.shape)

    # labeled
    logger.debug("Finding labeled")
    is_labeled = ~pd_data[TARGET_COLUMN_NAME].isin([-1])
    logger.debug(is_labeled.head)
    pd_labeled = pd_data[is_labeled]
    logger.debug(pd_labeled.shape)

    if features is not None:
        logger.debug("Unlabel Data: Filtering to some features only")
        wr_features = _wrap_features(features)
        wr_features = _get_columns_in_dataframe(wr_features, pd_unlabeled)
        pd_unlabeled = pd_unlabeled[wr_features]
        pd_labeled = pd_labeled[wr_features]

    # sort by index to reduce randomness
    pd_unlabeled = pd_unlabeled.sort_index()
    pd_labeled = pd_labeled.sort_index()

    # remove nans
    pd_unlabeled = pd_unlabeled.fillna(0)
    pd_labeled = pd_labeled.fillna(0)

    return pd_unlabeled, pd_labeled


def scale_data(scaler,
               scaler_name,
               pd_data,
               features,
               fit=False,
               transform=True,
               save=False,
               output_directory=None,
               output_suffix=None):
    if fit:
        scaler.fit(pd_data[features])
        if transform:
            pd_data[features] = scaler.transform(pd_data[features])
        # save the scaler
        if save:
            if output_directory:
                filename = output_directory + os.sep + 'scaler_' + scaler_name
                if output_suffix:
                    filename += '_' + output_suffix
            else:
                filename = 'scaler_' + scaler_name

            filename += ".sav"

            logger.debug("Saving scaler: " + filename)
            pickle.dump(scaler, open(filename, 'wb'))
    else:
        if transform:
            pd_data[features] = scaler.transform(pd_data[features])

    return pd_data, scaler


# Cleans the raw_csv_features_file_path by splitting it into ground truth and unlabel files
# For ground truth file, removes all row that are entirely of zeroes
# Add scaling to the data, ignoring some features
# ignore_feature_scaling : list of features to ignore during scaling
# test_features_only : list of features names to include only. Ignore everything else. If this is None, we keep all columns
def _clean_scale_data_for_training(csv_file_name,
                                   output_suffix,
                                   output_directory,
                                   ignore_feature_scaling,
                                   test_features_only=None,
                                   random_state=40,
                                   should_scale=True,
                                   ground_truth_file=None,
                                   webshrinker_csv=None,
                                   languages=None,
                                   categories=None,
                                   reverse_language=False):
    # read in inputs
    csv_suffix = output_suffix

    header = ""
    with open(csv_file_name, 'r') as csv_file:
        for line in csv_file:
            header = line.replace("\n", "")
            break
    header_names = header.split(",")

    pd_data = pd.read_csv(csv_file_name)
    logger.debug("Shape of main file: %s" % str(pd_data.shape))

    pd_data = _fillNA(pd_data)

    if webshrinker_csv:
        webshrinker_file = pd.read_csv(webshrinker_csv, index_col=0)
        logger.debug("Webshrinker file: %s" % str(webshrinker_file.shape))
        # 'Crawl URL'
        if languages:
            languages_list = languages.split(",")
            if not reverse_language:
                apply_languages = webshrinker_file["Language"].isin(
                    languages_list)
            else:
                apply_languages = ~webshrinker_file["Language"].isin(
                    languages_list)

            logger.debug("Applying lanaguge %s" % str(languages))
            webshrinker_file = webshrinker_file[apply_languages]
            logger.debug("After language: %s" % str(webshrinker_file.shape))

        if categories:
            categories_list = categories.split(",")
            logger.debug("Applying categories %s" % str(categories_list))
            apply_categories = []
            for row in webshrinker_file["Categories"]:
                match = False
                for categ in categories_list:
                    if categ in row:
                        match = True

                apply_categories.append(match)

            # apply_categories = webshrinker_file["Categories"].isin(categories_list)
            webshrinker_file = webshrinker_file[apply_categories]
            logger.debug("After categories: %s" % str(webshrinker_file.shape))

        # filter out by crawl url
        row_mask = []
        for row in pd_data[CRAWL_URL_COLUMN_NAME]:
            url_tld = extract_tld(row)
            sld = get_second_level_domain_from_tld(url_tld)
            if sld in webshrinker_file.index:
                row_mask.append(True)
            else:
                row_mask.append(False)

        pd_data = pd_data[row_mask]
        logger.debug("Shape of main file after applying webshrinker: %s" %
                     str(pd_data.shape))

    ### GROUND TRUTH FILE
    pd_gr_truth = clean_ground_truth(pd_data,
                                     features=test_features_only,
                                     ground_truth_file=ground_truth_file)
    gr_truth_trunc_file_name = "ground_truth_trunc_" + csv_suffix + ".csv"
    pd_gr_truth.to_csv(output_directory + os.sep + gr_truth_trunc_file_name,
                       index=False)

    ### UNLABEL DATA FILE
    pd_unlabeled, _ = clean_unlabel_data(pd_data, features=test_features_only)
    unlabel_trunc_file_name = "unlabelled_data_trunc_" + csv_suffix + ".csv"
    pd_unlabeled.to_csv(output_directory + os.sep + unlabel_trunc_file_name,
                        index=False)

    if test_features_only:
        wr_features = _wrap_features(test_features_only)
        # this affects the header_names as well
        logger.debug("updating header names to some features only")
        header_names = wr_features

    ### Find features that we want to scale, then scaling them
    if should_scale:
        scale_features = [
            x for x in header_names if x not in ignore_feature_scaling
        ]

        scalers = [("standardscale", StandardScaler()),
                   ("minmax", MinMaxScaler()), ("robust", RobustScaler()),
                   ("quantile",
                    QuantileTransformer(random_state=random_state)),
                   ("powertrans", PowerTransformer())]

        for scaler_name, scaler in scalers:
            logger.debug("Ground truth %s Scaling" % scaler_name)
            # fit transform to the training data
            pd_gr_truth_temp = pd_gr_truth.copy()
            pd_unlabeled_temp = pd_unlabeled.copy()

            # fit and scale for ground truth
            pd_gr_truth_temp, _ = scale_data(scaler,
                                             scaler_name,
                                             pd_gr_truth_temp,
                                             scale_features,
                                             fit=True,
                                             save=True,
                                             output_directory=output_directory,
                                             output_suffix=output_suffix)

            logger.debug(pd_gr_truth_temp)
            gr_truth_file_name = "ground_truth_" + scaler_name + "_" + csv_suffix + ".csv"
            pd_gr_truth_temp.to_csv(output_directory + os.sep +
                                    gr_truth_file_name)

            logger.debug("Unlabelled data %s Scaling" % scaler_name)
            pd_unlabeled_temp, _ = scale_data(scaler, scaler_name,
                                              pd_unlabeled_temp,
                                              scale_features)
            logger.debug(pd_unlabeled_temp)
            unlabelled_file_name = "unlabelled_data_" + scaler_name + "_" + csv_suffix + ".csv"
            pd_unlabeled_temp.to_csv(output_directory + os.sep +
                                     unlabelled_file_name)


# main method to clean and scale data for labelling
# here we use the main features listed in constants
def clean_scale_data_for_training_default(csv_file_name,
                                          output_suffix,
                                          output_directory,
                                          random_state=40,
                                          should_scale=True,
                                          ground_truth_file=None,
                                          webshrinker_csv=None,
                                          languages=None,
                                          categories=None,
                                          reverse_language=False):
    ignore_feature_scaling = BOOLEAN_FEATURES
    test_features_only = None
    _clean_scale_data_for_training(csv_file_name,
                                   output_suffix,
                                   output_directory,
                                   ignore_feature_scaling,
                                   test_features_only,
                                   random_state=random_state,
                                   should_scale=should_scale,
                                   ground_truth_file=ground_truth_file,
                                   webshrinker_csv=webshrinker_csv,
                                   languages=languages,
                                   categories=categories,
                                   reverse_language=reverse_language)


# main method to clean and scale data for labelling
# here we use the main features listed in a given file from features_file_path


def clean_scale_data_for_training(csv_file_name,
                                  features_file_path,
                                  output_suffix,
                                  output_directory,
                                  random_state=40,
                                  should_scale=True,
                                  ground_truth_file=None,
                                  webshrinker_csv=None,
                                  languages=None,
                                  categories=None,
                                  reverse_language=False):
    ignore_feature_scaling = BOOLEAN_FEATURES

    # read features from a file given
    test_features_only = []
    with open(features_file_path, "r") as features_file:
        for feature_name in features_file:
            test_features_only.append(feature_name.rstrip('\n'))

    _clean_scale_data_for_training(csv_file_name,
                                   output_suffix,
                                   output_directory,
                                   ignore_feature_scaling,
                                   test_features_only,
                                   random_state=random_state,
                                   should_scale=should_scale,
                                   ground_truth_file=ground_truth_file,
                                   webshrinker_csv=webshrinker_csv,
                                   languages=languages,
                                   categories=categories,
                                   reverse_language=reverse_language)


def _clean_scale_data_for_labeling(csv_file_name,
                                   output_suffix,
                                   output_directory,
                                   ignore_feature_scaling,
                                   scaler_file_path=None,
                                   test_features_only=None,
                                   ignore_labels=False,
                                   webshrinker_csv=None,
                                   languages=None,
                                   categories=None,
                                   reverse_language=False):
    result_files = dict()

    # read in inputs
    csv_suffix = output_suffix

    header = ""
    with open(csv_file_name, 'r') as csv_file:
        for line in csv_file:
            header = line.replace("\n", "")
            break

    pd_data = pd.read_csv(csv_file_name, index_col=0)
    pd_data = _fillNA(pd_data)

    # filter by webshrinker
    if webshrinker_csv:
        webshrinker_file = pd.read_csv(webshrinker_csv, index_col=0)
        logger.debug("Webshrinker file: %s" % str(webshrinker_file.shape))
        # 'Crawl URL'
        if languages:
            languages_list = languages.split(",")
            if not reverse_language:
                apply_languages = webshrinker_file["Language"].isin(
                    languages_list)
            else:
                apply_languages = ~webshrinker_file["Language"].isin(
                    languages_list)

            logger.debug("Applying lanaguge %s" % str(languages))
            webshrinker_file = webshrinker_file[apply_languages]
            logger.debug("After language: %s" % str(webshrinker_file.shape))

        if categories:
            categories_list = categories.split(",")
            logger.debug("Applying categories %s" % str(categories_list))
            apply_categories = []
            for row in webshrinker_file["Categories"]:
                match = False
                for categ in categories_list:
                    if categ in row:
                        match = True

                apply_categories.append(match)

            # apply_categories = webshrinker_file["Categories"].isin(categories_list)
            webshrinker_file = webshrinker_file[apply_categories]
            logger.debug("After categories: %s" % str(webshrinker_file.shape))

        # filter out by crawl url
        row_mask = []
        for row in pd_data.index:
            url_tld = extract_tld(row)
            sld = get_second_level_domain_from_tld(url_tld)
            if sld in webshrinker_file.index:
                row_mask.append(True)
            else:
                row_mask.append(False)

        logger.debug("Data before filtering by webshrinker %s " %
                     str(pd_data.shape))
        pd_data = pd_data[row_mask]
        logger.debug("Data after filtering by webshrinker %s " %
                     str(pd_data.shape))

    ### UNLABEL DATA FILE

    # first save the truncated version
    pd_unlabeled_trunc = pd_data.copy()

    # treat everything as negative columns
    if ignore_labels:
        pd_unlabeled_trunc[TARGET_COLUMN_NAME] = -1

    pd_unlabeled_trunc, pd_labeled = clean_unlabel_data(
        pd_unlabeled_trunc, features=test_features_only)

    unlabel_trunc_file_name = "unlabelled_data_trunc_" + csv_suffix + ".csv"
    unlabel_trunc_file_path = output_directory + os.sep + unlabel_trunc_file_name
    pd_unlabeled_trunc.to_csv(unlabel_trunc_file_path, index=True)

    labeled_trunc_file_name = "labeled_data_tmp_" + csv_suffix + ".csv"
    labeled_trunc_file_path = output_directory + os.sep + labeled_trunc_file_name
    pd_labeled.to_csv(labeled_trunc_file_path, index=True)

    result_files[RAW_UNLABEL_FILE_KEY] = unlabel_trunc_file_path

    return result_files


def get_test_features_from_file(features_file_path):
    # read features from a file given
    test_features_only = None
    if features_file_path:
        with open(features_file_path, "r") as features_file:
            test_features_only = []
            for feature_name in features_file:
                test_features_only.append(feature_name.rstrip('\n'))

    return test_features_only
