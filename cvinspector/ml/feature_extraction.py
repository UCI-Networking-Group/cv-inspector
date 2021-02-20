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
import glob
import json
import logging
import os
import re
import time

import numpy as np
from bs4 import BeautifulSoup

from cvinspector.common.dommutation_utils import get_attribute_changed_info, get_nodes_added__node_name
from cvinspector.common.dommutation_utils import get_attribute_changed_key
from cvinspector.common.utils import CONTROL, VARIANT, get_anticv_client_and_db, \
    avg_growth_rate, get_linear_regress, \
    ABP_BLOCKED_ELEMENT, ANTICV_OFFSETWIDTH, ANTICV_OFFSETHEIGHT, PAGE_SOURCE_SUFFIX, \
    WEBREQUESTS_DATA_FILE_SUFFIX_VARIANT, \
    WEBREQUESTS_DATA_FILE_SUFFIX_CONTROL, get_css_dict, get_trial_file_name_details, get_trial_label
from cvinspector.common.webrequests_utils import find_all_first_and_third_party_webrequests, \
    split_info_first_and_third_party_requests, \
    get_domain_only_from_url, get_webrequest_detail_value
from cvinspector.common.webrequests_utils import get_domain_only_from_tld, has_subdomain_larger_than_n, \
    get_subdomain_stats, get_path_and_query_params
from cvinspector.common.webrequests_utils import get_path_and_query_stats, has_short_special_character_path, \
    has_subdomain_as_path, get_second_level_domain_from_tld, get_subdomain_entropy_from_set, \
    get_content_type_mismatch, get_content_type_mapping, extract_tld, build_cache_control_mapping, \
    get_path_and_query_stats_with_cache_control, \
    filter_requests_by_header, get_url_without_query
from cvinspector.data_migrate.utils import get_file_name
from cvinspector.diff_analysis.utils import create_trial_group

logger = logging.getLogger(__name__)
#logger.setLevel("DEBUG")

TIMESERIES_KEY = "ts_"

CSV_BIN_NORM = "bin_norm"
CSV_NODES_ADDED = "nodes_added"
CSV_NODES_REMOVED = "nodes_removed"
CSV_ATTRIBUTE_CHANGED = "attribute_changed"
CSV_TEXT_CHANGED = "text_changed"
CSV_TOTAL_CHANGES = "total_changes"

CSV_BLOCKED = "blocked"
CSV_WR_BLOCKED = "web_req_blocked"
CSV_ELEM_BLOCKED = "elem_blocked"
CSV_SNIPPET_BLOCKED = "snippet_blocked"

CSV_IFRAME_SRC_CHANGED = "iframe_src_changed"
CSV_IFRAME_BLOCKED = "iframe_blocked"

IMPORTANT_CSV_KEYS = [
    CSV_NODES_ADDED, CSV_NODES_REMOVED, CSV_ATTRIBUTE_CHANGED, CSV_TEXT_CHANGED
]


SPIKE_MIN = 2


# given a last_event_key, get the time for that. Then count the occurrences specific_keys after that
def get_count_of_events_after_last_event(specific_keys, last_event_key, rows):
    def _has_spike(specific_keys, row):
        for key in row.keys():
            if key in specific_keys and int(row[key]) >= SPIKE_MIN:
                return True
        return False

    considered_rows = rows
    last_event_index = 0
    for index, row in enumerate(reversed(considered_rows), start=1):
        if last_event_key in row and int(row[last_event_key]) > 0:
            last_event_index = -1 * index
            break

    event_count = 0

    # only if find the last_event_key. Else return zero
    if last_event_index < 0:
        considered_rows = considered_rows[last_event_index:]

        for row in considered_rows:
            if _has_spike(specific_keys, row):
                event_count += 1

    return event_count


# get the average time between a block event and the first occurrence of spike after
# supposed to get how fast the site may react to the block
def get_avg_time_after_blocked(specific_keys,
                               blocked_key,
                               rows,
                               after_event_key=None,
                               logger_prefix=""):
    def _has_spike(specific_keys, row):
        for key in row.keys():
            if key in specific_keys and int(row[key]) >= SPIKE_MIN:
                return True
        return False

    considered_rows = rows
    after_event_index = -1
    if after_event_key is not None:
        for index, row in enumerate(considered_rows, start=0):
            if after_event_key in row and int(row[after_event_key]) > 0:
                after_event_index = index
                break

    # only consider events after the event of the passed in after_event_key
    if after_event_index >= 0:
        considered_rows = considered_rows[after_event_index:]
    else:
        logger.debug("Considering all rows for get_avg_time_after_blocked")

    times = []
    time_count = 0
    found_first_block = False

    for row in considered_rows:
        if blocked_key in row:
            if int(row[blocked_key]) > 0:
                # reset
                time_count = 0
                found_first_block = True
            elif _has_spike(specific_keys, row):
                # keep track of per spike
                if found_first_block:
                    times.append(time_count)
                    #reset
                    found_first_block = False
                    time_count = 0

        time_count += 1

    if len(times) == 0:
        return 0
    logger.debug("%s - Average times found %s" % (logger_prefix, times))
    return np.average(times)


def get_avg_time_after_domcontentloaded(rows):
    return get_avg_time_after_blocked(IMPORTANT_CSV_KEYS,
                                      CSV_BLOCKED,
                                      rows,
                                      after_event_key="dom_content_loaded")


def get_clusters(property_name, rows):
    # list of lists
    clusters = []

    threshold = 2
    count = 0

    current_cluster = []
    for row in rows:
        if int(row[property_name]) == 0:
            if count > 0:
                count -= 1
        else:
            # reset count
            count = threshold

        if count > 0 and int(row[property_name]) >= SPIKE_MIN:
            current_cluster.append(int(row[property_name]))

        if count == 0:
            # end the current cluster
            if len(current_cluster) > 0:
                clusters.append(current_cluster)
                current_cluster = []

    if len(current_cluster) > 0:
        clusters.append(current_cluster)

    return clusters


class BaseCVFeatureExtraction:
    def __init__(self,
                 crawl_url,
                 control_only_docs,
                 variant_only_docs,
                 docs_as_dict=False,
                 trials=4):
        self.crawl_url = crawl_url
        self.logs = []
        self.trials=trials
        if docs_as_dict:
            self.control_only_docs = control_only_docs or {}
            self.variant_only_docs = variant_only_docs or {}
        else:
            self.control_only_docs = control_only_docs or []
            self.variant_only_docs = variant_only_docs or []

    # returns a dict
    def extract_features(self):
        return {}

    def add_log(self, message):
        self.logs.append(message)

    def extract_features_vector(self):
        features_dict = self.extract_features()
        sorted_keys = sorted(features_dict.keys())

        features_vector = []
        for key in sorted_keys:
            value = features_dict.get(key)
            features_vector.append(value)

        #print(features_vector)
        return sorted_keys, features_vector


class TimeSeriesDOMFeatureExtraction(BaseCVFeatureExtraction):

    # we pass time_series_dict the mapping between and the time series csv file
    def __init__(self, crawl_url, time_series_dict, logger_prefix="", trials=4):
        BaseCVFeatureExtraction.__init__(self, crawl_url, None, None, trials=trials)
        self.time_series_dict = time_series_dict
        self.logger_prefix = logger_prefix

    def get_event_frequency(self, property_key, rows, spike_min=1):
        event_count = 0

        for row in rows:
            if property_key in row:
                if int(row[property_key]) >= spike_min:
                    event_count += 1

        event_freq = 0
        if len(rows) > 0:
            event_freq = event_count / len(rows)

        return event_count, event_freq

    def get_time_between_event(self, property_key, rows, spike_min=1):
        time_gaps = []
        last_event_time = -1

        # we don't count the first occurrence as a time gap. we only look at the avg time between each event
        for index, row in enumerate(rows, start=1):
            if property_key in row and int(row[property_key]) >= spike_min:
                if last_event_time > 0:
                    gap = index - last_event_time
                    time_gaps.append(gap)

                last_event_time = index

        logger.debug("Gap found for get_time_between_event %s " % time_gaps)

        if len(time_gaps) == 0:
            return 0

        return np.average(time_gaps)

    def get_event_frequency_by_half_features(self,
                                             property_key,
                                             rows,
                                             spike_min=1):
        event_freq_first_half = 0
        time_between_first_half = 0
        event_freq_second_half = 0
        time_between_second_half = 0
        if len(rows) > 1:
            mid = int(len(rows) / 2)
            _, event_freq_first_half = self.get_event_frequency(
                property_key, rows[:mid], spike_min=spike_min)
            _, event_freq_second_half = self.get_event_frequency(
                property_key, rows[mid:], spike_min=spike_min)
            time_between_first_half = self.get_time_between_event(
                property_key, rows[:mid], spike_min=spike_min)
            time_between_second_half = self.get_time_between_event(
                property_key, rows[mid:], spike_min=spike_min)
        else:
            logger.debug(
                "%s - Time series did not have correct number of rows %s" %
                (self.logger_prefix, self.crawl_url))

        features = dict()
        features["event_freq_first_half"] = event_freq_first_half
        features["time_between_first_half"] = time_between_first_half
        features["event_freq_second_half"] = event_freq_second_half
        features["time_between_second_half"] = time_between_second_half

        logger.debug(features)
        return features

    def get_event_frequency_by_fifths_features(self,
                                               property_key,
                                               rows,
                                               spike_min=1,
                                               expected_rows=250):
        event_freq_first = 0
        event_freq_second = 0
        event_freq_third = 0
        event_freq_fourth = 0
        event_freq_fifth = 0
        if len(rows) == expected_rows:
            _, event_freq_first = self.get_event_frequency(property_key,
                                                           rows[:50],
                                                           spike_min=spike_min)
            _, event_freq_second = self.get_event_frequency(
                property_key, rows[50:100], spike_min=spike_min)
            _, event_freq_third = self.get_event_frequency(property_key,
                                                           rows[100:150],
                                                           spike_min=spike_min)
            _, event_freq_fourth = self.get_event_frequency(
                property_key, rows[150:200], spike_min=spike_min)
            _, event_freq_fifth = self.get_event_frequency(property_key,
                                                           rows[200:],
                                                           spike_min=spike_min)
        else:
            logger.debug(
                "%s - Time series did not have correct number of rows %s" %
                (self.logger_prefix, self.crawl_url))

        features = dict()
        features["event_freq_first_fifth"] = event_freq_first
        features["event_freq_second_fifth"] = event_freq_second
        features["event_freq_third_fifth"] = event_freq_third
        features["event_freq_fourth_fifth"] = event_freq_fourth
        features["event_freq_fifth_fifth"] = event_freq_fifth
        logger.debug(features)
        return features

    def get_avg_cluster_size(self, property_name, rows):
        # avg cluster size
        clusters = get_clusters(property_name, rows)
        cluster_lengths = [len(i) for i in clusters]
        if len(cluster_lengths) > 0:
            return np.average(cluster_lengths)

        return 0

    def get_simple_trial_features(self, trial, time_series):

        _MIN = 10000

        blocked_spikes = 0
        wr_blocked_spikes = 0
        elem_blocked_spikes = 0
        snippet_blocked_spikes = 0
        nodes_added_spikes = 0
        nodes_removed_spikes = 0
        attribute_changed_spikes = 0
        text_changed_spikes = 0

        max_nodes_added = 0
        min_nodes_added = _MIN
        max_nodes_removed = 0
        min_nodes_removed = _MIN
        max_attribute_changed = 0
        min_attribute_changed = _MIN
        max_text_changed = 0
        min_text_changed = _MIN

        rows = []
        feature_dict = dict()

        with open(time_series) as time_file:

            reader = csv.DictReader(time_file, delimiter=',')
            rows = list(reader)
            for row in rows:
                csv_blocked = 0
                csv_wr_blocked = 0
                csv_elem_blocked = 0
                csv_snippet_blocked = 0
                # if it has blocked
                if CSV_BLOCKED in row:
                    csv_blocked = int(row[CSV_BLOCKED])
                    csv_wr_blocked = int(row[CSV_WR_BLOCKED])
                    csv_elem_blocked = int(row[CSV_ELEM_BLOCKED])
                    if CSV_SNIPPET_BLOCKED in row:
                        csv_snippet_blocked = int(row[CSV_SNIPPET_BLOCKED])

                csv_nodes_added = int(row[CSV_NODES_ADDED])
                csv_nodes_removed = int(row[CSV_NODES_REMOVED])
                csv_attribute_changed = int(row[CSV_ATTRIBUTE_CHANGED])
                csv_text_changed = int(row[CSV_TEXT_CHANGED])

                # we treat a block as > 0
                if csv_blocked > 0:
                    blocked_spikes += 1
                    if csv_wr_blocked > 0:
                        wr_blocked_spikes += 1
                    if csv_elem_blocked > 0:
                        elem_blocked_spikes += 1
                    if csv_snippet_blocked > 0:
                        snippet_blocked_spikes += 1

                # we treat a spike as having an event happen at least >= SPIKE_MIN
                if csv_nodes_added >= SPIKE_MIN:
                    nodes_added_spikes += 1
                    if csv_nodes_added > max_nodes_added:
                        max_nodes_added = csv_nodes_added
                    if csv_nodes_added < min_nodes_added:
                        min_nodes_added = csv_nodes_added
                if csv_nodes_removed >= SPIKE_MIN:
                    nodes_removed_spikes += 1
                    if csv_nodes_removed > max_nodes_removed:
                        max_nodes_removed = csv_nodes_removed
                    if csv_nodes_removed < min_nodes_removed:
                        min_nodes_removed = csv_nodes_removed
                if csv_attribute_changed >= SPIKE_MIN:
                    attribute_changed_spikes += 1
                    if csv_attribute_changed > max_attribute_changed:
                        max_attribute_changed = csv_attribute_changed
                    if csv_attribute_changed < min_attribute_changed:
                        min_attribute_changed = csv_attribute_changed
                if csv_text_changed >= SPIKE_MIN:
                    text_changed_spikes += 1
                    if csv_text_changed > max_text_changed:
                        max_text_changed = csv_text_changed
                    if csv_text_changed < min_text_changed:
                        min_text_changed = csv_text_changed

        feature_dict["blocked_spikes"] = blocked_spikes
        feature_dict["wr_blocked_spikes"] = wr_blocked_spikes
        feature_dict["elem_blocked_spikes"] = elem_blocked_spikes
        feature_dict["snippet_blocked_spikes"] = snippet_blocked_spikes
        feature_dict["nodes_added_spikes"] = nodes_added_spikes
        feature_dict["nodes_removed_spikes"] = nodes_removed_spikes
        feature_dict["attribute_changed_spikes"] = attribute_changed_spikes
        feature_dict["text_changed_spikes"] = text_changed_spikes
        feature_dict["max_nodes_added"] = max_nodes_added
        feature_dict["min_nodes_added"] = min_nodes_added
        feature_dict["max_nodes_removed"] = max_nodes_removed
        feature_dict["min_nodes_removed"] = min_nodes_removed
        feature_dict["max_attribute_changed"] = max_attribute_changed
        feature_dict["min_attribute_changed"] = min_attribute_changed
        feature_dict["max_text_changed"] = max_text_changed
        feature_dict["min_text_changed"] = min_text_changed

        return rows, feature_dict

    def get_trial_features(self, trial, time_series):
        _MIN = 10000
        features_dict = {}

        rows, features = self.get_simple_trial_features(trial, time_series)

        last_time = 0
        # Find last spike
        if len(rows) > 0:
            # find last spike
            for index, row in enumerate(reversed(rows)):
                if int(row[CSV_TOTAL_CHANGES]) >= SPIKE_MIN:
                    last_time = int(row[CSV_BIN_NORM])
                    break

        features_dict[TIMESERIES_KEY + "_last_time"] = last_time

        # AVG cluster size
        variant_cluster_len_avg__nodes_added = self.get_avg_cluster_size(
            CSV_NODES_ADDED, rows)
        variant_cluster_len_avg__nodes_removed = self.get_avg_cluster_size(
            CSV_NODES_REMOVED, rows)
        variant_cluster_len_avg__attr_changed = self.get_avg_cluster_size(
            CSV_ATTRIBUTE_CHANGED, rows)
        variant_cluster_len_avg__text_changed = self.get_avg_cluster_size(
            CSV_TEXT_CHANGED, rows)

        features_dict[TIMESERIES_KEY + CSV_NODES_ADDED +
                      "_cluster_avg"] = variant_cluster_len_avg__nodes_added
        features_dict[TIMESERIES_KEY + CSV_NODES_REMOVED +
                      "_cluster_avg"] = variant_cluster_len_avg__nodes_removed
        features_dict[TIMESERIES_KEY + CSV_ATTRIBUTE_CHANGED +
                      "_cluster_avg"] = variant_cluster_len_avg__attr_changed
        features_dict[TIMESERIES_KEY + CSV_TEXT_CHANGED +
                      "_cluster_avg"] = variant_cluster_len_avg__text_changed

        #features_dict[TIMESERIES_KEY + "_spikes_after_block"] = get_avg_time_after_blocked(IMPORTANT_CSV_KEYS, CSV_BLOCKED, rows, after_event_key="dom_content_loaded")
        features_dict[
            TIMESERIES_KEY +
            "_spikes_after_domloaded"] = get_avg_time_after_domcontentloaded(
                rows)

        # how many spikes happened after last iframe was loaded
        features_dict[
            TIMESERIES_KEY +
            "_spikes_after_last_iframe"] = get_count_of_events_after_last_event(
                IMPORTANT_CSV_KEYS, CSV_IFRAME_SRC_CHANGED, rows)
        # how many spikes happened after last iframe was blocked
        features_dict[
            TIMESERIES_KEY +
            "_spikes_after_last_iframe_blocked"] = get_count_of_events_after_last_event(
                IMPORTANT_CSV_KEYS, CSV_IFRAME_BLOCKED, rows)

        # MISC FEATURES (by half)
        misc_features_blocked = self.get_event_frequency_by_half_features(
            CSV_BLOCKED, rows)
        for feature_key in misc_features_blocked:
            features_dict[TIMESERIES_KEY + CSV_BLOCKED + "_" +
                          feature_key] = misc_features_blocked.get(feature_key)

        # EVENT FREQ by fifths
        for csv_feature in [
                CSV_BLOCKED, CSV_ELEM_BLOCKED, CSV_NODES_ADDED,
                CSV_NODES_REMOVED, CSV_ATTRIBUTE_CHANGED, CSV_TEXT_CHANGED
        ]:
            event_features_freq_dict = self.get_event_frequency_by_fifths_features(
                csv_feature, rows)
            for feature_key in event_features_freq_dict:
                features_dict[TIMESERIES_KEY + csv_feature + "_" +
                              feature_key] = event_features_freq_dict.get(
                                  feature_key)

        # SIMPLE FEFATURES BELOW VARIANT
        features_dict[TIMESERIES_KEY + CSV_BLOCKED +
                      "_spikes"] = features.get("blocked_spikes")
        features_dict[TIMESERIES_KEY + CSV_WR_BLOCKED +
                      "_spikes"] = features.get("wr_blocked_spikes")
        features_dict[TIMESERIES_KEY + CSV_ELEM_BLOCKED +
                      "_spikes"] = features.get("elem_blocked_spikes")
        features_dict[TIMESERIES_KEY + CSV_SNIPPET_BLOCKED +
                      "_spikes"] = features.get("snippet_blocked_spikes")
        features_dict[TIMESERIES_KEY + CSV_NODES_ADDED +
                      "_spikes"] = features.get("nodes_added_spikes")
        features_dict[TIMESERIES_KEY + CSV_NODES_ADDED +
                      "_max"] = features.get("max_nodes_added")
        features_dict[TIMESERIES_KEY + CSV_NODES_ADDED +
                      "_min"] = features.get(
                          "min_nodes_added"
                      ) if features.get("min_nodes_added") != _MIN else 0
        features_dict[TIMESERIES_KEY + CSV_NODES_REMOVED +
                      "_spikes"] = features.get("nodes_removed_spikes")
        features_dict[TIMESERIES_KEY + CSV_NODES_REMOVED +
                      "_max"] = features.get("max_nodes_removed")
        features_dict[TIMESERIES_KEY + CSV_NODES_REMOVED +
                      "_min"] = features.get(
                          "min_nodes_removed"
                      ) if features.get("min_nodes_removed") != _MIN else 0
        features_dict[TIMESERIES_KEY + CSV_ATTRIBUTE_CHANGED +
                      "_spikes"] = features.get("attribute_changed_spikes")
        features_dict[TIMESERIES_KEY + CSV_ATTRIBUTE_CHANGED +
                      "_max"] = features.get("max_attribute_changed")
        features_dict[TIMESERIES_KEY + CSV_ATTRIBUTE_CHANGED +
                      "_min"] = features.get(
                          "min_attribute_changed"
                      ) if features.get("min_attribute_changed") != _MIN else 0
        features_dict[TIMESERIES_KEY + CSV_TEXT_CHANGED +
                      "_spikes"] = features.get("text_changed_spikes")
        features_dict[TIMESERIES_KEY + CSV_TEXT_CHANGED +
                      "_max"] = features.get("max_text_changed")
        features_dict[TIMESERIES_KEY + CSV_TEXT_CHANGED +
                      "_min"] = features.get(
                          "min_text_changed"
                      ) if features.get("min_text_changed") != _MIN else 0

        return features_dict

    def extract_features(self):

        features_dict = {}

        if self.time_series_dict is None:
            return features_dict

        trials_features = []
        trials_features_ctr = []
        for trial_index in range(self.trials):
            trial_label = get_trial_label(trial_index)
            control_ts_name = CONTROL + " " + trial_label
            variant_ts_name = VARIANT + " " + trial_label
            control_time_series = self.time_series_dict.get(control_ts_name)
            variant_time_series = self.time_series_dict.get(variant_ts_name)

            if control_time_series and variant_time_series:
                ctr_trial_features = self.get_trial_features(
                    trial_label, control_time_series)
                trials_features_ctr.append(ctr_trial_features)

                var_trial_features = self.get_trial_features(
                    trial_label, variant_time_series)
                trials_features.append(var_trial_features)

        # here we assume there should be at least one
        if len(trials_features) != self.trials or len(
                trials_features_ctr) != self.trials:
            logger.warn("Could not calculate features for ts %s" %
                        self.crawl_url)
            return features_dict

        feature_keys = trials_features[0].keys()
        for key in feature_keys:
            feature_arr_var = []
            feature_arr_ctr = []
            for indiv_trial_features in trials_features:
                feature_arr_var.append(indiv_trial_features.get(key))
            for indiv_trial_features in trials_features_ctr:
                feature_arr_ctr.append(indiv_trial_features.get(key))

            # for every feature key, we get the average and put in the final features dict
            if len(feature_arr_var) > 0:
                var_avg = round(np.average(feature_arr_var), 2)
                ctr_avg = np.average(feature_arr_ctr)
                features_dict[key] = var_avg
                # block features do not have diffs since control does not have block events
                if "block" not in key:
                    features_dict[key + "_diff"] = round(var_avg - ctr_avg, 2)
            else:
                features_dict[key] = 0
                if "block" not in key:
                    features_dict[key + "_diff"] = 0

        return features_dict


class DOMMutationFeatureExtraction(BaseCVFeatureExtraction):

    NEW_LINE = "\n"
    NEW_LINE_KEY = "new_line"
    CHARACTER_CHANGE_KEY = "characters"
    WORDS_KEY = "words"

    TEXT_CHANGE_KEYS = [NEW_LINE_KEY, CHARACTER_CHANGE_KEY, WORDS_KEY]

    # maybe add "#text" and "(text)" later
    NODE_ADDED_KEYS = [
        "script",
        "img",
        "div",
        "a",
        "header",
        "iframe",
        "audio",
        "video",
        "span",
        "link",
        "style",
        "canvas",
        "rect",
        "circle",
        "section",
        "textarea",
        "option",
        "svg",
        "line",
        "text",
        "image",
        "table",
        "path",
        "g",
        "td",
        "tr",
        "tbody",
        "ul",
        "li",
        "form",
        "input",
        "select",
        "fieldset",
        "center",
        "br",
        "hr",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "small",
        "button",
        "noscript",
        "meta",
        "embed",
        "picture",
        "article",
        "#comment",
        "aside",
        "nav",
        "footer",
        "p",
        "i",
        "source",
        "symbol",
        "null",
        "figure",
        "ins",
        "b",
    ]

    ATTR_CHANGED_CSS_KEYS = [
        "display", "visibility", "opacity", "height", "width", "background",
        "max-height", "max-width", "important", "padding", "margin",
        "position", "overflow", "min-width", "transform", "text-align",
        "bottom", "top", "left", "right", "float", "z-index",
        "background-image", "margin-left", "margin-right", "text-anchor",
        "fill", "stroke", "font-weight", "font-size", "overflow-y",
        "overflow-x", "box-sizing", "min-height", "cursor", "padding-right",
        "padding-left", "background-color", "padding-top", "padding-bottom",
        "zoom", "margin-top", "color", "vertical-align", "margin-bottom",
        "margin-top", "margin-right", "margin-left"
    ]

    ATTR_CHANGED_CSS_KEYS_IGNORE = [
        "place", "align", "object", "user-select", "clear", "flex", "filter",
        "line-height", "border", "box-shadow", "white-space",
        "text-decoration", "letter-spacing", "align-self", "ins",
        "font-family", "animation", "-webkit", "-moz", "transition",
        "transform"
    ]

    ATTR_CHANGED_OTHER_KEY = "other"
    # keys that we care about
    ATTR_CHANGED_KEYS = ["id", "style", "src", "class", "data", "href"]

    KEY_ATTR_CHANGED_MODIFY = "_modify_"

    # we pass in the entire document for variant, not a subset
    def __init__(self, crawl_url, variant_dommutation_doc, log_prefix="", trials=4):
        BaseCVFeatureExtraction.__init__(self,
                                         crawl_url,
                                         None,
                                         None,
                                         docs_as_dict=True,
                                         trials=trials)
        self.variant_dommutation_doc = variant_dommutation_doc
        self.node_change_added_type_count = dict()
        self.node_change_removed_type_count = dict()
        self.attribute_change_type_count = dict()
        self.text_change_type_count = dict()
        # this is just a count since text nodes are just one type
        self.text_node_added_type_count = 0
        self.text_node_removed_type_count = 0

        self.log_prefix = log_prefix

    def increment_node_change_added_type(self, node_type):
        node_type_lower = node_type.lower()
        if node_type_lower not in self.node_change_added_type_count:
            self.node_change_added_type_count[node_type_lower] = 0
        self.node_change_added_type_count[node_type_lower] += 1

    def increment_text_node_added_type(self):
        self.text_node_added_type_count += 1

    def increment_text_node_removed_type(self):
        self.text_node_removed_type_count += 1

    def increment_node_change_removed_type(self, node_type):
        node_type_lower = node_type.lower()
        if node_type_lower not in self.node_change_removed_type_count:
            self.node_change_removed_type_count[node_type_lower] = 0
        self.node_change_removed_type_count[node_type_lower] += 1

    def increment_text_change_type(self, text_change_value):
        if text_change_value is None or len(text_change_value) == 0:
            return

        self.add_log("%s - Text change %s" %
                     (str(self.log_prefix), str(text_change_value)))

        # there can be multiple new lines
        new_line_count = text_change_value.count(self.NEW_LINE)
        if self.NEW_LINE_KEY not in self.text_change_type_count:
            self.text_change_type_count[self.NEW_LINE_KEY] = 0
        self.text_change_type_count[self.NEW_LINE_KEY] += new_line_count

        # character count
        char_count = len(text_change_value)
        if self.CHARACTER_CHANGE_KEY not in self.text_change_type_count:
            self.text_change_type_count[self.CHARACTER_CHANGE_KEY] = 0
        self.text_change_type_count[self.CHARACTER_CHANGE_KEY] += char_count

        # find word count by splitting by space
        word_split = text_change_value.split(" ")
        # remove empty words
        word_split = [x for x in word_split if len(x.strip()) > 0]
        # get word count
        word_count = len(word_split)
        if self.WORDS_KEY not in self.text_change_type_count:
            self.text_change_type_count[self.WORDS_KEY] = 0
        self.text_change_type_count[self.WORDS_KEY] += word_count

    def get_attribute_change_set(self, attribute_change_events, defining_keys):
        attr_events_filtered = dict()
        defining_keys_filtered = dict()

        # this time we only count an change with the same oldValue and newValue combo
        for event, defining_key in zip(attribute_change_events, defining_keys):

            key, _ = get_attribute_changed_key(event.get("event"),
                                               use_compare_values=True)
            if key not in attr_events_filtered:
                attr_events_filtered[key] = event
                defining_keys_filtered[key] = defining_key

        #flatten each list
        attr_events_list = []
        defining_keys_list = []
        for key in attr_events_filtered:
            attr_events_list.append(attr_events_filtered.get(key))
            defining_keys_list.append(defining_keys_filtered.get(key))

        return attr_events_list, defining_keys_list

    def extract_features(self):
        DOM_FEATURE_PREFIX = "dom_"

        features_dict = {}

        # get node changes count
        node_added_list = self.variant_dommutation_doc.get("node_added") or []
        node_added_keys = self.variant_dommutation_doc.get(
            "node_added_defining") or []
        logger.debug("%s - BEFORE Node added events %d" %
                     (self.log_prefix, len(node_added_list)))

        node_added_considered_events = 0
        if len(node_added_list) > 0:
            # get the necessary events
            for event_and_index, node_added_key in zip(node_added_list,
                                                       node_added_keys):
                event, event_index = event_and_index
                event_inner_obj = event.get("event")

                node_add_type = get_nodes_added__node_name(
                    event_inner_obj, event_index)
                self.increment_node_change_added_type(node_add_type)
                node_added_considered_events += 1

        logger.debug("%s - AFTER Node added events %d" %
                     (self.log_prefix, node_added_considered_events))

        node_removed_list = self.variant_dommutation_doc.get(
            "node_removed") or []
        node_removed_keys = self.variant_dommutation_doc.get(
            "node_removed_defining") or []
        logger.debug("%s - BEFORE Node removed events %d" %
                     (self.log_prefix, len(node_removed_list)))

        node_removed_considered_events = 0
        if len(node_removed_list) > 0:
            # get the necessary events
            for event_and_index, _ in zip(node_removed_list,
                                          node_removed_keys):
                event, event_index = event_and_index
                event_inner_obj = event.get("event")

                node_type = get_nodes_added__node_name(event_inner_obj,
                                                       event_index)
                self.increment_node_change_removed_type(node_type)
                node_removed_considered_events += 1

        logger.debug("%s - AFTER Node removed events %d" %
                     (self.log_prefix, node_removed_considered_events))

        #print(self.node_change_type_count)
        NODE_CHANGE_ADDED_FEATURE_PREFIX = "node_changed_added__"
        NODE_CHANGE_REMOVED_FEATURE_PREFIX = "node_changed_removed__"

        # init the keys into features_dict
        for key in self.NODE_ADDED_KEYS:
            node_added_feature_key = DOM_FEATURE_PREFIX + NODE_CHANGE_ADDED_FEATURE_PREFIX + key.lower(
            )
            if node_added_feature_key not in features_dict:
                features_dict[node_added_feature_key] = 0
            node_removed_feature_key = DOM_FEATURE_PREFIX + NODE_CHANGE_REMOVED_FEATURE_PREFIX + key.lower(
            )
            if node_removed_feature_key not in features_dict:
                features_dict[node_removed_feature_key] = 0

        ### bring values over to the features_dict
        node_change_added__others_count = 0  #keep track of misc

        # for node ADD changes
        for key in self.node_change_added_type_count:
            value_count = self.node_change_added_type_count.get(key)
            is_other = True

            for node_added_key in self.NODE_ADDED_KEYS:
                if key.lower() == node_added_key:
                    feature_key = NODE_CHANGE_ADDED_FEATURE_PREFIX + node_added_key
                    features_dict[DOM_FEATURE_PREFIX +
                                  feature_key] += value_count
                    is_other = False

            if is_other:
                logger.debug("Other node found: %s" % key)
                node_change_added__others_count += value_count

        features_dict[DOM_FEATURE_PREFIX + NODE_CHANGE_ADDED_FEATURE_PREFIX +
                      "others"] = node_change_added__others_count
        features_dict[DOM_FEATURE_PREFIX + NODE_CHANGE_ADDED_FEATURE_PREFIX +
                      "total"] = sum([
                          x
                          for x in self.node_change_added_type_count.values()
                      ])

        # node REMOVAL changes
        node_change_removed__others_count = 0  #keep track of misc
        node_change_removed__ad_count = 0  #keep track of node changes with words "ad"

        for key in self.node_change_removed_type_count:
            value_count = self.node_change_removed_type_count.get(key)
            is_other = True

            for node_added_key in self.NODE_ADDED_KEYS:
                if key.lower() == node_added_key:
                    feature_key = NODE_CHANGE_REMOVED_FEATURE_PREFIX + node_added_key
                    features_dict[DOM_FEATURE_PREFIX +
                                  feature_key] += value_count
                    is_other = False

            if is_other:
                node_change_removed__others_count += value_count

            if "ad" in key:
                node_change_removed__ad_count += value_count

        features_dict[DOM_FEATURE_PREFIX + NODE_CHANGE_REMOVED_FEATURE_PREFIX +
                      "others"] = node_change_removed__others_count
        features_dict[
            DOM_FEATURE_PREFIX + NODE_CHANGE_REMOVED_FEATURE_PREFIX +
            "total"] = sum(
                [x for x in self.node_change_removed_type_count.values()])

        # init the keys into features_dict
        ATTR_CHANGE_FEATURE_PREFIX = "attribute_changed__"
        for key in self.ATTR_CHANGED_CSS_KEYS:
            feature_key = DOM_FEATURE_PREFIX + ATTR_CHANGE_FEATURE_PREFIX + key.lower(
            )
            if feature_key not in features_dict:
                features_dict[feature_key] = 0
        for key in self.ATTR_CHANGED_KEYS:
            feature_key = DOM_FEATURE_PREFIX + ATTR_CHANGE_FEATURE_PREFIX + key.lower(
            )
            if feature_key not in features_dict:
                features_dict[feature_key] = 0
        for key in self.ATTR_CHANGED_CSS_KEYS:
            feature_key = DOM_FEATURE_PREFIX + ATTR_CHANGE_FEATURE_PREFIX + self.KEY_ATTR_CHANGED_MODIFY + key.lower(
            )
            if feature_key not in features_dict:
                features_dict[feature_key] = 0
        features_dict[DOM_FEATURE_PREFIX + ATTR_CHANGE_FEATURE_PREFIX +
                      self.ATTR_CHANGED_OTHER_KEY] = 0

        # get attribute change counts
        attribute_changed_list = self.variant_dommutation_doc.get(
            "attribute_changed") or []
        attribute_changed_keys = self.variant_dommutation_doc.get(
            "attribute_changed_defining") or []

        # filter them down to sets
        attribute_changed_list, attribute_changed_keys = self.get_attribute_change_set(
            attribute_changed_list, attribute_changed_keys)

        logger.debug("%s - BEFORE Attribute change events %d" %
                     (self.log_prefix, len(attribute_changed_list)))

        actual_event_count = 0
        if len(attribute_changed_list) > 0:
            # get the necessary events
            for event, defining_key in zip(attribute_changed_list,
                                           attribute_changed_keys):
                event_inner_obj = event.get("event")

                attr, old_val, new_val = get_attribute_changed_info(
                    event_inner_obj)
                if attr:
                    actual_event_count += 1

        logger.debug("%s - AFTER Attribute change events %d" %
                     (self.log_prefix, actual_event_count))

        for key in self.attribute_change_type_count:
            feature_key = DOM_FEATURE_PREFIX + ATTR_CHANGE_FEATURE_PREFIX + key.lower(
            )
            value_count = self.attribute_change_type_count.get(key)
            features_dict[feature_key] += value_count
        features_dict[DOM_FEATURE_PREFIX + ATTR_CHANGE_FEATURE_PREFIX +
                      "total"] = sum([
                          x for x in self.attribute_change_type_count.values()
                      ])

        # init the keys into features_dict
        TEXT_CHANGE_FEATURE_PREFIX = "text_changed__"
        for key in self.TEXT_CHANGE_KEYS:
            feature_key = DOM_FEATURE_PREFIX + TEXT_CHANGE_FEATURE_PREFIX + key.lower(
            )
            if feature_key not in features_dict:
                features_dict[feature_key] = 0

        # get text change counts
        text_changed_list = self.variant_dommutation_doc.get(
            "text_changed") or []
        for _, text_diff in text_changed_list:
            self.increment_text_change_type(text_diff)

        text_node_added_list = self.variant_dommutation_doc.get(
            "text_node_added") or []
        for event in text_node_added_list:
            self.increment_text_node_added_type()

        text_node_removed_list = self.variant_dommutation_doc.get(
            "text_node_removed") or []
        for event in text_node_removed_list:
            self.increment_text_node_removed_type()

        #print(self.text_change_type_count)

        for key in self.text_change_type_count:
            feature_key = DOM_FEATURE_PREFIX + TEXT_CHANGE_FEATURE_PREFIX + key.lower(
            )
            value_count = self.text_change_type_count.get(key)
            features_dict[feature_key] += value_count
        features_dict[DOM_FEATURE_PREFIX + TEXT_CHANGE_FEATURE_PREFIX +
                      "total"] = sum(self.text_change_type_count.values())

        features_dict[DOM_FEATURE_PREFIX + TEXT_CHANGE_FEATURE_PREFIX +
                      "node_added"] = self.text_node_added_type_count
        features_dict[DOM_FEATURE_PREFIX + TEXT_CHANGE_FEATURE_PREFIX +
                      "node_removed"] = self.text_node_removed_type_count

        # get block event counts
        # init the keys into features_dict
        BLOCKED_ELEMENTS_FEATURE_PREFIX = "blocked_elements__"
        for key in self.NODE_ADDED_KEYS:
            feature_key = DOM_FEATURE_PREFIX + BLOCKED_ELEMENTS_FEATURE_PREFIX + key.lower(
            )
            if feature_key not in features_dict:
                features_dict[feature_key] = 0

        # blocked_events is a dict() of lists
        blocked_events = self.variant_dommutation_doc.get("blocked_events")
        blocked_element_count = dict()
        for elem_tag in self.NODE_ADDED_KEYS:
            blocked_element_count[elem_tag] = 0

        for block_event_key in blocked_events:
            # Note: blocked events here are only attribute changes events
            if ABP_BLOCKED_ELEMENT in block_event_key:
                for event_tuple in blocked_events.get(block_event_key):
                    _, event, _, _ = event_tuple
                    event_inner_obj = event.get("event")
                    target_type = event_inner_obj["targetType"] or ""
                    target_type = target_type.lower()
                    if target_type in self.NODE_ADDED_KEYS:
                        blocked_element_count[target_type] += 1

        for key in blocked_element_count:
            feature_key = DOM_FEATURE_PREFIX + BLOCKED_ELEMENTS_FEATURE_PREFIX + key.lower(
            )
            value_count = blocked_element_count.get(key)
            features_dict[feature_key] += value_count

        return features_dict


class WebRequestsFeatureExtraction(BaseCVFeatureExtraction):

    media_extensions = [
        ".jpg", ".png", ".jpeg", ".gif", ".tif", ".woff", ".woff2", ".JPG",
        ".PNG", ".JPEG", ".GIF", ".TIF", ".WOFF", ".WOFF2"
    ]

    ignore_slds = [
        "doubleclick.net", "twitter.com", "facebook.com", "addthis.com",
        "google.com"
    ]

    resource_types = [
        "main_frame", "sub_frame", "stylesheet", "script", "image", "font",
        "object", "xmlhttprequest", "ping", "csp_report", "media", "websocket",
        "other", "unknown"
    ]

    def __init__(self,
                 crawl_url,
                 control_only_docs,
                 variant_only_docs,
                 blocked_requests=None,
                 mismatch_resources=None,
                 content_type_resources=None,
                 resource_type_resources=None,
                 ctr_blocked_requests=None,
                 ctr_content_type_resources=None,
                 ctr_resource_type_resources=None,
                 ctr_mismatch_resources=None,
                 ignore_media_requests=True,
                 ctr_diff_obj=None,
                 var_diff_obj=None,
                 adblock_parser=None,
                 log_prefix="",
                 urls_collector_queue=None,
                 urls_collector_queue_control=None,
                 tracking_dict=None,
                 trials=4):
        BaseCVFeatureExtraction.__init__(self, crawl_url, control_only_docs,
                                         variant_only_docs, trials=trials)
        self.control_requests_orig = []
        self.variant_requests_orig = []
        self.control_first_party_requests = []
        self.control_third_party_requests = []
        self.variant_first_party_requests = []
        self.variant_third_party_requests = []
        self.log_prefix = log_prefix
        self.adblock_parser = adblock_parser
        self.urls_collector_queue = urls_collector_queue
        self.urls_collector_queue_control = urls_collector_queue_control
        self.tracking_dict = tracking_dict

        # variant
        self.variant_first_party_mismatch_resources = []
        self.variant_third_party_mismatch_resources = []
        self.blocked_requests = blocked_requests or []
        self.mismatch_resources = mismatch_resources or []
        if content_type_resources:
            content_type_resources = content_type_resources
        self.content_type_resources = content_type_resources or dict()
        if resource_type_resources:
            resource_type_resources = resource_type_resources
        self.resource_type_resources = resource_type_resources or dict()
        self.ignore_media_requests = ignore_media_requests
        self.var_diff_obj = var_diff_obj

        # control
        self.ctr_blocked_requests = ctr_blocked_requests or []
        if ctr_content_type_resources:
            ctr_content_type_resources = ctr_content_type_resources
        self.ctr_content_type_resources = ctr_content_type_resources or dict()
        if ctr_resource_type_resources:
            ctr_resource_type_resources = ctr_resource_type_resources
        self.ctr_resource_type_resources = ctr_resource_type_resources or dict(
        )
        self.ctr_mismatch_resources = ctr_mismatch_resources or []
        self.ctr_diff_obj = ctr_diff_obj

    def is_ignored_slds(self, url_tld):
        sld = get_second_level_domain_from_tld(url_tld)
        if sld in self.ignore_slds:
            return True

    def is_request_blocked(self, request, is_control=False):
        if not is_control:
            if self.blocked_requests:
                if request in self.blocked_requests:
                    #print("Found blocked request " + request)
                    return True
        else:
            if self.ctr_blocked_requests:
                if request in self.ctr_blocked_requests:
                    #print("Found blocked request " + request)
                    return True
        return False

    def extract_features(self):
        features_dict = {}
        client, db = get_anticv_client_and_db()

        _, control_first_party_requests, control_third_party_requests = find_all_first_and_third_party_webrequests(
            self.crawl_url, self.control_only_docs)

        _, variant_first_party_requests, variant_third_party_requests = find_all_first_and_third_party_webrequests(
            self.crawl_url, self.variant_only_docs)

        # break things down into control and variant and tlds and urls
        control_first_party_tlds = []
        control_third_party_tlds = []
        control_fp_requests = []
        control_tp_requests = []
        variant_first_party_tlds = []
        variant_third_party_tlds = []
        variant_fp_requests = []
        variant_tp_requests = []

        control_only_domains = []
        variant_only_domains = []

        control_all_requests = []
        variant_all_requests = []
        for request, url_tld in control_first_party_requests:
            tld = get_domain_only_from_tld(url_tld)
            self.control_requests_orig.append(tld)
            control_only_domains.append(tld)
            control_all_requests.append(request)

        for request, url_tld in control_third_party_requests:
            tld = get_domain_only_from_tld(url_tld)
            self.control_requests_orig.append(tld)
            control_only_domains.append(tld)
            control_all_requests.append(request)

        for request, url_tld in variant_first_party_requests:
            tld = get_domain_only_from_tld(url_tld)
            self.variant_requests_orig.append(tld)
            variant_only_domains.append(tld)
            variant_all_requests.append(request)

        for request, url_tld in variant_third_party_requests:
            tld = get_domain_only_from_tld(url_tld)
            self.variant_requests_orig.append(tld)
            variant_only_domains.append(tld)
            variant_all_requests.append(request)

        control_all_requests = list(set(control_all_requests))
        variant_all_requests = list(set(variant_all_requests))

        control_only_requests_set = set(control_all_requests) - set(
            variant_all_requests)
        variant_only_requests_set = set(variant_all_requests) - set(
            control_all_requests)

        # save an original
        original_variant_only_requests_set = variant_only_requests_set

        # special edge case to save requests that are new even though its refer is no longer in the diff set
        # this means that the request spawned only due to perhaps the adblocker
        save_requests = []
        for req in variant_only_requests_set:
            _, req_obj, _ = self.resource_type_resources.get(req)
            initiator_url = get_webrequest_detail_value(req_obj, "initiator")
            if initiator_url:
                req_domain = get_domain_only_from_url(initiator_url)
                req_path, _ = get_path_and_query_params(initiator_url)
                initiator_domain_path = req_domain + req_path
                self.add_log("\nVAR Found initiator requests: " +
                             initiator_domain_path)

                for var_req in variant_only_requests_set:
                    if var_req != req and initiator_domain_path in var_req:
                        save_requests.append(req)
                        self.add_log("\nVAR Save Requests: " + var_req)
                        break

        control_only_requests_set = filter_requests_by_header(
            control_only_requests_set,
            self.ctr_resource_type_resources,
            self.ctr_content_type_resources,
            self.crawl_url,
            log_prefix=self.log_prefix + "_control")
        self.add_log("\nCTR Filter Requests: " +
                     " ,\n".join(control_only_requests_set))

        variant_only_requests_set = filter_requests_by_header(
            variant_only_requests_set,
            self.resource_type_resources,
            self.content_type_resources,
            self.crawl_url,
            log_prefix=self.log_prefix + "_variant")
        self.add_log("\nVar  Filter requests: " +
                     " ,\n".join(variant_only_requests_set))

        # re-add back the saved request
        for saved_req in save_requests:
            if saved_req not in variant_only_requests_set:
                variant_only_requests_set.append(saved_req)

        crawl_url_tld = extract_tld(self.crawl_url)
        crawl_url_sld = get_second_level_domain_from_tld(crawl_url_tld)

        # there is a queue to write urls
        urls_collector_delimiter = ";;"
        if self.urls_collector_queue:
            for req in variant_only_requests_set:
                resource_type = "None"
                _, _, misc_types = self.resource_type_resources.get(req)
                if misc_types:
                    resource_type = misc_types.get("resource_type")
                self.urls_collector_queue.put(self.crawl_url +
                                              urls_collector_delimiter +
                                              crawl_url_sld +
                                              urls_collector_delimiter + req +
                                              urls_collector_delimiter +
                                              resource_type)

        if self.urls_collector_queue_control:
            for req in control_only_requests_set:
                resource_type = "None"
                _, _, misc_types = self.ctr_resource_type_resources.get(req)
                if misc_types:
                    resource_type = misc_types.get("resource_type")
                self.urls_collector_queue_control.put(
                    self.crawl_url + urls_collector_delimiter + crawl_url_sld +
                    urls_collector_delimiter + req + urls_collector_delimiter +
                    resource_type)

        # if the tracking domains are already known, then check it
        # structure of tracking_dict is main_url -> dict() of urls -> their resources type.
        # must match all to consider blocked main_url + url + resource type
        if self.tracking_dict:
            logger.debug(
                "\n%s - Before removing tracking: control: %d, variant: %d\n" %
                (str(self.log_prefix), len(control_only_requests_set),
                 len(variant_only_requests_set)))
            self.add_log(
                "\n%s - Before removing tracking: control: %d, variant: %d\n" %
                (str(self.log_prefix), len(control_only_requests_set),
                 len(variant_only_requests_set)))

            tracking_domains = self.tracking_dict.get(crawl_url_sld)
            if not tracking_domains:
                logger.debug("Did not find any tracking domains for %s" %
                               crawl_url_sld)
            if tracking_domains:
                variant_only_requests_set_no_tracking = []
                for req in variant_only_requests_set:
                    resource_type = "None"
                    _, _, misc_types = self.resource_type_resources.get(req)
                    if misc_types:
                        resource_type = misc_types.get("resource_type")
                    tracking_resource = tracking_domains.get(req)
                    if not tracking_resource:
                        variant_only_requests_set_no_tracking.append(req)
                    elif tracking_resource != resource_type:
                        variant_only_requests_set_no_tracking.append(req)

                variant_only_requests_set = variant_only_requests_set_no_tracking
            logger.debug(
                "\n%s - After removing tracking: control: %d, variant: %d\n" %
                (str(self.log_prefix), len(control_only_requests_set),
                 len(variant_only_requests_set)))
            self.add_log(
                "\n%s - After removing tracking: control: %d, variant: %d\n" %
                (str(self.log_prefix), len(control_only_requests_set),
                 len(variant_only_requests_set)))

        self.add_log("\nCTR Filter Requests After Tracking: " +
                     " ,\n".join(control_only_requests_set))
        self.add_log("\nVar Filter requests After Tracking: " +
                     " ,\n".join(variant_only_requests_set))

        # if domain is part of the uniques set, then add it to first party/ third party sets + requests
        for request, url_tld in control_first_party_requests:
            if not self.is_request_blocked(
                    request,
                    is_control=True) and request in control_only_requests_set:
                control_first_party_tlds.append(url_tld)
                control_fp_requests.append(request)
        for request, url_tld in control_third_party_requests:
            if not self.is_request_blocked(
                    request,
                    is_control=True) and request in control_only_requests_set:
                control_third_party_tlds.append(url_tld)
                control_tp_requests.append(request)

        # IMPORTANT: for variant, we want to ignore all requests that were blocked from EasyList
        for request, url_tld in variant_first_party_requests:
            if not self.is_request_blocked(
                    request) and request in variant_only_requests_set:
                if not self.is_ignored_slds(url_tld):
                    variant_first_party_tlds.append(url_tld)
                    variant_fp_requests.append(request)
        for request, url_tld in variant_third_party_requests:
            if not self.is_request_blocked(
                    request) and request in variant_only_requests_set:
                if not self.is_ignored_slds(url_tld):
                    variant_third_party_tlds.append(url_tld)
                    variant_tp_requests.append(request)

        self.control_requests_orig = list(set(self.control_requests_orig))
        self.variant_requests_orig = list(set(self.variant_requests_orig))
        self.control_first_party_requests = control_fp_requests
        self.control_third_party_requests = control_tp_requests
        self.variant_first_party_requests = variant_fp_requests
        self.variant_third_party_requests = variant_tp_requests

        # turn all tld lists into sets
        control_first_party_tlds = list(set(control_first_party_tlds))
        control_third_party_tlds = list(set(control_third_party_tlds))

        variant_first_party_tlds = list(set(variant_first_party_tlds))
        variant_third_party_tlds = list(set(variant_third_party_tlds))

        control_first_party_tlds_set = set(control_first_party_tlds)
        control_third_party_tlds_set = set(control_third_party_tlds)

        variant_first_party_tlds_set = set(variant_first_party_tlds)
        variant_third_party_tlds_set = set(variant_third_party_tlds)

        # entire urls count between control and variant for first/third party
        features_dict["control_first_party_count"] = len(control_fp_requests)
        features_dict["control_third_party_count"] = len(control_tp_requests)
        features_dict["variant_first_party_count"] = len(variant_fp_requests)
        features_dict["variant_third_party_count"] = len(variant_tp_requests)

        numbers_count_in_subdomain__fp = 0
        dash_count_in_subdomain__fp = 0
        for variant_tld in variant_first_party_tlds:
            numbers_count_in_subdomain__fp += sum(
                c.isdigit() for c in variant_tld.subdomain)
            dash_count_in_subdomain__fp += sum(c == "-"
                                               for c in variant_tld.subdomain)

        numbers_count_in_subdomain__tp = 0
        dash_count_in_subdomain__tp = 0
        for variant_tld in variant_third_party_tlds:
            numbers_count_in_subdomain__tp += sum(
                c.isdigit() for c in variant_tld.subdomain)
            dash_count_in_subdomain__tp += sum(c == "-"
                                               for c in variant_tld.subdomain)

        features_dict[
            "variant_first_party_number_in_subdomain_count"] = numbers_count_in_subdomain__fp
        features_dict[
            "variant_third_party_number_in_subdomain_count"] = numbers_count_in_subdomain__tp

        features_dict[
            "variant_first_party_dash_in_subdomain_count"] = dash_count_in_subdomain__fp
        features_dict[
            "variant_third_party_dash_in_subdomain_count"] = dash_count_in_subdomain__tp

        PATH_LENGTH_THRESHOLD = 0  #5
        path_numbers_count__fp = 0
        path_special_chars_count__fp = 0
        path_capitalized_chars_count__fp = 0
        path_lowercase_chars_count__fp = 0
        # counts encountered from one request
        path_numbers_count__fp_max = 0
        path_special_chars_count__fp_max = 0
        path_capitalized_chars_count__fp_max = 0
        path_lowercase_chars_count__fp_max = 0
        for req in variant_fp_requests:
            path, _ = get_path_and_query_params(req)
            if path and len(path) > 0:
                path_split = path.split("/")
                path_sections = [
                    x for x in path_split if len(x) > PATH_LENGTH_THRESHOLD
                ]
                for index, section in enumerate(path_sections):

                    #remove periods form last path section to prevent counting things like init.js, etc
                    if index == len(path_sections):
                        section = section.replace(".", "")

                    numbers_count = sum(c.isdigit() for c in section)
                    path_numbers_count__fp += numbers_count
                    if numbers_count > path_numbers_count__fp_max:
                        path_numbers_count__fp_max = numbers_count

                    # count special chars
                    special_chars = re.findall('[^A-Za-z0-9]', section)
                    special_chars_count = len(special_chars)
                    path_special_chars_count__fp += special_chars_count
                    if special_chars_count > path_special_chars_count__fp_max:
                        path_special_chars_count__fp_max = special_chars_count

                    special_chars_upper = re.findall('[A-Z]', section)
                    special_chars_upper_count = len(special_chars_upper)
                    path_capitalized_chars_count__fp += special_chars_upper_count

                    if special_chars_upper_count > path_capitalized_chars_count__fp_max:
                        path_capitalized_chars_count__fp_max = special_chars_upper_count

                    special_chars_lower = re.findall('[a-z]', section)
                    special_chars_lower_count = len(special_chars_lower)
                    path_lowercase_chars_count__fp += special_chars_lower_count

                    if special_chars_lower_count > path_lowercase_chars_count__fp_max:
                        path_lowercase_chars_count__fp_max = special_chars_lower_count

        # we retrieve the average
        if len(variant_fp_requests) > 0:
            var_len_reqs = len(variant_fp_requests)
            path_numbers_count__fp = int(path_numbers_count__fp / var_len_reqs)
            path_special_chars_count__fp = int(path_special_chars_count__fp /
                                               var_len_reqs)
            path_capitalized_chars_count__fp = int(
                path_capitalized_chars_count__fp / var_len_reqs)
            path_lowercase_chars_count__fp = int(
                path_lowercase_chars_count__fp / var_len_reqs)

        features_dict[
            "variant_first_party_number_in_path_avg"] = path_numbers_count__fp
        features_dict[
            "variant_first_party_special_chars_in_path_avg"] = path_special_chars_count__fp
        features_dict[
            "variant_first_party_upper_chars_in_path_avg"] = path_capitalized_chars_count__fp
        features_dict[
            "variant_first_party_lower_chars_in_path_avg"] = path_lowercase_chars_count__fp

        features_dict[
            "variant_first_party_number_in_path_max"] = path_numbers_count__fp_max
        features_dict[
            "variant_first_party_special_chars_in_path_max"] = path_special_chars_count__fp_max
        features_dict[
            "variant_first_party_upper_chars_in_path_max"] = path_capitalized_chars_count__fp_max
        features_dict[
            "variant_first_party_lower_chars_in_path_max"] = path_lowercase_chars_count__fp_max

        path_numbers_count__tp = 0
        path_special_chars_count__tp = 0
        path_capitalized_chars_count__tp = 0
        path_lowercase_chars_count__tp = 0

        # counts encountered from one request
        path_numbers_count__tp_max = 0
        path_special_chars_count__tp_max = 0
        path_capitalized_chars_count__tp_max = 0
        path_lowercase_chars_count__tp_max = 0
        for req in variant_tp_requests:
            path, _ = get_path_and_query_params(req)
            if path and len(path) > 0:
                path_split = path.split("/")
                path_sections = [
                    x for x in path_split if len(x) > PATH_LENGTH_THRESHOLD
                ]
                for index, section in enumerate(path_sections):
                    #remove periods form last path section to prevent counting things like init.js, etc
                    if index == len(path_sections):
                        section = section.replace(".", "")
                    numbers_count = sum(c.isdigit() for c in section)
                    path_numbers_count__tp += numbers_count

                    if numbers_count > path_numbers_count__tp_max:
                        path_numbers_count__tp_max = numbers_count

                    special_chars = re.findall('[^A-Za-z0-9]', section)
                    special_chars_count = len(special_chars)
                    path_special_chars_count__tp += special_chars_count

                    if special_chars_count > path_special_chars_count__tp_max:
                        path_special_chars_count__tp_max = special_chars_count

                    special_chars_upper = re.findall('[A-Z]', section)
                    special_chars_upper_count = len(special_chars_upper)
                    path_capitalized_chars_count__tp += special_chars_upper_count

                    if special_chars_upper_count > path_capitalized_chars_count__tp_max:
                        path_capitalized_chars_count__tp_max = special_chars_upper_count

                    special_chars_lower = re.findall('[a-z]', section)
                    special_chars_lower_count = len(special_chars_lower)
                    path_lowercase_chars_count__tp += special_chars_lower_count

                    if special_chars_lower_count > path_lowercase_chars_count__tp_max:
                        path_lowercase_chars_count__tp_max = special_chars_lower_count

        # we retrieve the average
        if len(variant_tp_requests) > 0:
            var_len_reqs = len(variant_tp_requests)
            path_numbers_count__tp = int(path_numbers_count__tp / var_len_reqs)
            path_special_chars_count__tp = int(path_special_chars_count__tp /
                                               var_len_reqs)
            path_capitalized_chars_count__tp = int(
                path_capitalized_chars_count__tp / var_len_reqs)
            path_lowercase_chars_count__tp = int(
                path_lowercase_chars_count__tp / var_len_reqs)

        features_dict[
            "variant_third_party_number_in_path_avg"] = path_numbers_count__tp
        features_dict[
            "variant_third_party_special_chars_in_path_avg"] = path_special_chars_count__tp
        features_dict[
            "variant_third_party_upper_chars_in_path_avg"] = path_capitalized_chars_count__tp
        features_dict[
            "variant_third_party_lower_chars_in_path_avg"] = path_lowercase_chars_count__tp

        features_dict[
            "variant_third_party_number_in_path_max"] = path_numbers_count__tp_max
        features_dict[
            "variant_third_party_special_chars_in_path_max"] = path_special_chars_count__tp_max
        features_dict[
            "variant_third_party_upper_chars_in_path_max"] = path_capitalized_chars_count__tp_max
        features_dict[
            "variant_third_party_lower_chars_in_path_max"] = path_lowercase_chars_count__tp_max

        # look at mismatch resources
        # this means that when compared to control, it loaded more/fewer resources within a given directory. (see diff analysis)
        variant_fp_mm_count = 0
        variant_tp_mm_count = 0
        if len(self.mismatch_resources) > 0:

            _, variant_fp_mismatch_requests, variant_tp_mismatch_requests = find_all_first_and_third_party_webrequests(
                self.crawl_url, self.mismatch_resources)

            self.variant_first_party_mismatch_resources = variant_fp_mismatch_requests
            self.variant_third_party_mismatch_resources = variant_tp_mismatch_requests
            variant_fp_mm_count = len(variant_fp_mismatch_requests)
            variant_tp_mm_count = len(variant_tp_mismatch_requests)

        # Combine into one (path)
        features_dict[
            "variant_mismatch_resources_count"] = variant_fp_mm_count + variant_tp_mm_count

        logger.debug("******Content Types******")

        # Content type counts only
        content_type_results = get_content_type_mapping(
            variant_only_requests_set, self.content_type_resources)
        content_type_prefix = "variant_content_type"
        content_type_total = 0
        for key in content_type_results:
            tmp_len = len(content_type_results.get(key))
            features_dict[content_type_prefix + "__" + key] = tmp_len
            content_type_total += tmp_len
        features_dict[content_type_prefix] = content_type_total

        # Content type mismatch (this compares the extension of the path (example.com/some.json) to the actual content-type in headers
        content_type_results_mm = get_content_type_mismatch(
            original_variant_only_requests_set, self.content_type_resources,
            self.media_extensions)

        content_type_prefix_mm = "variant_content_type_mismatch"
        for key in content_type_results_mm:
            features_dict[content_type_prefix_mm + "__" +
                          key] = content_type_results_mm.get(key)
        # add up total without unknown
        content_type_mm_without_unknown = [
            content_type_results_mm.get(x)
            for x in content_type_results_mm.keys() if x != "unknown"
        ]
        features_dict[content_type_prefix_mm] = sum(
            content_type_mm_without_unknown)

        logger.debug("******Resource Types******")

        # init features to zero
        resource_types_dict = dict()
        for resource_type in self.resource_types:
            resource_types_dict[resource_type] = 0

        for webreq_url in self.resource_type_resources:
            url = webreq_url
            _, _, misc_types = self.resource_type_resources.get(webreq_url)
            if misc_types:
                resource_type = misc_types.get("resource_type")
                if resource_type and url in variant_only_requests_set and resource_type.lower(
                ) in self.resource_types:
                    resource_types_dict[resource_type.lower()] += 1

        resource_type_prefix = "variant_resource_type"
        features_dict[resource_type_prefix] = sum(resource_types_dict.values())
        for key in resource_types_dict:
            features_dict[resource_type_prefix + "__" +
                          key] = resource_types_dict.get(key)

        # subdomain count
        control_tlds = list(control_first_party_tlds_set) + list(
            control_third_party_tlds_set)
        variant_tlds = list(variant_first_party_tlds_set) + list(
            variant_third_party_tlds_set)
        features_dict[
            "control_subdomain_length_more5"] = 1 if has_subdomain_larger_than_n(
                control_tlds, n=5) else 0
        features_dict[
            "variant_subdomain_length_more5"] = 1 if has_subdomain_larger_than_n(
                variant_tlds, n=5) else 0

        #subdomain stats
        control_subdomain_stats = get_subdomain_stats(control_tlds)
        variant_subdomain_stats = get_subdomain_stats(variant_tlds)

        for key in control_subdomain_stats.keys():
            features_dict["control_subdomain_" +
                          key] = control_subdomain_stats.get(key)
        for key in variant_subdomain_stats.keys():
            features_dict["variant_subdomain_" +
                          key] = variant_subdomain_stats.get(key)

        control_first_party_subdomain_entropy = get_subdomain_entropy_from_set(
            control_first_party_tlds_set)
        features_dict[
            "control_first_party_subdomain_entropy"] = control_first_party_subdomain_entropy

        control_third_party_subdomain_entropy = get_subdomain_entropy_from_set(
            control_third_party_tlds_set)
        features_dict[
            "control_third_party_subdomain_entropy"] = control_third_party_subdomain_entropy

        variant_first_party_subdomain_entropy = get_subdomain_entropy_from_set(
            variant_first_party_tlds_set)
        features_dict[
            "variant_first_party_subdomain_entropy"] = variant_first_party_subdomain_entropy

        variant_third_party_subdomain_entropy = get_subdomain_entropy_from_set(
            variant_third_party_tlds_set)
        features_dict[
            "variant_third_party_subdomain_entropy"] = variant_third_party_subdomain_entropy

        logger.debug("******CONTENT TYPE SUBDOMAINS******")

        # split subdomain into content types
        content_type_fp_mapping = get_content_type_mapping(
            variant_fp_requests, self.content_type_resources)
        CONTENT_TYPE_FP_SUBDOMAIN_PREFIX = "var_ct_fp_subdomain_entropy__"
        CONTENT_TYPE_FP_PATH_PREFIX = "var_ct_fp_path_entropy__"
        for key in content_type_fp_mapping:
            content_type_tlds = []
            for url in content_type_fp_mapping.get(key):
                temp_tld = extract_tld(url)
                content_type_tlds.append(temp_tld)
            features_dict[CONTENT_TYPE_FP_SUBDOMAIN_PREFIX +
                          key] = get_subdomain_entropy_from_set(
                              content_type_tlds)
            _, _, var_ct_path_entropy_all, _, _ = get_path_and_query_stats(
                content_type_fp_mapping.get(key))
            features_dict[CONTENT_TYPE_FP_PATH_PREFIX +
                          key] = var_ct_path_entropy_all

        content_type_tp_mapping = get_content_type_mapping(
            variant_tp_requests, self.content_type_resources)
        CONTENT_TYPE_TP_SUBDOMAIN_PREFIX = "var_ct_tp_subdomain_entropy__"
        CONTENT_TYPE_TP_PATH_PREFIX = "var_ct_tp_path_entropy__"
        for key in content_type_tp_mapping:
            content_type_tlds = []
            for url in content_type_tp_mapping.get(key):
                temp_tld = extract_tld(url)
                content_type_tlds.append(temp_tld)
            features_dict[CONTENT_TYPE_TP_SUBDOMAIN_PREFIX +
                          key] = get_subdomain_entropy_from_set(
                              content_type_tlds)
            _, _, var_ct_path_entropy_all, _, _ = get_path_and_query_stats(
                content_type_tp_mapping.get(key))
            features_dict[CONTENT_TYPE_TP_PATH_PREFIX +
                          key] = var_ct_path_entropy_all

        # CONTROL FIRST PARTY

        _, _, ctr_first_party_path_entropy_all, ctr_first_party_query_entropy_all, _ = get_path_and_query_stats(
            control_fp_requests)
        features_dict["ctr_1_path_entropy"] = ctr_first_party_path_entropy_all
        features_dict[
            "ctr_1_query_entropy"] = ctr_first_party_query_entropy_all

        control_fp_cache_mapping = build_cache_control_mapping(
            control_fp_requests, self.ctr_resource_type_resources)
        # the key for this dict are cache control keys we care about
        control_fp_path_query_stats_dict = get_path_and_query_stats_with_cache_control(
            control_fp_cache_mapping)
        for cache_key in control_fp_path_query_stats_dict:
            # query and path stats based on entire urls
            control_first_party_path_len_stats, control_first_party_query_len_stats, control_first_party_path_entropy, control_first_party_query_entropy, control_first_party_query_value_entropy = control_fp_path_query_stats_dict.get(
                cache_key)
            for key in control_first_party_path_len_stats.keys():
                features_dict[
                    "ctr_1_pth_len_" + key +
                    cache_key] = control_first_party_path_len_stats.get(key)
            for key in control_first_party_query_len_stats.keys():
                features_dict[
                    "ctr_1_query_len_" + key +
                    cache_key] = control_first_party_query_len_stats.get(key)

            features_dict["ctr_1_path_entropy_" +
                          cache_key] = control_first_party_path_entropy
            features_dict["ctr_1_query_entropy_" +
                          cache_key] = control_first_party_query_entropy
            features_dict["ctr_1_query_val_entropy_" +
                          cache_key] = control_first_party_query_value_entropy

        # CONTROL THIRD PARTY
        control_tp_requests__filtered = control_tp_requests  #filter_requests_by_header(control_tp_requests, self.ctr_resource_type_resources, self.ctr_content_type_resources, log_prefix=self.log_prefix+"_control")
        #self.add_log("\nCTR TP Filter requests: " + " ,\n".join(control_tp_requests__filtered))

        _, _, control_third_party_path_entropy_all, control_third_party_query_entropy_all, control_third_party_query_val_entropy_all = get_path_and_query_stats(
            control_tp_requests__filtered)
        features_dict[
            "ctr_3rd_path_entropy"] = control_third_party_path_entropy_all
        features_dict[
            "ctr_3rd_query_entropy"] = control_third_party_query_entropy_all
        features_dict[
            "ctr_3rd_query_val_entropy"] = control_third_party_query_val_entropy_all

        control_tp_cache_mapping = build_cache_control_mapping(
            control_tp_requests, self.ctr_resource_type_resources)
        # the key for this dict are cache control keys we care about
        control_tp_path_query_stats_dict = get_path_and_query_stats_with_cache_control(
            control_tp_cache_mapping)
        for cache_key in control_tp_path_query_stats_dict:
            control_third_party_path_len_stats, control_third_party_query_len_stats, control_third_party_path_entropy, control_third_party_query_entropy, control_third_party_query_val_entropy = control_tp_path_query_stats_dict.get(
                cache_key)
            for key in control_third_party_path_len_stats.keys():
                features_dict[
                    "ctr_3rd_pth_len_" + key +
                    cache_key] = control_third_party_path_len_stats.get(key)
            for key in control_third_party_query_len_stats.keys():
                features_dict[
                    "ctr_3rd_query_len_" + key +
                    cache_key] = control_third_party_query_len_stats.get(key)

            features_dict["ctr_3rd_path_entropy" +
                          cache_key] = control_third_party_path_entropy
            features_dict["ctr_3rd_query_entropy" +
                          cache_key] = control_third_party_query_entropy
            features_dict["ctr_3rd_query_val_entropy" +
                          cache_key] = control_third_party_query_val_entropy

        logger.debug("******VARIANT - CACHE CONTROL PATH AND QUERY******")
        logger.debug(
            "******VARIANT - First Party CACHE CONTROL PATH AND QUERY******")

        # VARIANT FIRST PARTY
        variant_fp_requests__filtered = variant_fp_requests  #filter_requests_by_header(variant_fp_requests, self.resource_type_resources, self.content_type_resources, log_prefix=self.log_prefix+"_variant")
        #self.add_log("\nVar FP Filter Requests: " + " ,\n".join(variant_fp_requests__filtered))

        _, _, variant_first_party_path_entropy_all, variant_first_party_query_entropy_all, variant_first_party_query_val_entropy_all = get_path_and_query_stats(
            variant_fp_requests__filtered)
        features_dict[
            "var_1_path_entropy"] = variant_first_party_path_entropy_all
        features_dict[
            "var_1_query_entropy"] = variant_first_party_query_entropy_all
        features_dict[
            "var_1_query_val_entropy"] = variant_first_party_query_val_entropy_all

        variant_fp_cache_mapping = build_cache_control_mapping(
            variant_fp_requests__filtered, self.resource_type_resources)

        # the key for this dict are cache control keys we care about
        logger.debug("path_len_debug: Variant First Party")
        variant_fp_path_query_stats_dict = get_path_and_query_stats_with_cache_control(
            variant_fp_cache_mapping)

        for cache_key in variant_fp_path_query_stats_dict:
            variant_first_party_path_len_stats, variant_first_party_query_len_stats, variant_first_party_path_entropy, variant_first_party_query_entropy, variant_first_party_query_val_entropy = variant_fp_path_query_stats_dict.get(
                cache_key)
            for key in variant_first_party_path_len_stats.keys():
                features_dict[
                    "var_1_pth_len_" + key +
                    cache_key] = variant_first_party_path_len_stats.get(key)
            for key in variant_first_party_query_len_stats.keys():
                features_dict[
                    "var_1_query_len_" + key +
                    cache_key] = variant_first_party_query_len_stats.get(key)

            features_dict["var_1_path_entropy_" +
                          cache_key] = variant_first_party_path_entropy
            features_dict["var_1_query_entropy_" +
                          cache_key] = variant_first_party_query_entropy
            features_dict["var_1_query_val_entropy_" +
                          cache_key] = variant_first_party_query_val_entropy

        # VARIANT THIRD PARTY
        logger.debug(
            "******VARIANT - Third Party CACHE CONTROL PATH AND QUERY******")

        _, _, variant_third_party_path_entropy_all, variant_third_party_query_entropy_all, variant_third_party_query_val_entropy_all = get_path_and_query_stats(
            variant_tp_requests)
        features_dict[
            "var_3rd_path_entropy"] = variant_third_party_path_entropy_all
        features_dict[
            "var_3rd_query_entropy"] = variant_third_party_query_entropy_all
        features_dict[
            "var_3rd_query_val_entropy"] = variant_third_party_query_val_entropy_all

        variant_tp_cache_mapping = build_cache_control_mapping(
            variant_tp_requests, self.resource_type_resources)
        # the key for this dict are cache control keys we care about
        logger.debug("path_len_debug: Variant Third Party")
        variant_tp_path_query_stats_dict = get_path_and_query_stats_with_cache_control(
            variant_tp_cache_mapping)
        for cache_key in variant_tp_path_query_stats_dict:
            variant_third_party_path_len_stats, variant_third_party_query_len_stats, variant_third_party_path_entropy, variant_third_party_query_entropy, variant_third_party_query_val_entropy = variant_tp_path_query_stats_dict.get(
                cache_key)

            for key in variant_third_party_path_len_stats.keys():
                features_dict[
                    "var_3rd_pth_len_" + key +
                    cache_key] = variant_third_party_path_len_stats.get(key)
            for key in variant_third_party_query_len_stats.keys():
                features_dict[
                    "var_3rd_query_len_" + key +
                    cache_key] = variant_third_party_query_len_stats.get(key)

            features_dict["var_3rd_path_entropy" +
                          cache_key] = variant_third_party_path_entropy
            features_dict["var_3rd_query_entropy" +
                          cache_key] = variant_third_party_query_entropy
            features_dict["var_3rd_query_val_entropy" +
                          cache_key] = variant_third_party_query_val_entropy

        # path has special chars?
        control_first_party_has_spec_char = 1 if has_short_special_character_path(
            control_fp_requests) else 0
        control_third_party_has_spec_char = 1 if has_short_special_character_path(
            control_tp_requests) else 0
        variant_first_party_has_spec_char = 1 if has_short_special_character_path(
            variant_fp_requests) else 0
        variant_third_party_has_spec_char = 1 if has_short_special_character_path(
            variant_tp_requests) else 0

        features_dict["ctr_1_spec_char_path"] = str(
            control_first_party_has_spec_char)
        features_dict["ctr_3rd_spec_char_path"] = str(
            control_third_party_has_spec_char)
        features_dict["var_1_spec_char_path"] = str(
            variant_first_party_has_spec_char)
        features_dict["var_3rd_spec_char_path"] = str(
            variant_third_party_has_spec_char)

        # has subdomain as path
        control_first_party_has_dom_as_path = False
        for request in control_fp_requests:
            control_first_party_has_dom_as_path = control_first_party_has_dom_as_path or has_subdomain_as_path(
                request, min_length=3)

        features_dict[
            "ctr_1_dom_as_path"] = 1 if control_first_party_has_dom_as_path else 0

        control_third_party_has_dom_as_path = False
        for request in control_tp_requests:
            control_third_party_has_dom_as_path = control_third_party_has_dom_as_path or has_subdomain_as_path(
                request, min_length=3)

        features_dict[
            "ctr_3rd_dom_as_path"] = 1 if control_third_party_has_dom_as_path else 0

        variant_first_party_has_dom_as_path = False
        for request in variant_fp_requests:
            variant_first_party_has_dom_as_path = variant_first_party_has_dom_as_path or has_subdomain_as_path(
                request, min_length=3)

        features_dict[
            "var_1_dom_as_path"] = 1 if variant_first_party_has_dom_as_path else 0

        variant_third_party_has_dom_as_path = False
        for request in variant_tp_requests:
            variant_third_party_has_dom_as_path = variant_third_party_has_dom_as_path or has_subdomain_as_path(
                request, min_length=3)

        features_dict[
            "var_3rd_dom_as_path"] = 1 if variant_third_party_has_dom_as_path else 0

        ### Extract features based on Trials (this is not entirely based on control only or variant only sides)

        features_dict["control_trials_urls"] = self.ctr_diff_obj.get(
            "trials_urls_cumulative")[-1]
        features_dict["variant_trials_urls"] = self.var_diff_obj.get(
            "trials_urls_cumulative")[-1]
        features_dict["control_trials_domains"] = self.ctr_diff_obj.get(
            "trials_domains_cumulative")[-1]
        features_dict["variant_trials_domains"] = self.var_diff_obj.get(
            "trials_domains_cumulative")[-1]

        # growth rate
        control_trials_urls_growth_rate = avg_growth_rate(
            self.ctr_diff_obj.get("trials_urls_cumulative")[0],
            self.ctr_diff_obj.get("trials_urls_cumulative")[-1],
            len(self.ctr_diff_obj.get("trials_urls_cumulative")))
        variant_trials_urls_growth_rate = avg_growth_rate(
            self.var_diff_obj.get("trials_urls_cumulative")[0],
            self.var_diff_obj.get("trials_urls_cumulative")[-1],
            len(self.var_diff_obj.get("trials_urls_cumulative")))
        control_trials_domains_growth_rate = avg_growth_rate(
            self.ctr_diff_obj.get("trials_domains_cumulative")[0],
            self.ctr_diff_obj.get("trials_domains_cumulative")[-1],
            len(self.ctr_diff_obj.get("trials_domains_cumulative")))
        variant_trials_domains_growth_rate = avg_growth_rate(
            self.var_diff_obj.get("trials_domains_cumulative")[0],
            self.var_diff_obj.get("trials_domains_cumulative")[-1],
            len(self.var_diff_obj.get("trials_domains_cumulative")))

        features_dict["variant_ratio_urls_growth_rate"] = 0
        if control_trials_urls_growth_rate > 0:
            features_dict[
                "variant_ratio_urls_growth_rate"] = variant_trials_urls_growth_rate / control_trials_urls_growth_rate

        features_dict["variant_ratio_domains_growth_rate"] = 0
        if control_trials_domains_growth_rate > 0:
            features_dict[
                "variant_ratio_domains_growth_rate"] = variant_trials_domains_growth_rate / control_trials_domains_growth_rate

        y = self.ctr_diff_obj.get("trials_urls_cumulative")
        x = list(range(0,
                       len(self.ctr_diff_obj.get("trials_urls_cumulative"))))
        try:
            slope, _, _, _, _ = get_linear_regress(x, y)
        except:
            slope = 0
        features_dict["control_trials_urls_slope"] = slope

        y = self.var_diff_obj.get("trials_urls_cumulative")
        x = list(range(0,
                       len(self.var_diff_obj.get("trials_urls_cumulative"))))
        try:
            slope, _, _, _, _ = get_linear_regress(x, y)
        except:
            slope = 0
        features_dict["variant_trials_urls_slope"] = slope

        y = self.ctr_diff_obj.get("trials_domains_cumulative")
        x = list(
            range(0, len(self.ctr_diff_obj.get("trials_domains_cumulative"))))
        try:
            slope, _, _, _, _ = get_linear_regress(x, y)
        except:
            slope = 0
        features_dict["control_trials_domains_slope"] = slope

        y = self.var_diff_obj.get("trials_domains_cumulative")
        x = list(
            range(0, len(self.var_diff_obj.get("trials_domains_cumulative"))))
        try:
            slope, _, _, _, _ = get_linear_regress(x, y)
        except:
            slope = 0
        features_dict["variant_trials_domains_slope"] = slope

        return features_dict


def is_applicable_attribute(attribute, crawl_url_sld):
    if attribute.startswith("http"):
        return attribute
    elif attribute.startswith("//"):
        return "http:" + attribute
    elif attribute.startswith("/"):
        return "http://" + crawl_url_sld + attribute

    #<span class="prometeo-article-image-bg contentImagen"
    # style="background-image: url('https://dkumiip2e9ary.cloudfront.net/prometeo/9667-LAINFO3.jpeg');"></span>
    if "background" in attribute:
        css_dict = get_css_dict(attribute)
        for val in css_dict.values():
            if "http" in val:
                if val.startswith("url"):
                    val_clean = val.replace("'", "").replace("\"", "").replace(
                        "url(", "").replace(")", "").strip()
                    #logger.warning("%s - PAGESOURCE: val clean : %s" % (self.log_prefix, str(val_clean)))
                    if val_clean.startswith("http"):
                        #logger.warning("%s - PAGESOURCE: got url from background : %s" % (self.log_prefix, str(val_clean)))
                        return val_clean

    return None


def find_urls_from_element(soup_element, crawl_url_sld):
    urls = []
    for attr_key in soup_element.attrs.keys():
        # ignore attributes we know don't have urls
        if attr_key in [
                "srcset", "src-set", "alt", "title", "class", "target", "rel",
                "height", "width", "border", "data-srcset"
        ]:
            #skip src set
            continue
        attr_val = soup_element.attrs.get(attr_key)
        # attr_val can be a list
        if attr_val and len(attr_val) > 0:
            if isinstance(attr_val, list):
                for attr in attr_val:
                    url_found = is_applicable_attribute(attr, crawl_url_sld)
                    if url_found is not None and len(url_found.strip()) > 0:
                        urls.append(url_found)
            else:
                url_found = is_applicable_attribute(attr_val, crawl_url_sld)
                if url_found is not None and len(url_found.strip()) > 0:
                    urls.append(url_found)
    return urls


def get_width_height_through_style(soup_element, log_prefix=""):
    width = None
    height = None
    if soup_element.attrs.get("style"):
        style_str = soup_element.attrs.get("style")
        if style_str:
            css_dict = get_css_dict(style_str)
            for key_css, value_css in css_dict.items():
                if key_css == "width":
                    value_css = value_css.replace("px", "").replace(
                        "!important", "").strip()
                    if "%" not in value_css and "auto" not in value_css:
                        try:
                            width = int(value_css)
                            #logger.warning("%s - PAGESOURCE: got width from style %s : %s" % (log_prefix, "width", str(css_property)))
                        except:
                            pass
                if key_css == "height":
                    value_css = value_css.replace("px", "").replace(
                        "!important", "").strip()
                    if "%" not in value_css and "auto" not in value_css:
                        try:
                            height = int(value_css)
                            #logger.warning("%s - PAGESOURCE: got height from style %s : %s" % (log_prefix, "height", str(css_property)))
                        except:
                            pass

    return width, height


def get_element_width_or_height(soup_element, dim="width", log_prefix=""):
    if soup_element.attrs.get(dim) and len(soup_element.attrs.get(dim)) > 0:
        value = soup_element.attrs.get(dim).replace("px", "").strip()
        if value != "auto" and "%" not in value:
            try:
                value = int(value)
                if dim == ANTICV_OFFSETHEIGHT or dim == ANTICV_OFFSETWIDTH:
                    logger.debug(
                        "%s - PAGESOURCE: found dimension from %s: %s" %
                        (log_prefix, dim, str(value)))
                return value
            except:
                pass
    #logger.warning("%s - PAGESOURCE: could not parse %s : %s" % (log_prefix, dim, str(soup_element)))


def is_hidden_through_style(soup_element, log_prefix=""):
    if soup_element is not None and soup_element.attrs.get("style"):
        style = soup_element.attrs.get("style")
        match = re.search(
            r'display:(\s+)?none|visibility:(\s+)?hidden|opacity:(\s+)?0',
            style)
        if match:
            logger.warning(
                "%s - PAGESOURCE: hidden element due to style : %s" %
                (log_prefix, str(style)))
            return True

    return False


# find a parent that is not visible
def has_hidden_parent(soup_element, max_up=10, log_prefix=""):
    PARENTS_MIN_SIZE = 2  # if element is smaller than this threshold, then it is considered hidden

    traversal = max_up
    parent = soup_element.parent
    while (traversal > 0):
        if parent is not None:
            if hasattr(parent, "name") and parent.name.lower() in ["body"]:
                #logger.warning("%s - PAGESOURCE: NO hidden parent due to body" % (log_prefix))
                return None
            if hasattr(parent, "name") and parent.name.lower() in ["noscript"]:
                logger.debug(
                    "%s - PAGESOURCE: hidden parent due to noscript" %
                    (log_prefix))
                return parent

            if is_hidden_through_style(parent, log_prefix=log_prefix):
                style = parent.attrs.get("style")
                logger.debug(
                    "%s - PAGESOURCE: hidden parent due to style : %s" %
                    (log_prefix, str(style)))
                return parent

            width, height = get_width_height_through_style(parent)

            if height is None and parent.attrs.get("height"):
                height = get_element_width_or_height(parent, dim="height")
                if height is None and parent.attrs.get(ANTICV_OFFSETHEIGHT):
                    height = get_element_width_or_height(
                        parent, dim=ANTICV_OFFSETHEIGHT)

            if width is None and parent.attrs.get("width"):
                width = get_element_width_or_height(parent)
                if width is None and parent.attrs.get(ANTICV_OFFSETWIDTH):
                    width = get_element_width_or_height(parent,
                                                        dim=ANTICV_OFFSETWIDTH)

            if height is not None and height <= PARENTS_MIN_SIZE:
                logger.warning(
                    "%s - PAGESOURCE: hidden parent due to height == 0 : %s" %
                    (log_prefix, str(height)))
                return parent

            if width is not None and width <= PARENTS_MIN_SIZE:
                logger.warning(
                    "%s - PAGESOURCE: hidden parent due to width == 0 : %s" %
                    (log_prefix, str(width)))
                return parent

            parent = parent.parent
        traversal = traversal - 1


def is_smaller_than_ad_dimensions(element,
                                  crawl_url_sld,
                                  log_prefix="",
                                  img_dict=None,
                                  element_urls=None):
    # hide small dimension pics 160×90, 120×600 (american standards?)
    MIN_AD_WIDTH = 120
    MIN_AD_HEIGHT = 57  # needs to lower to capture asian ads

    width, height = get_width_height_through_style(element)
    # parse px and percentage
    if height is None and element.attrs.get("height"):
        height = get_element_width_or_height(element, dim="height")
        if height is None and element.attrs.get(ANTICV_OFFSETHEIGHT):
            height = get_element_width_or_height(element,
                                                 dim=ANTICV_OFFSETHEIGHT)

    if width is None and element.attrs.get("width"):
        width = get_element_width_or_height(element)
        if width is None and element.attrs.get(ANTICV_OFFSETWIDTH):
            width = get_element_width_or_height(element,
                                                dim=ANTICV_OFFSETWIDTH)

    # rely on actual img dimension
    ico_src = None
    if img_dict is not None and (width is None or height is None):
        if element_urls and len(element_urls) > 0:
            for element_src in element_urls:
                no_protocol_src = element_src.replace("https",
                                                      "").replace("http", "")

                # skip favicons
                if ".ico" in no_protocol_src:
                    ico_src = element_src

                if no_protocol_src in img_dict:
                    width, height = img_dict.get(no_protocol_src)
                    logger.debug(
                        "%s - %s - PAGESOURCE: From external file: Element width %s, height %s"
                        % (log_prefix, crawl_url_sld, str(width), str(height)))
                    break

    if ico_src is not None:
        logger.debug(
            "%s - %s - PAGESOURCE: ignoring element due to ico type : %s" %
            (log_prefix, crawl_url_sld, str(ico_src)))
        return True

    if width is not None and width < MIN_AD_WIDTH:
        logger.debug(
            "%s - %s - PAGESOURCE: ignoring element due to small width : %s" %
            (log_prefix, crawl_url_sld, str(width)))
        return True
    if height is not None and height < MIN_AD_HEIGHT:
        logger.debug(
            "%s - %s - PAGESOURCE: ignoring element due to small height : %s" %
            (log_prefix, crawl_url_sld, str(height)))
        return True

    return False


def escape_tag_and_attr_key_chars(some_string,
                                  escape_str="\\",
                                  special_chars=[":", "*", "!", "{", "}",
                                                 "1"]):
    new_string = some_string
    for special_char in special_chars:
        if special_char in new_string:
            split_name = new_string.split(special_char)
            # add in escapes to allow selection to work for colons in tag names
            escape_str_delimiter = escape_str + special_char
            new_string = escape_str_delimiter.join(split_name)

    return new_string


def has_digit(some_string):
    for character in some_string:
        if character.isdigit():
            return True
    return False


def get_element_selector(element,
                         ignore_attributes=["onclick", "onload", "onerror"],
                         reduce_random_attributes=False,
                         log_prefix=""):

    element_selector = element.name
    element_selector = escape_tag_and_attr_key_chars(element_selector)

    # make sure we add in our abp blocked element
    element_selector += ":not([abp-blocked-element])"

    used_attr = 0
    for attr_key, attr_val in element.attrs.items():

        ignore_match = False
        # completely ignore certain attributes
        for ignore_attr in ignore_attributes:
            if ignore_attr in attr_key:
                ignore_match = True
        if ignore_match:
            continue

        # attr_key , escape colons
        attr_key = escape_tag_and_attr_key_chars(attr_key)
        if has_digit(attr_key):
            continue

        if reduce_random_attributes:
            # if we reduce randomness, we ignore attributes that can change easily across page loads
            if attr_key in [
                    "srcset", "src-set", "class", "id", "style", "data"
            ]:
                # use only attr_key if we know the value can be dynamic
                element_selector += "[%s]" % attr_key
        elif attr_key.count(".") > 0 or attr_key.count("?") > 0:
            continue
        elif attr_val is not None and len(attr_val) > 0:
            if isinstance(attr_val, list):
                attr_val_json = json.dumps(" ".join(attr_val))
            else:
                attr_val_json = json.dumps(attr_val)

            # remove double quotes
            if attr_val_json.startswith("\""):
                attr_val_json = attr_val_json[1:]
            if attr_val_json.endswith("\""):
                attr_val_json = attr_val_json[:-1]

            attr_val_json = attr_val_json.replace("'", "\\'")
            element_selector += "[%s*='%s']" % (attr_key, attr_val_json)

            used_attr += 1
            # do up to 5 attrs
            if used_attr == 5:
                break

    return element_selector


def find_iframe_parent_structure(iframe_elements,
                                 levels=3,
                                 log_prefix="",
                                 reduce_random_attributes=False):

    iframe_parent_selectors = []
    iframe_parent_selectors_only = []

    for iframe_el in iframe_elements:
        current_parent = iframe_el.parent

        if current_parent is None:
            break

        parent_selector = get_element_selector(
            current_parent, reduce_random_attributes=reduce_random_attributes)
        immediate_parent_children_count = len(
            get_valid_soup_elements(current_parent.children))

        # ignore ones that are on the body
        if "body" in parent_selector:
            continue

        if current_parent.parent is None:
            # if there is only one parent, it needs to be an #, else ignore it
            if "id=" in parent_selector:
                iframe_parent_selectors.append(
                    (parent_selector, immediate_parent_children_count))
            return iframe_parent_selectors
        else:
            # get siblings
            grand_parent = current_parent.parent
            parent_index = 0  # index of where parent is in regards to its siblings
            children_count = 0
            for child in grand_parent.children:
                if str(child) == "\n":
                    continue
                children_count += 1
                if child is current_parent:
                    parent_index = children_count

            if children_count > 1:
                if parent_index > 1:
                    # we must offset the children count
                    children_count = children_count - parent_index + 1
                parent_selector += ":nth-last-child(%d):nth-child(%d)" % (
                    children_count, parent_index)

        current_parent = current_parent.parent

        traversal = levels
        parent_selector_aggregate = parent_selector
        while (traversal > 0):
            parent_selector = get_element_selector(
                current_parent,
                reduce_random_attributes=reduce_random_attributes)

            # make sure parent_selector goes in front
            parent_selector_aggregate = parent_selector + " > " + parent_selector_aggregate

            if current_parent.parent is None or "body" in parent_selector:
                break
            else:
                current_parent = current_parent.parent
                traversal -= 1

        if len(
                parent_selector_aggregate
        ) > 0 and parent_selector_aggregate not in iframe_parent_selectors_only:
            parent_selector_aggregate = ":not([abp-blocked-element]) " + parent_selector_aggregate
            logger.debug("%s - PAGESOURCE: new parent_selector_aggregate: %s" %
                         (log_prefix, parent_selector_aggregate))
            iframe_parent_selectors.append(
                (parent_selector_aggregate, immediate_parent_children_count))
            iframe_parent_selectors_only.append(parent_selector_aggregate)

    return iframe_parent_selectors


def get_valid_soup_elements(some_list):

    if some_list is not None:
        return [
            x for x in some_list if x != "\n" and x.name is not None
            and x.name.lower() != "noscript"
        ]

    return []


class PageSourceFeatureNewExtraction(BaseCVFeatureExtraction):

    parent_keys = [
        "sld_mm_with_parent_count", "parent_subdomain_entropy",
        "parent_path_entropy", "parent_query_entropy",
        "parent_query_val_entropy", "parent_sibling_count", "parent_depth",
        "ancestor_previous_children", "ancestor_next_children",
        "ancestor_previous_tag_count_div", "ancestor_previous_tag_count_span",
        "ancestor_previous_tag_count_other", "ancestor_next_tag_count_div",
        "ancestor_next_tag_count_span", "ancestor_next_tag_count_other",
        "ancestor_previous_tag_count_ul", "ancestor_next_tag_count_ul",
        "ancestor_previous_tag_count_li", "ancestor_next_tag_count_li",
        "ancestor_previous_tag_count_script", "ancestor_next_tag_count_script"
    ]

    ignore_keywords = [
        "social", "twitter", "weibo", "facebook", "youtube", "pinterest",
        "instagram", "dailymotion", "digg", "email", "mailto", "Apple",
        "App Store", "nav", "feed", "menu", "footer", "embed", "linkedin"
    ]

    def __init__(self,
                 crawl_group_name,
                 crawl_url,
                 diff_group,
                 crawl_collection,
                 diff_queue,
                 log_prefix="",
                 output_external_logs=True,
                 variant_blocked_urls_by_trial=None,
                 img_dimension_dict=None,
                 trials=4):
        BaseCVFeatureExtraction.__init__(self, crawl_url, None, None, trials=trials)
        self.diff_group = diff_group
        self.log_prefix = log_prefix
        self.crawl_collection = crawl_collection
        self.diff_queue = diff_queue
        self.crawl_group_name = crawl_group_name
        self.output_external_logs = output_external_logs
        self.variant_blocked_urls_by_trial = variant_blocked_urls_by_trial or dict(
        )
        self.img_dimension_dict = img_dimension_dict

    def get_file_path_and_name(self, trial_instance, crawl_type):
        def _go_up_path(file_path, level_up=3):
            # here the file_path has the filename as well
            file_path_split = file_path.split(os.sep)
            lvl_up = -1 * (level_up + 1)
            file_path_split = file_path_split[:lvl_up]
            # join up to make the dir we need
            file_path = os.sep.join(file_path_split)
            # then combine to find the pagesource dir
            file_path = file_path + os.sep + "pagesource_" + self.crawl_group_name
            return file_path

        file_name_result = ""
        file_path_result = ""

        file_path = trial_instance.get("file_path")
        file_name = trial_instance.get("file_name")

        middle_prefix = "__" + crawl_type
        if crawl_type == CONTROL:
            file_name_split = file_name.split(
                WEBREQUESTS_DATA_FILE_SUFFIX_CONTROL)
        else:
            file_name_split = file_name.split(
                WEBREQUESTS_DATA_FILE_SUFFIX_VARIANT)
        prefix_name_index = 0
        prefix_name = None
        if len(file_name_split) == 2:
            prefix_name = file_name_split[prefix_name_index]

        if prefix_name:
            file_name_result = prefix_name + middle_prefix + PAGE_SOURCE_SUFFIX

        file_path_result = _go_up_path(file_path)

        # double check if file exists
        if not os.path.isfile(file_path_result + os.sep + file_name_result):
            file_key, trial_number, _, control_or_variant, _ = get_trial_file_name_details(
                file_name, file_path_result)
            trial_file_names = glob.glob(file_path_result + os.sep + "*" +
                                         file_key + "*")
            trial_label = "_trial" + str(trial_number)
            for trial_file_name in trial_file_names:
                if crawl_type in trial_file_name and trial_file_name.endswith(
                        ".html") and trial_label in trial_file_name:
                    file_name_result = get_file_name(trial_file_name)
                    break

        assert os.path.isfile(
            file_path_result + os.sep + file_name_result
        ) is True, "Could not find corresponding Pagesource %s/%s" % (
            file_path_result, file_name_result)

        #assert len(file_path_result) > 0, "Could not find file path"
        return file_path_result, file_name_result

    def find_parent_with_target_or_rel(self,
                                       soup_element,
                                       max_up=10):
        traversal = max_up
        parent = soup_element.parent
        while (traversal > 0):
            if parent is not None:
                if hasattr(parent, "name") and parent.name.lower() in ["body"]:
                    return None
                if hasattr(parent,
                           "name") and parent.name.lower() in ["noscript"]:
                    return None

                if parent.attrs.get("target"):
                    return parent
                if parent.attrs.get("rel") is not None and len(
                        parent.attrs.get("rel")) > 0:
                    return parent
                parent = parent.parent
            traversal = traversal - 1

    def parents_have_ignore_keywords(self,
                                     soup_element,
                                     crawl_url_sld,
                                     max_up=10):
        #logger.warning("%s - PAGESOURCE: checking parents on keywords %s " % (self.log_prefix, str(soup_element)))
        traversal = max_up
        parent = soup_element.parent
        has_ignore = False
        force_consider = False
        while (traversal > 0):
            if parent is not None:
                if hasattr(parent, "name") and parent.name.lower() in ["body"]:
                    break
                if hasattr(parent,
                           "name") and parent.name.lower() in ["noscript"]:
                    has_ignore = True
                    break
                #logger.warning("%s - PAGESOURCE: parents element: %s " % (self.log_prefix, str(parent)))
                has_ignore, force_consider = self.element_has_ignore_keywords(
                    parent, crawl_url_sld)
                #logger.warning("%s - PAGESOURCE: parents on keywords: has keywords: %s, force consider: %s " % (self.log_prefix, str(has_ignore), str(force_consider)))
                if force_consider:
                    break
                if has_ignore:
                    break

                parent = parent.parent
            traversal = traversal - 1

        return has_ignore, force_consider

    def element_has_ignore_keywords(self, element, crawl_url_sld):

        #key_attrs = ["class", "href", "src", "title", "alt"]
        force_consider = False
        has_ignore = False

        for klasses in element.attrs.values():
            if isinstance(klasses, list):
                klasses = [x.lower() for x in klasses]
                # make a long string to compare
                klasses = ",".join(klasses)  
            if isinstance(klasses, str):
                klasses = klasses.lower()

            if len(klasses) == 0:
                continue

            # ignore social buttons
            for ignore_keyword in self.ignore_keywords:

                if ignore_keyword in klasses:
                    has_ignore = True
                    logger.debug(
                        "%s - PAGESOURCE: setting has_ignore to True  %s, %s" %
                        (self.log_prefix, ignore_keyword, str(klasses)))
                    break

            if "google" in klasses and "play" in klasses:
                has_ignore = True

            if has_ignore or force_consider:
                break

        return has_ignore, force_consider

    def find_tlds_and_slds(self, some_list):
        url_tlds = []
        url_slds = []
        for url in some_list:
            url_tld = extract_tld(url)
            url_sld = get_second_level_domain_from_tld(url_tld)
            url_tlds.append(url_tld)
            url_slds.append(url_sld)

        return url_tlds, url_slds

    def get_img_features(self,
                         imgs,
                         crawl_url_sld,
                         look_for_parents=True,
                         skip_if_siblings=False,
                         skip_if_parent_siblings=False,
                         trial_key=0,
                         soup_control=None,
                         control_imgs=None,
                         crawl_url_tld=None):

        # extract control related src and hrefs
        control_imgs_src = []
        if control_imgs:
            for control_element in control_imgs:
                if control_element.attrs.get("src") is not None and len(
                        control_element.attrs.get("src")) > 0:
                    applicable_url = is_applicable_attribute(
                        control_element.attrs.get("src"), crawl_url_sld)
                    if applicable_url:
                        img_url_no_query = get_url_without_query(
                            applicable_url)
                        control_imgs_src.append(img_url_no_query)
        logger.debug("%s - %s - PAGESOURCE: Control img srcs: %d" %
                     (self.log_prefix, crawl_url_sld, len(control_imgs_src)))
        logger.debug("%s - %s - PAGESOURCE: Control img srcs trace: %s" %
                     (self.log_prefix, crawl_url_sld, str(control_imgs_src)))

        def _should_skip_element(element,
                                 skip_if_siblings,
                                 element_slds=None,
                                 element_tlds=None,
                                 element_urls=None,
                                 img_dict=None):

            # for now try ignoring all first parties
            # must only for a tags
            is_first_party = False
            has_ignore = False
            force_consider = False

            if element_urls is not None and len(element_urls) == 0:
                logger.debug(
                    "%s - %s - PAGESOURCE:  skip element due to empty urls: %s"
                    % (self.log_prefix, crawl_url_sld, str(element_urls)))

                return True, is_first_party, force_consider

            if element.attrs.get("rel") or element.attrs.get("target"):
                if element_slds:
                    for element_sld in element_slds:
                        if element_sld == crawl_url_sld:
                            is_first_party = True
                            logger.debug(
                                "%s - %s - PAGESOURCE:  first party (1): %s" %
                                (self.log_prefix, crawl_url_sld,
                                 str(element_slds)))
                            break
                if not is_first_party and element_tlds and crawl_url_tld:
                    for element_tld in element_tlds:
                        if element_tld.domain in crawl_url_tld.domain:
                            is_first_party = True
                            logger.debug(
                                "%s - %s - PAGESOURCE: first party (2) : %s" %
                                (self.log_prefix, crawl_url_sld,
                                 str(element_slds)))
                            break

            if element.name.lower() == "iframe":
                if element_slds:
                    match_all = True
                    for element_sld in element_slds:
                        if element_sld != crawl_url_sld:
                            match_all = False
                    if match_all:
                        logger.debug(
                            "%s - %s - PAGESOURCE: iframe: ignoring element due to first party : %s"
                            % (self.log_prefix, crawl_url_sld,
                               str(element_slds)))
                        return True, is_first_party, force_consider

            if self.variant_blocked_urls_by_trial:
                if trial_key in self.variant_blocked_urls_by_trial:
                    blocked_urls = self.variant_blocked_urls_by_trial.get(
                        trial_key)

                    if element_urls:
                        for url in element_urls:
                            if url in blocked_urls:
                                logger.debug(
                                    "%s - %s - PAGESOURCE: ignoring element due to blocked url : %s"
                                    %
                                    (self.log_prefix, crawl_url_sld, str(url)))
                                return True, is_first_party, force_consider

            # if url is just an image, then ignore it (for parent elements only)
            if (element.attrs.get("rel")
                    or element.attrs.get("target")) and element_urls:
                for url in element_urls:

                    # if url has two http, then it will most likely be a redirect
                    http_occurs = re.findall("http", url)
                    if len(http_occurs) > 1:
                        return False, is_first_party, force_consider

                    # we care only about paths
                    path, _ = get_path_and_query_params(url)

                    # ignore images
                    for media in WebRequestsFeatureExtraction.media_extensions:
                        if path.endswith(media):
                            logger.debug(
                                "%s - %s - PAGESOURCE: ignoring element due to ending with media : %s"
                                % (self.log_prefix, crawl_url_sld, str(url)))

                            return True, is_first_party, force_consider

            # if urls both have http and data:image, something is wrong and we ignore it
            if element_urls and len(element_urls) > 0:
                if element.attrs.get("srcset") and element.attrs.get(
                        "srcset").startswith("data:"):
                    logger.debug(
                        "%s - %s - PAGESOURCE: ignoring element due to http and srcset data : %s"
                        % (self.log_prefix, crawl_url_sld,
                           str(element.attrs.get("srcset"))))
                    return True, is_first_party, force_consider

            smaller_than_ad = is_smaller_than_ad_dimensions(
                element,
                crawl_url_sld,
                log_prefix=self.log_prefix,
                img_dict=img_dict,
                element_urls=element_urls)

            if smaller_than_ad:
                return smaller_than_ad, is_first_party, force_consider

            # look at keywords  (must go before the firstparty/thirdpary check and AFTER the dimensions check)
            has_ignore, force_consider = self.element_has_ignore_keywords(
                element, crawl_url_sld)

            if has_ignore or force_consider:
                return has_ignore, is_first_party, force_consider

            # ads generally don't have siblings
            if skip_if_siblings:

                has_sibs = _has_siblings(element)
                if has_sibs:
                    logger.debug(
                        "%s - %s - PAGESOURCE: ignoring element due to siblings: %s"
                        % (self.log_prefix, crawl_url_sld, str(element)))
                return has_sibs, is_first_party, force_consider

            return False, is_first_party, force_consider

        def _has_siblings(element):
            siblings_exist = False

            next_sibs = get_valid_soup_elements(element.next_siblings)
            if len(next_sibs) > 0:
                siblings_exist = True

            if not siblings_exist:
                prev_sibs = get_valid_soup_elements(element.previous_siblings)
                if len(prev_sibs) > 0:
                    siblings_exist = True

            return siblings_exist

        def _get_siblings_count(element):
            # find siblings count, must ignore new lines as siblings (which is possible due to beautifulsoup)
            siblings_count = 0
            next_sibs = get_valid_soup_elements(element.next_siblings)
            prev_sibs = get_valid_soup_elements(element.previous_siblings)
            siblings_count += len(next_sibs)
            siblings_count += len(prev_sibs)
            return siblings_count

        def _get_sibling_features(element):
            sib_features = dict()
            sib_features["previous_tag"] = "None"
            sib_features["next_tag"] = "None"
            sib_features["previous_children"] = 0
            sib_features["next_children"] = 0

            if element.previous_siblings is not None:
                prev_sibs = get_valid_soup_elements(element.previous_siblings)
                for prev_sib in prev_sibs:
                    if hasattr(prev_sib, "contents"):
                        sib_features["previous_tag"] = prev_sib.name
                        sib_features["previous_children"] = len(
                            prev_sib.contents)

            if element.next_siblings is not None:
                next_sibs = get_valid_soup_elements(element.next_siblings)
                for next_sib in next_sibs:
                    if hasattr(next_sib, "contents"):
                        sib_features["next_tag"] = next_sib.name
                        sib_features["next_children"] = len(next_sib.contents)

            return sib_features

        def _find_parent_attribute(element):
            # find siblings count
            siblings_count = _get_siblings_count(element)

            # find depth to body, count only depth that has siblings
            depth = 0
            for index, parent in enumerate(element.parents):
                if parent.name == "body":
                    break
                if _has_siblings(parent):
                    depth += 1

            return siblings_count, depth

        def _get_ancestor_features(element):
            if element.parent:
                feat = _get_sibling_features(element.parent)
                return feat

        def _does_control_have_element(element_urls):
            if control_imgs is None:
                return False

            if element_urls:
                for el_url in element_urls:
                    img_url_no_query = get_url_without_query(el_url)
                    logger.debug(
                        "%s - %s - PAGESOURCE: check img url with control: %s"
                        % (self.log_prefix, crawl_url_sld, img_url_no_query))

                    if img_url_no_query in control_imgs_src:
                        logger.debug(
                            "%s - %s - PAGESOURCE: found element in control src: %s"
                            % (self.log_prefix, crawl_url_sld,
                               str(element_urls)))
                        return True

            return False

        visible_imgs = []
        for img in imgs:
            hidden_parent = has_hidden_parent(img, log_prefix=self.log_prefix)
            img_hidden = is_hidden_through_style(img,
                                                 log_prefix=self.log_prefix)
            if not hidden_parent and not img_hidden:
                visible_imgs.append(img)
            else:
                pass

        logger.debug("%s - %s - PAGESOURCE: Found VISIBLE img : %d" %
                     (self.log_prefix, crawl_url_sld, len(visible_imgs)))

        img_features = dict()
        img_features["subdomain_entropy"] = []
        img_features["path_entropy"] = []
        img_features["query_entropy"] = []
        img_features["query_val_entropy"] = []

        if look_for_parents:
            for key in self.parent_keys:
                img_features[key] = []

        consider_imgs = 0

        for img_el in visible_imgs:

            img_src = find_urls_from_element(img_el, crawl_url_sld)
            #: Found url src : %s" % (self.log_prefix, str(img_src)))

            # get domain, path, query
            url_tlds, url_slds = self.find_tlds_and_slds(img_src)

            skip, is_first_party, force_consider_img = _should_skip_element(
                img_el,
                skip_if_siblings,
                element_slds=url_slds,
                element_tlds=url_tlds,
                element_urls=img_src,
                img_dict=self.img_dimension_dict)
            if skip and not force_consider_img:
                logger.debug(
                    "%s - %s - PAGESOURCE: skipping image (first stage): %s" %
                    (self.log_prefix, crawl_url_sld, str(img_el)))

                continue

            logger.debug(
                "%s - %s - PAGESOURCE: force_consider_img %s" %
                (self.log_prefix, crawl_url_sld, str(force_consider_img)))

            imgs_subdomain_entropy = get_subdomain_entropy_from_set(url_tlds)
            _, _, path_entropy, query_entropy, query_val_entropy = get_path_and_query_stats(
                img_src)

            # compare with parent:
            if look_for_parents:
                parent = self.find_parent_with_target_or_rel(img_el)

                if parent:
                    parent_urls = find_urls_from_element(parent, crawl_url_sld)
                    parent_url_tlds, parent_url_slds = self.find_tlds_and_slds(
                        parent_urls)

                    #if not force_consider_img:
                    skip, is_first_party, force_consider_parent = _should_skip_element(
                        parent,
                        skip_if_parent_siblings,
                        element_slds=parent_url_slds,
                        element_tlds=url_tlds,
                        element_urls=parent_urls)
                    if skip and not force_consider_parent:
                        logger.debug(
                            "%s - %s - PAGESOURCE: skipping due to parent element"
                            % (self.log_prefix, crawl_url_sld))
                        continue
                    if is_first_party:
                        # check for control here
                        if soup_control is not None and _does_control_have_element(
                                img_src):
                            logger.debug(
                                "%s - %s - PAGESOURCE: skipping due soup control (parent part)"
                                % (self.log_prefix, crawl_url_sld))
                            continue

                    logger.debug(
                        "%s - %s - PAGESOURCE: force_consider_parent %s" %
                        (self.log_prefix, crawl_url_sld,
                         str(force_consider_parent)))

                    if not force_consider_parent:
                        # make sure the parent is not part of something we don't care about
                        skip, force_consider_ancestors = self.parents_have_ignore_keywords(
                            parent, crawl_url_sld)
                        if skip and not force_consider_ancestors:
                            logger.debug(
                                "%s - %s - PAGESOURCE: skipping ancestor due to keywords"
                                % (self.log_prefix, crawl_url_sld))
                            continue
                        if is_first_party:
                            # check for control here
                            if soup_control is not None and _does_control_have_element(
                                    img_src):
                                logger.debug(
                                    "%s - %s - PAGESOURCE: skipping ancestor soup control (ancestors)"
                                    % (self.log_prefix, crawl_url_sld))
                                continue
                        logger.debug(
                            "%s - %s - PAGESOURCE: force_consider_ancestors %s"
                            % (self.log_prefix, crawl_url_sld,
                               str(force_consider_ancestors)))

                    if self.output_external_logs:
                        self.diff_queue.put(
                            "%s - %s parent element used %s " % (str(
                                self.log_prefix), self.crawl_url, str(parent)))

                    parent_subdomain_entropy = get_subdomain_entropy_from_set(
                        parent_url_tlds)
                    _, _, parent_path_entropy, parent_query_entropy, parent_query_val_entropy = get_path_and_query_stats(
                        parent_urls)

                    logger.debug(
                        "%s - %s parent element used %s " %
                        (str(self.log_prefix), self.crawl_url, str(parent)))

                    if parent_subdomain_entropy is not None and parent_subdomain_entropy > 0:
                        img_features["parent_subdomain_entropy"].append(
                            parent_subdomain_entropy)

                    # for paths and queries, we don't count zeroes because not all urls will have paths or queries
                    if parent_path_entropy is not None:  # and parent_path_entropy>0:
                        img_features["parent_path_entropy"].append(
                            parent_path_entropy)
                    if parent_query_entropy is not None:  # and parent_query_entropy > 0:
                        img_features["parent_query_entropy"].append(
                            parent_query_entropy)
                    if parent_query_val_entropy is not None:  # and parent_query_val_entropy > 0:
                        img_features["parent_query_val_entropy"].append(
                            parent_query_val_entropy)

                    siblings_count, depth = _find_parent_attribute(parent)
                    img_features["parent_sibling_count"].append(siblings_count)
                    img_features["parent_depth"].append(depth)

                    # could one mismatch per img
                    for img_url_sld in url_slds:
                        if img_url_sld not in parent_url_slds:
                            img_features["sld_mm_with_parent_count"].append(1)
                            break

                    # ancestor
                    ancestor_features = _get_ancestor_features(parent)
                    prefix = "ancestor_"
                    for key in ancestor_features:
                        if "tag" not in key:
                            img_features[prefix + key].append(
                                ancestor_features.get(key))
                        else:
                            if ancestor_features.get(key) != "None":
                                acceptable_tags = [
                                    "div", "span", "ul", "li", "script"
                                ]
                                tag = ancestor_features.get(key).lower()
                                if tag in acceptable_tags:
                                    img_features[prefix + key + "_count_" +
                                                 tag].append(1)
                                else:
                                    img_features[prefix + key +
                                                 "_count_other"].append(1)
            else:
                # make sure the parent is not part of something we don't care about
                skip, force_consider_leaf = self.parents_have_ignore_keywords(
                    img_el, crawl_url_sld)
                if skip and not force_consider_leaf:
                    logger.debug(
                        "%s - %s - PAGESOURCE: skipping img due to keywords (leaf)"
                        % (self.log_prefix, crawl_url_sld))
                    continue

            consider_imgs += 1
            logger.debug("%s - %s element used %s" %
                         (str(self.log_prefix), self.crawl_url, str(img_el)))

            if self.output_external_logs:
                self.diff_queue.put(
                    "%s - %s element used %s " %
                    (str(self.log_prefix), self.crawl_url, str(img_el)))

            # for paths and queries, we don't count zeroes because not all urls will have paths or queries
            if imgs_subdomain_entropy is not None and imgs_subdomain_entropy > 0:
                img_features["subdomain_entropy"].append(
                    imgs_subdomain_entropy)
            if path_entropy is not None and path_entropy > 0:
                img_features["path_entropy"].append(path_entropy)
            if query_entropy is not None and query_entropy > 0:
                img_features["query_entropy"].append(query_entropy)
            if query_val_entropy is not None and query_val_entropy > 0:
                img_features["query_val_entropy"].append(query_val_entropy)

        logger.debug("%s - %s - PAGESOURCE: considered img: %s" %
                     (self.log_prefix, crawl_url_sld, str(consider_imgs)))

        logger.debug("%s - %s - PAGESOURCE: img_features inside: %s" %
                     (self.log_prefix, crawl_url_sld, str(img_features)))
        return img_features

    def extract_features(self):

        features_dict = {}
        # create custom structure
        crawl_trial_group = create_trial_group(self.diff_group,
                                               self.crawl_collection)

        #TARGET_TYPES = ["_blank"]
        #A_TAG_REL = ["noopener", "noreferrer", "nofollow", "sponsored"]

        crawl_url_tld = extract_tld(self.crawl_url)
        crawl_url_sld = get_second_level_domain_from_tld(crawl_url_tld)

        # init keys
        main_keys_default = [
            "subdomain_entropy",
            "path_entropy",
            "query_entropy",
            "query_val_entropy",
        ]

        one_off_keys = []

        main_keys_default += self.parent_keys

        main_keys = list(main_keys_default) + one_off_keys

        for diff_type in ["_no_rel", "_leaf", "_iframe"]:
            for key in main_keys_default:
                main_keys.append(key + diff_type)

        start_time = time.time()
        control_words = []
        variant_words = []
        # grab text as words only
        for crawl_type in [CONTROL]:
            for trial_key in crawl_trial_group[crawl_type].keys():
                trial_inst = crawl_trial_group[crawl_type].get(trial_key)
                file_path, file_name = self.get_file_path_and_name(
                    trial_inst, crawl_type)
                abs_file_path = file_path + os.sep + file_name
                if os.path.isfile(abs_file_path):
                    try:
                        f = open(abs_file_path, 'r')
                        soup = BeautifulSoup(f, 'html.parser')
                        control_words += soup.get_text().split()
                        # close soups and files
                        soup.decompose()
                        f.close()
                    except:
                        pass

        logger.debug(
            "%s - PAGESOURCE TIMER - GET CONTROL WORDS TIME --- %s seconds --- %s"
            % (self.log_prefix, time.time() - start_time, self.crawl_url))

        target_found_both = dict()
        target_found_both[VARIANT] = dict()

        target_found = target_found_both[VARIANT]

        for main_key in main_keys:
            target_found[main_key] = []

        for trial_key in crawl_trial_group[VARIANT].keys():
            trial_inst = crawl_trial_group[VARIANT].get(trial_key)
            trial_inst_control = crawl_trial_group[CONTROL].get(trial_key)
            file_path, file_name = self.get_file_path_and_name(
                trial_inst, VARIANT)
            abs_file_path = file_path + os.sep + file_name
            file_path_control, file_name_control = self.get_file_path_and_name(
                trial_inst_control, CONTROL)
            abs_file_path_control = file_path_control + os.sep + file_name_control
            if os.path.isfile(abs_file_path):
                soup_file = open(abs_file_path, 'r')
                soup = BeautifulSoup(soup_file, 'html.parser')

                # get rid of noscript
                for noscript_el in soup.select("noscript"):
                    noscript_el.extract()

                # open up control soup
                soup_control = None
                if os.path.isfile(abs_file_path_control):
                    logger.debug(
                        "%s - PAGESOURCE - loading control file : %s" %
                        (self.log_prefix, abs_file_path_control))

                    soup_file_control = open(abs_file_path_control, 'r')
                    soup_control = BeautifulSoup(soup_file_control,
                                                 'html.parser')

                    # get rid of noscript
                    for noscript_el in soup_control.select("noscript"):
                        noscript_el.extract()
                else:
                    logger.debug(
                        "%s - PAGESOURCE - could not load control file : %s" %
                        (self.log_prefix, abs_file_path_control))

                variant_words += soup.get_text().split()

                start_time = time.time()
                # use i for case insenstive
                # get all that is not blocked by adblocker
                imgs_reg = soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer) a:not([abp-blocked-element]):not([anticv-hidden])[href][target*="blank" i][rel*="no" i] img:not([abp-blocked-element]):not([anticv-hidden])[src]'
                )
                imgs_reg += soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer) a:not([abp-blocked-element]):not([anticv-hidden]):not([target])[href][rel*="no" i]  img:not([abp-blocked-element]):not([anticv-hidden])[src]'
                )
                imgs_reg += soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer) a:not([abp-blocked-element]):not([anticv-hidden])[href][target*="blank" i][rel*="no" i] :not([abp-blocked-element]):not([anticv-hidden])[style*="background-image"][src]'
                )
                if len(imgs_reg) > 0:
                    logger.debug(
                        "%s - %s - PAGESOURCE - before soup select (img_reg) : %d"
                        % (self.log_prefix, crawl_url_sld, len(imgs_reg)))
                # make it a set here due to duplicates
                imgs_reg = set(imgs_reg)  
                if len(imgs_reg) > 0:
                    logger.debug(
                        "%s - %s - PAGESOURCE - after soup select (img_reg) : %d"
                        % (self.log_prefix, crawl_url_sld, len(imgs_reg)))

                imgs_reg_control = soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer) a:not([abp-blocked-element]):not([anticv-hidden])[href][target*="blank" i][rel*="no" i] img:not([abp-blocked-element]):not([anticv-hidden])[src]'
                )
                imgs_reg_control += soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer) a:not([abp-blocked-element]):not([anticv-hidden]):not([target])[href][rel*="no" i] img:not([abp-blocked-element]):not([anticv-hidden])[src]'
                )
                imgs_reg_control += soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer) a:not([abp-blocked-element]):not([anticv-hidden])[href][target*="blank" i][rel*="no" i] :not([abp-blocked-element]):not([anticv-hidden])[style*="background-image"][src]'
                )
                # make it a set here due to duplicates
                imgs_reg_control = set(imgs_reg_control)  
                if len(imgs_reg_control) > 0:
                    logger.debug(
                        "%s - %s - PAGESOURCE - soup control found (img_reg): %d"
                        % (self.log_prefix, crawl_url_sld,
                           len(imgs_reg_control)))

                img_features_reg = self.get_img_features(
                    imgs_reg,
                    crawl_url_sld,
                    skip_if_siblings=True,
                    trial_key=trial_key,
                    soup_control=soup_control,
                    control_imgs=imgs_reg_control,
                    crawl_url_tld=crawl_url_tld)

                logger.debug(
                    "%s - PAGESOURCE TIMER - imgs_reg TIME --- %s seconds --- %s"
                    % (self.log_prefix, time.time() - start_time,
                       self.crawl_url))

                # this is the overall feature per trial=
                # average or sum the img features
                for key in img_features_reg:
                    val = 0

                    if len(img_features_reg.get(key)) > 0 and np.sum(
                            img_features_reg.get(key)) > 0:
                        if "count" not in key:
                            val = np.average(img_features_reg.get(key))
                        else:
                            val = np.sum(img_features_reg.get(key))

                    target_found[key].append(val)

                start_time = time.time()

                imgs_no_rel = soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer) a:not([abp-blocked-element]):not([anticv-hidden]):not([rel])[href][target] img:not([abp-blocked-element]):not([anticv-hidden])[src]'
                )
                imgs_no_rel_control = soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer) a:not([abp-blocked-element]):not([anticv-hidden]):not([rel])[href][target] img:not([abp-blocked-element]):not([anticv-hidden])[src]'
                )
                if len(imgs_no_rel) > 0:
                    logger.debug(
                        "%s - %s - PAGESOURCE - before soup select (imgs_no_rel) : %d"
                        % (self.log_prefix, crawl_url_sld, len(imgs_no_rel)))

                if len(imgs_no_rel_control) > 0:
                    logger.debug(
                        "%s - %s - PAGESOURCE - soup control found (imgs_no_rel): %d"
                        % (self.log_prefix, crawl_url_sld,
                           len(imgs_no_rel_control)))

                img_features_no_rel = self.get_img_features(
                    imgs_no_rel,
                    crawl_url_sld,
                    skip_if_siblings=True,
                    trial_key=trial_key,
                    soup_control=soup_control,
                    control_imgs=imgs_no_rel_control,
                    crawl_url_tld=crawl_url_tld)

                logger.debug(
                    "%s - PAGESOURCE TIMER - imgs_no_rel TIME --- %s seconds --- %s"
                    % (self.log_prefix, time.time() - start_time,
                       self.crawl_url))

                # this is the overall feature per trial
                suffix = "_no_rel"
                # average the img features
                for key in img_features_no_rel:
                    val = 0
                    if len(img_features_no_rel.get(key)) > 0 and np.sum(
                            img_features_no_rel.get(key)) > 0:
                        if "count" not in key:
                            val = np.average(img_features_no_rel.get(key))
                        else:
                            val = np.sum(img_features_no_rel.get(key))

                    target_found[key + suffix].append(val)

                start_time = time.time()

                # find leaf target a
                imgs_leaf = soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer):not(form) a:not([abp-blocked-element]):not([anticv-hidden]):not(:has(*))[href][target][rel]'
                )
                imgs_leaf += soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer):not(form) a:not([abp-blocked-element]):not([anticv-hidden]):not(:has(*)):not([rel])[href][target]'
                )
                imgs_leaf += soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer):not(form) a:not([abp-blocked-element]):not([anticv-hidden]):not(:has(*)):not([target])[href][rel]'
                )
                imgs_leaf += soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer):not(form) a:not([abp-blocked-element]):not([anticv-hidden])[href][target] span[style*="block"]'
                )
                if len(imgs_leaf) > 0:
                    logger.debug(
                        "%s - %s - PAGESOURCE - before soup select (imgs_leaf) : %d"
                        % (self.log_prefix, crawl_url_sld, len(imgs_leaf)))

                imgs_leaf = [
                    x for x in imgs_leaf
                    if x.string is None or len(x.string) == 0
                ]
                # make it a set here due to duplicates
                imgs_leaf = set(imgs_leaf)  
                if len(imgs_leaf) > 0:
                    logger.debug(
                        "%s - %s - PAGESOURCE - after soup select (imgs_leaf) : %d"
                        % (self.log_prefix, crawl_url_sld, len(imgs_leaf)))

                imgs_control_leaf = soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer):not(form) a:not([abp-blocked-element]):not([anticv-hidden]):not(:has(*))[href][target][rel]'
                )
                imgs_control_leaf += soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer):not(form) a:not([abp-blocked-element]):not([anticv-hidden]):not(:has(*)):not([rel])[href][target]'
                )
                imgs_control_leaf += soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer):not(form) a:not([abp-blocked-element]):not([anticv-hidden]):not(:has(*)):not([target])[href][rel]'
                )
                imgs_control_leaf += soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]):not(footer):not(form) a:not([abp-blocked-element]):not([anticv-hidden])[href][target] span[style*="block"]'
                )
                imgs_control_leaf = [
                    x for x in imgs_control_leaf
                    if x.string is None or len(x.string) == 0
                ]
                # make it a set here due to duplicates
                imgs_control_leaf = set(imgs_control_leaf)  
                if len(imgs_control_leaf) > 0:
                    logger.debug(
                        "%s - %s - PAGESOURCE - soup control found (imgs_leaf): %d"
                        % (self.log_prefix, crawl_url_sld,
                           len(imgs_control_leaf)))


                img_features_leaf = self.get_img_features(
                    imgs_leaf,
                    crawl_url_sld,
                    look_for_parents=False,
                    skip_if_siblings=False,
                    trial_key=trial_key,
                    soup_control=soup_control,
                    control_imgs=imgs_control_leaf,
                    crawl_url_tld=crawl_url_tld)

                logger.debug(
                    "%s - PAGESOURCE TIMER - imgs_leaf TIME --- %s seconds --- %s"
                    % (self.log_prefix, time.time() - start_time,
                       self.crawl_url))

                # this is the overall feature per trial
                suffix = "_leaf"
                # average the img features
                for key in img_features_leaf:
                    val = 0
                    if len(img_features_leaf.get(key)) > 0 and np.sum(
                            img_features_leaf.get(key)) > 0:
                        if "count" not in key:
                            val = np.average(img_features_leaf.get(key))
                        else:
                            val = np.sum(img_features_leaf.get(key))

                    target_found[key + suffix].append(val)

                start_time = time.time()

                # handle iframes that has src
                iframe_items = soup.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]) iframe[src]:not([abp-blocked-element]):not([anticv-hidden]):not([height="0"]):not([height="1"]):not([width="0"]):not([width="1"]):not([style*="display:none"]):not([style*="display: none"]):not([style*="visibility:hidden"]):not([style*="visibility: hidden"]):not([style*="opacity: 0"]):not([style*="opacity:0"])'
                )
                iframe_items_control = soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]) iframe[src]:not([abp-blocked-element]):not([anticv-hidden]):not([height="0"]):not([height="1"]):not([width="0"]):not([width="1"]):not([style*="display:none"]):not([style*="display: none"]):not([style*="visibility:hidden"]):not([style*="visibility: hidden"]):not([style*="opacity: 0"]):not([style*="opacity:0"])'
                )
                if len(iframe_items_control) > 0:
                    logger.debug(
                        "%s - %s - PAGESOURCE - soup control found (iframe_items): %d"
                        % (self.log_prefix, crawl_url_sld,
                           len(iframe_items_control)))


                iframe_features_items = self.get_img_features(
                    iframe_items,
                    crawl_url_sld,
                    look_for_parents=False,
                    skip_if_siblings=False,
                    trial_key=trial_key,
                    soup_control=soup_control,
                    control_imgs=iframe_items_control,
                    crawl_url_tld=crawl_url_tld)

                suffix = "_iframe"
                # average the img features
                for key in iframe_features_items:
                    val = 0

                    if len(iframe_features_items.get(key)) > 0 and np.sum(
                            iframe_features_items.get(key)) > 0:
                        if "count" not in key:
                            val = np.average(iframe_features_items.get(key))
                        else:
                            val = np.sum(iframe_features_items.get(key))

                    target_found[key + suffix].append(val)

                logger.debug(
                    "%s - PAGESOURCE TIMER - iframe_items TIME --- %s seconds --- %s"
                    % (self.log_prefix, time.time() - start_time,
                       self.crawl_url))

                # close soups and files
                soup.decompose()
                soup_control.decompose()
                soup_file.close()
                soup_file_control.close()

        pagesource_prefix = "pagesource_"

        # Calculate the text difference
        text_diff = set(variant_words) - set(control_words)

        text_diff_feature_key = pagesource_prefix + "var_" + "text_diff"
        features_dict[text_diff_feature_key] = len(text_diff)

        # Count the new lines
        new_lines_count = 0
        char_count = 0
        adblock_keyword_found = False
        adblock_keywords = [
            "disable", "detected", "adblocker", "whitelisting", "ad block",
            "support", "sponsor", "subscribe"
        ]
        for some_string in text_diff:
            new_lines_count += some_string.count("\n")
            char_count += len(some_string)
            some_string = some_string.lower()
            for adblock_keyword in adblock_keywords:
                if adblock_keyword in some_string:
                    adblock_keyword_found = True
                    break

        new_lines_feature_key = pagesource_prefix + "var_" + "new_lines"
        chars_feature_key = pagesource_prefix + "var_" + "characters"
        adblock_feature_key = pagesource_prefix + "var_" + "adblock_keyword"

        features_dict[new_lines_feature_key] = new_lines_count
        features_dict[chars_feature_key] = char_count
        features_dict[adblock_feature_key] = 1 if adblock_keyword_found else 0
        main_keys += [
            text_diff_feature_key, new_lines_feature_key, chars_feature_key,
            adblock_feature_key
        ]

        # for variant individual features
        for key in target_found_both[VARIANT].keys():
            avg = 0
            if len(target_found_both[VARIANT].get(key)) > 0:
                avg = np.average(target_found_both[VARIANT].get(key))
            features_dict[pagesource_prefix + "var_" + key] = round(avg, 2)

        if self.diff_queue and self.output_external_logs:
            self.diff_queue.put(str(features_dict))

        assert len(features_dict) == len(
            main_keys
        ), "Key lengths do not match: main_keys %d %s , feature keys %d %s " % (
            len(main_keys), str(main_keys), len(features_dict),
            str(features_dict))

        return features_dict


class PageSourceCorrespFeatureNewExtraction(PageSourceFeatureNewExtraction):
    def get_corres_elements(self, soup, selectors_and_children_count,
                            crawl_url_sld):
        found_corresp_imgs = 0
        found_corresp_iframes = 0
        child_threshold = 1
        found_threshold = 10
        for selector, children_count in selectors_and_children_count:
            #corresp_imgs_control = soup_control.select(selector + " img:not([abp-blocked-element]):not([anticv-hidden])")
            logger.debug("%s - PAGESOURCE CORRES: using selector %sd" %
                         (self.log_prefix, selector))
            corresp_imgs = soup.select(
                selector +
                ' img:not([abp-blocked-element]):not([height="0"]):not([height="1"]):not([width="0"]):not([width="1"]):not([style*="display:none"]):not([style*="display: none"]):not([style*="visibility:hidden"]):not([style*="visibility: hidden"]):not([style*="opacity: 0"]):not([style*="opacity:0"])'
            )
            corresp_imgs_len = 0
            for corres_img in corresp_imgs:
                has_ignore_keyword, _ = self.element_has_ignore_keywords(
                    corres_img, crawl_url_sld)
                smaller_than_ad_dimensions = is_smaller_than_ad_dimensions(
                    corres_img, crawl_url_sld, log_prefix=self.log_prefix)
                if not has_ignore_keyword and not smaller_than_ad_dimensions:
                    corresp_imgs_len += 1

            if corresp_imgs_len > 0:
                if corresp_imgs_len <= found_threshold or (
                        children_count >= child_threshold
                        and corresp_imgs_len < children_count * 2):
                    found_corresp_imgs += corresp_imgs_len
                    logger.debug(
                        "%s - PAGESOURCE CORRES: found corresponding images for selector %s, where variant iframes count: %d"
                        % (self.log_prefix, selector, len(corresp_imgs)))
                    logger.debug(
                        "%s - PAGESOURCE CORRES: found corresponding images for selector %s, where iframes would be: %s"
                        % (self.log_prefix, selector, str(corresp_imgs)))
                else:
                    logger.debug(
                        "%s - PAGESOURCE CORRES: found TOO many images for selector %s, where iframes would be: %d, control child count: %d"
                        % (self.log_prefix, selector, len(corresp_imgs),
                           children_count))

            corresp_iframes = soup.select(
                selector +
                ' iframe:not([abp-blocked-element]):not([height="0"]):not([height="1"]):not([width="0"]):not([width="1"]):not([style*="display:none"]):not([style*="display: none"]):not([style*="visibility:hidden"]):not([style*="visibility: hidden"]):not([style*="opacity: 0"]):not([style*="opacity:0"])'
            )
            corresp_iframes_len = 0
            for corresp_iframe in corresp_iframes:
                has_ignore_keyword, _ = self.element_has_ignore_keywords(
                    corresp_iframe, crawl_url_sld)
                smaller_than_ad_dimensions = is_smaller_than_ad_dimensions(
                    corresp_iframe, crawl_url_sld, log_prefix=self.log_prefix)
                if not has_ignore_keyword and not smaller_than_ad_dimensions:
                    corresp_iframes_len += 1

            if corresp_iframes_len > 0:
                if corresp_iframes_len <= found_threshold or (
                        children_count >= child_threshold
                        and corresp_iframes_len < children_count * 2):
                    found_corresp_iframes += corresp_iframes_len
                    logger.debug(
                        "%s - PAGESOURCE CORRES: found corresponding iframes for selector %s, where variant iframes count: %d"
                        % (self.log_prefix, selector, len(corresp_iframes)))
                    logger.debug(
                        "%s - PAGESOURCE CORRES: found corresponding iframes for selector %s, where iframes would be: %s"
                        % (self.log_prefix, selector, str(corresp_iframes)))
                else:
                    logger.debug(
                        "%s - PAGESOURCE CORRES: found TOO many iframes for selector %s, where iframes would be: %d, control child count: %d"
                        % (self.log_prefix, selector, len(corresp_iframes),
                           children_count))

        return found_corresp_imgs, found_corresp_iframes

    def extract_features(self):

        features_dict = {}
        # create custom structure
        crawl_trial_group = create_trial_group(self.diff_group,
                                               self.crawl_collection)

        crawl_url_tld = extract_tld(self.crawl_url)
        crawl_url_sld = get_second_level_domain_from_tld(crawl_url_tld)

        # init keys
        main_keys_default = []

        one_off_keys = [
            "corresp_img_fp", "corresp_img_tp", "corresp_iframe_fp",
            "corresp_iframe_tp"
        ]

        main_keys = list(main_keys_default) + one_off_keys

        target_found_both = dict()
        target_found_both[VARIANT] = dict()

        target_found = target_found_both[VARIANT]

        for main_key in main_keys:
            target_found[main_key] = []

        for trial_key in crawl_trial_group[VARIANT].keys():
            trial_inst = crawl_trial_group[VARIANT].get(trial_key)
            trial_inst_control = crawl_trial_group[CONTROL].get(trial_key)
            file_path, file_name = self.get_file_path_and_name(
                trial_inst, VARIANT)
            abs_file_path = file_path + os.sep + file_name
            file_path_control, file_name_control = self.get_file_path_and_name(
                trial_inst_control, CONTROL)
            abs_file_path_control = file_path_control + os.sep + file_name_control

            if os.path.isfile(abs_file_path):
                soup_file = open(abs_file_path, 'r')
                soup = BeautifulSoup(soup_file, 'html.parser')

                # get rid of noscript
                for noscript_el in soup.select("noscript"):
                    noscript_el.extract()

                # open up control soup
                soup_control = None
                if os.path.isfile(abs_file_path_control):
                    logger.debug(
                        "%s - PAGESOURCE - loading control file : %s" %
                        (self.log_prefix, abs_file_path_control))

                    soup_file_control = open(abs_file_path_control, 'r')
                    soup_control = BeautifulSoup(soup_file_control,
                                                 'html.parser')

                    # get rid of noscript
                    for noscript_el in soup_control.select("noscript"):
                        noscript_el.extract()

                # find where control iframes are
                start_time = time.time()

                iframe_items_control = soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]) iframe[src]:not([abp-blocked-element]):not([anticv-hidden]):not([height="0"]):not([height="1"]):not([width="0"]):not([width="1"]):not([style*="display:none"]):not([style*="display: none"]):not([style*="visibility:hidden"]):not([style*="visibility: hidden"]):not([style*="opacity: 0"]):not([style*="opacity:0"])'
                )
                iframe_items_control_fp = []
                iframe_items_control_tp = []
                for iframe_el in iframe_items_control:
                    has_ignore_keyword, _ = self.element_has_ignore_keywords(
                        iframe_el, crawl_url_sld)
                    if not has_ignore_keyword:
                        iframe_hrefs = find_urls_from_element(
                            iframe_el, crawl_url_sld)
                        first_party_hrefs, _ = split_info_first_and_third_party_requests(
                            iframe_hrefs, self.crawl_url)
                        if len(first_party_hrefs) > 0:
                            iframe_items_control_fp.append(iframe_el)
                        else:
                            iframe_items_control_tp.append(iframe_el)
                    else:
                        logger.debug(
                            "%s - PAGRSOURCE CORRES: iframe ignored  %s" %
                            (self.log_prefix, str(iframe_el)))

                # get iframes that have src docs and default to third party since there is no real url here
                iframe_items_control_srcdoc = soup_control.select(
                    ':not([abp-blocked-element]):not([anticv-hidden]) iframe[srcdoc]:not([abp-blocked-element]):not([anticv-hidden]):not([height="0"]):not([height="1"]):not([width="0"]):not([width="1"]):not([style*="display:none"]):not([style*="display: none"]):not([style*="visibility:hidden"]):not([style*="visibility: hidden"]):not([style*="opacity: 0"]):not([style*="opacity:0"])'
                )
                logger.debug(
                    "%s - PAGRSOURCE CORRES: iframe with SRCDOC items %d" %
                    (self.log_prefix, len(iframe_items_control_srcdoc)))
                iframe_items_control_tp += iframe_items_control_srcdoc

                for party_suffix, iframe_party_items in [
                    ('fp', iframe_items_control_fp),
                    ('tp', iframe_items_control_tp)
                ]:
                    logger.debug(
                        "%s - PAGRSOURCE CORRES: iframe items %s, party: %s" %
                        (self.log_prefix, str(iframe_party_items),
                         party_suffix))
                    iframe_parent_selectors_and_children_count = find_iframe_parent_structure(
                        iframe_party_items, log_prefix=self.log_prefix)
                    found_corresp_imgs, found_corresp_iframes = self.get_corres_elements(
                        soup, iframe_parent_selectors_and_children_count,
                        crawl_url_sld)
                    if found_corresp_imgs == 0 and found_corresp_iframes == 0:
                        logger.debug(
                            "%s - PAGESOURCE CORRES: rely on more generic selectors"
                            % (self.log_prefix))
                        # make selectors more general by ignoring some attribute values and rely on attr keys only
                        iframe_parent_selectors_and_children_count = find_iframe_parent_structure(
                            iframe_party_items,
                            log_prefix=self.log_prefix,
                            reduce_random_attributes=True)
                        found_corresp_imgs, found_corresp_iframes = self.get_corres_elements(
                            soup, iframe_parent_selectors_and_children_count,
                            crawl_url_sld)

                    logger.debug(
                        "%s - PAGESOURCE CORRES: found corresponding images overall %d, party: %s"
                        % (self.log_prefix, found_corresp_imgs, party_suffix))
                    logger.debug(
                        "%s - PAGESOURCE CORRES: found corresponding iframes overall %d, party: %s"
                        %
                        (self.log_prefix, found_corresp_iframes, party_suffix))

                    target_found["corresp_img_" +
                                 party_suffix].append(found_corresp_imgs)
                    target_found["corresp_iframe_" +
                                 party_suffix].append(found_corresp_iframes)

                logger.debug(
                    "%s - PAGESOURCE CORRES TIMER - corresp_imgs TIME --- %s seconds --- %s"
                    % (self.log_prefix, time.time() - start_time,
                       self.crawl_url))

                # close soups and files
                soup.decompose()
                soup_control.decompose()
                soup_file.close()
                soup_file_control.close()

        pagesource_prefix = "pagesourcecorres_"

        # for variant individual features
        for key in target_found_both[VARIANT].keys():
            avg = 0
            if len(target_found_both[VARIANT].get(key)) > 0:
                avg = np.average(target_found_both[VARIANT].get(key))
            features_dict[pagesource_prefix + "var_" + key] = round(avg, 2)

        if self.diff_queue and self.output_external_logs:
            self.diff_queue.put(str(features_dict))

        assert len(features_dict) == len(
            main_keys
        ), "PAGESOURCE CORRES: Key lengths do not match: main_keys %d %s , feature keys %d %s " % (
            len(main_keys), str(main_keys), len(features_dict),
            str(features_dict))

        return features_dict
