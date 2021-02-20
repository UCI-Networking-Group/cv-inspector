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
import datetime
import json
import logging

from cvinspector.common.dommutation_utils import NODES_ADDED, NODES_REMOVED, \
    ATTRIBUTE_CHANGED, TEXT_CHANGED, DOM_CONTENT_LOADED
from cvinspector.common.dommutation_utils import get_nodes_added_key, get_nodes_removed_key
from cvinspector.common.utils import ABP_BLOCKED_ELEMENT, ERR_BLOCKED_BY_CLIENT, JSON_DOMMUTATION_KEY, \
    JSON_WEBREQUEST_KEY
from cvinspector.common.utils import ANTICV_ANNOTATION_PREFIX

logger = logging.getLogger(__name__)
# logger.setLevel("DEBUG")

TIME_KEY = "time"
TIME_KEY__WR = "requestTime"


def auto_bin_time_series_csv(json_file_path,
                             json_file_path__wr,
                             output_file_name,
                             time_step_ms,
                             max_seconds_later=None,
                             auto_determine_time=True):
    def _by_time(event):
        return event[TIME_KEY]

    def _by_time_wr(event):
        return event["event"][TIME_KEY__WR]

    def _get_range_key(event, range_keys, event_key=TIME_KEY):
        event_time = event.get(event_key)
        if event_time is None:
            event_time = event["event"][event_key]

        for index, range_step in enumerate(range_keys):
            next_time_index = index + 1
            if next_time_index == len(range_keys):
                # we are at the last step, so only check if event_time is larger
                if range_step <= event_time:
                    return range_step
            else:
                # if within current step and next step
                if range_step <= event_time <= range_keys[next_time_index]:
                    return range_step

    def _write_header(csv_writer):
        header = [
            "bin_norm", "Date", "blocked", "web_req_blocked", "elem_blocked",
            "snippet_blocked", "nodes_added", "nodes_removed",
            "attribute_changed", "text_changed", "dom_content_loaded",
            "iframe_src_changed", "iframe_blocked", "total_changes"
        ]
        csv_writer.writerow(header)

    output_file_opened = open(output_file_name + ".csv", 'w')
    csvwriter = csv.writer(output_file_opened)
    _write_header(csvwriter)

    dom_json_content = None
    wr_json_content = None
    # read in DOM Mutation file
    with open(json_file_path, 'r') as json_file:
        dom_json_content = json.load(json_file)

    try:
        dom_events = dom_json_content[JSON_DOMMUTATION_KEY]
    except KeyError:
        logger.debug("DOM JSON has no content")
        return

    dom_events_filtered = [x for x in dom_events if TIME_KEY in x]

    # read in WebRequest file
    with open(json_file_path__wr, 'r') as json_file__wr:
        wr_json_content = json.load(json_file__wr)

    wr_events = []
    try:
        wr_events = wr_json_content[JSON_WEBREQUEST_KEY]
    except KeyError:
        logger.debug("WR has no content")

    wr_events_filtered = [
        x for x in wr_events if TIME_KEY__WR in x.get("event")
    ]

    # sort by time
    dom_events_filtered.sort(key=_by_time)

    # sort by time
    wr_events_filtered.sort(key=_by_time_wr)

    # bin by time step
    first_time = 0  # default it to zero just in case there are no events
    first_time__wr = 0
    if len(dom_events_filtered) > 0:
        first_time = dom_events_filtered[0][TIME_KEY]
        last_time = dom_events_filtered[-1][TIME_KEY]
    if len(wr_events_filtered) > 0:
        first_time__wr = wr_events_filtered[0]["event"][TIME_KEY__WR]

    # if first_time__wr is smaller and not zero
    if first_time__wr != 0 and first_time > first_time__wr:
        first_time = first_time__wr

    if max_seconds_later is not None and not auto_determine_time:
        last_time = first_time + (max_seconds_later * 1000)
    else:
        # add extra 2 seconds
        last_time = last_time + (2 * 1000)
        last_time_based_on_max = first_time + (max_seconds_later * 1000)
        if last_time > last_time_based_on_max:
            last_time = last_time_based_on_max

    range_keys = list(range(int(first_time), int(last_time), time_step_ms))

    # build dict to hold range_key --> [events]
    events_binned = dict()
    for k in range_keys:
        events_binned.setdefault(k, [])

    for event in dom_events_filtered:
        event_key = _get_range_key(event, range_keys)
        events_binned[event_key].append(event)

    for event in wr_events_filtered:
        event_key = _get_range_key(event, range_keys, event_key=TIME_KEY__WR)
        events_binned[event_key].append(event)

    verify_event_count = 0
    bin_norm = 0
    bin_with_time_step = 0
    for bin_key in events_binned.keys():
        bin_size = len(events_binned.get(bin_key))
        event_time_iso = datetime.datetime.fromtimestamp(bin_key /
                                                         1000).isoformat()

        verify_event_count += bin_size
        block_count = 0
        not_block_count = 0
        attribute_changed_count = 0
        iframe_src_attribute_changed_count = 0
        iframe_blocked = 0
        text_changed_count = 0
        node_add_count = 0
        node_remove_count = 0
        wr_block_count = 0
        elem_blocked_count = 0
        snippet_blocked_count = 0
        dom_content_loaded = 0  # we expect at most one event here

        for event in events_binned.get(bin_key):
            event_item = event.get("event")
            if event_item is None:
                continue
            event_type = event.get("type")
            if event_type == "event":
                event_type = event_item.get("type")
            elif event_type != "onErrorOccurred":
                continue

            if event_type is None:
                continue

            is_blocked = False
            if event_type == DOM_CONTENT_LOADED:
                dom_content_loaded += 1
            if event_type == NODES_ADDED:
                for key, defining_text, is_text_node, is_snippet_blocked in get_nodes_added_key(
                        event_item):
                    # snippet blocks are considerd to be a node added, so count it only as a snippet block
                    if is_snippet_blocked:
                        is_blocked = True
                        snippet_blocked_count += 1
                    elif is_text_node:
                        text_changed_count += 1
                    else:
                        node_add_count += 1

            if event_type == NODES_REMOVED:
                for key, defining_text, is_text_node, _ in get_nodes_removed_key(
                        event_item):
                    if is_text_node:
                        text_changed_count += 1
                    else:
                        node_remove_count += 1
            if event_type == ATTRIBUTE_CHANGED:
                target_type = event_item["targetType"] or ""
                target_type = target_type.lower()
                # we ignore events that we purposely made to mark hidden elements
                if ANTICV_ANNOTATION_PREFIX in event_item["attribute"]:
                    continue
                # here we don't count the block event as a real attribute change
                if event_item["attribute"] == ABP_BLOCKED_ELEMENT:
                    is_blocked = True
                    elem_blocked_count += 1
                    if "iframe" in target_type:
                        iframe_blocked += 1
                else:
                    new_value = event_item["newValue"] or ""
                    # mark as iframe_src_attribute_changed event as well
                    if event_item["attribute"] == "src" and \
                            "iframe" in target_type and len(new_value) > 0:
                        iframe_src_attribute_changed_count += 1
                    attribute_changed_count += 1
            if event_type == TEXT_CHANGED:
                text_changed_count += 1
            if event_type == "onErrorOccurred":
                details = event_item.get("details")
                if details:
                    try:
                        details_json = json.loads(details)
                        if ERR_BLOCKED_BY_CLIENT in details_json.get("error"):
                            is_blocked = True
                            wr_block_count += 1
                        else:
                            continue
                    except Exception as e:
                        logger.debug("Could not parse details json")
                        logger.debug(e)
                        continue
                else:
                    continue

            # keep track of blocked or not
            if is_blocked:
                block_count += 1
            else:
                not_block_count += 1

        total_changes = node_add_count + node_remove_count + attribute_changed_count + text_changed_count
        # write row
        csvwriter.writerow([
            bin_norm, event_time_iso, block_count, wr_block_count,
            elem_blocked_count, snippet_blocked_count, node_add_count,
            node_remove_count, attribute_changed_count, text_changed_count,
            dom_content_loaded, iframe_src_attribute_changed_count,
            iframe_blocked, total_changes
        ])

        # update bin_norm
        bin_norm += time_step_ms
        bin_with_time_step += time_step_ms

    # close file
    output_file_opened.close()
