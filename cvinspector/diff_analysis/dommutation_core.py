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

from cvinspector.common.dommutation_utils import get_nodes_added_key, get_nodes_removed_key, get_attribute_changed_key, \
    get_text_changed_key, \
    NODES_ADDED, NODES_REMOVED, ATTRIBUTE_CHANGED, TEXT_CHANGED
from cvinspector.common.utils import ABP_BLOCKED_ELEMENT, ABP_BLOCKED_SNIPPET, ANTICV_ANNOTATION_PREFIX, CONTROL, \
    VARIANT, get_dom_mutation_from_raw_json
from cvinspector.diff_analysis.utils import create_trial_group

logger = logging.getLogger(__name__)


# logger.setLevel("DEBUG")


def get_dom_differences_only(diff_group,
                             crawler_group_name,
                             crawl_collection,
                             debug_queue,
                             thread_name=None,
                             output_external_logs=True):
    crawl_trial_group = create_trial_group(diff_group, crawl_collection)
    control_instance_ids = []
    control_instance_ids_str = []

    main_url = ""
    for trial_key in crawl_trial_group[CONTROL].keys():
        trial_inst = crawl_trial_group[CONTROL].get(trial_key)
        main_url = trial_inst.get("url")
        control_instance_ids.append(trial_inst.get("_id"))
        control_instance_ids_str.append(str(trial_inst.get("_id")))

    control_node_added_events = dict()
    control_node_removed_events = dict()
    control_attribute_changed_events = dict()
    control_text_changed_events = dict()
    control_text_node_added_events = dict()
    control_text_node_removed_events = dict()

    # we parse all trials (control)
    for trial_key in crawl_trial_group[CONTROL].keys():
        trial_inst = crawl_trial_group[CONTROL].get(trial_key)
        crawl_instance_id = trial_inst.get("_id")

        control_event_cursor = get_dom_mutation_from_raw_json(
            trial_inst.get("file_path"))
        for event in control_event_cursor:
            event_item = event.get("event")
            if event_item is None:
                continue
            event_type = event_item.get("type")
            if event_type is None:
                continue

            ## REMINDER: control side has no custom events like ABP_BLOCKED_SNIPPET
            if event_type == NODES_ADDED:
                # get_nodes_added_key returns list of tuples
                index_node_added = 0
                for key, defining_text, is_text_node, _ in get_nodes_added_key(
                        event_item):
                    if is_text_node:
                        if key not in control_text_node_added_events:
                            control_text_node_added_events[key] = []
                        control_text_node_added_events[key].append(
                            (trial_inst, event, index_node_added))
                    else:
                        if key not in control_node_added_events:
                            control_node_added_events[key] = []
                        control_node_added_events[key].append(
                            (trial_inst, event, defining_text,
                             index_node_added))
                    index_node_added += 1

            if event_type == NODES_REMOVED:
                index_node_removed = 0
                # get_nodes_removed_key returns list of tuples
                for key, defining_text, is_text_node, _ in get_nodes_removed_key(
                        event_item):
                    if is_text_node:
                        if key not in control_text_node_removed_events:
                            control_text_node_removed_events[key] = []
                        control_text_node_removed_events[key].append(
                            (trial_inst, event, index_node_removed))
                    else:
                        if key not in control_node_removed_events:
                            control_node_removed_events[key] = []
                        control_node_removed_events[key].append(
                            (trial_inst, event, defining_text,
                             index_node_removed))
                    index_node_removed += 1

            if event_type == ATTRIBUTE_CHANGED:
                attribute = event_item.get("attribute")
                # ignore short attributes that are usually used for svgs
                if len(attribute) <= 2:
                    continue
                if ANTICV_ANNOTATION_PREFIX in attribute:
                    continue

                key, defining_text = get_attribute_changed_key(event_item)

                if key not in control_attribute_changed_events:
                    control_attribute_changed_events[key] = []
                control_attribute_changed_events[key].append(
                    (trial_inst, event, defining_text))

            if event_type == TEXT_CHANGED:
                key, text_diff = get_text_changed_key(event_item)
                if key not in control_text_changed_events:
                    control_text_changed_events[key] = []
                control_text_changed_events[key].append(
                    (trial_inst, event, text_diff))

    variant_node_added_events = dict()
    variant_node_removed_events = dict()
    variant_attribute_changed_events = dict()
    variant_text_changed_events = dict()
    variant_text_node_added_events = dict()
    variant_text_node_removed_events = dict()

    # this only exists in variant
    variant_blocked_events = dict()

    # we parse all trials (variant)
    variant_instance_ids = []
    variant_instance_ids_str = []
    for trial_key in crawl_trial_group[VARIANT].keys():
        trial_inst = crawl_trial_group[VARIANT].get(trial_key)
        crawl_instance_id = trial_inst.get("_id")
        variant_instance_ids_str.append(str(crawl_instance_id))
        variant_instance_ids.append(crawl_instance_id)
        variant_event_cursor = get_dom_mutation_from_raw_json(
            trial_inst.get("file_path"))

        for event in variant_event_cursor:
            event_item = event.get("event")
            if event_item is None:
                continue
            event_type = event_item.get("type")
            if event_type is None:
                continue

            if event_type == NODES_ADDED:
                index_node_added = 0
                for key, defining_text, is_text_node, is_snippet_blocked in get_nodes_added_key(
                        event_item):
                    # skip our custom block events
                    if is_snippet_blocked:
                        tmp_key = key + ABP_BLOCKED_SNIPPET
                        if tmp_key not in variant_blocked_events:
                            variant_blocked_events[tmp_key] = []
                        variant_blocked_events[tmp_key].append(
                            (trial_inst, event, defining_text,
                             index_node_added))
                        continue
                    if is_text_node:
                        if key not in variant_text_node_added_events:
                            variant_text_node_added_events[key] = []
                        variant_text_node_added_events[key].append(
                            (trial_inst, event, index_node_added))
                    else:
                        if key not in variant_node_added_events:
                            variant_node_added_events[key] = []
                        variant_node_added_events[key].append(
                            (trial_inst, event, defining_text,
                             index_node_added))
                    index_node_added += 1

            if event_type == NODES_REMOVED:
                index_node_removed = 0
                for key, defining_text, is_text_node, _ in get_nodes_removed_key(
                        event_item):
                    if is_text_node:
                        if key not in variant_text_node_removed_events:
                            variant_text_node_removed_events[key] = []
                        variant_text_node_removed_events[key].append(
                            (trial_inst, event, index_node_removed))
                    else:
                        if key not in variant_node_removed_events:
                            variant_node_removed_events[key] = []
                        variant_node_removed_events[key].append(
                            (trial_inst, event, defining_text,
                             index_node_removed))
                    index_node_removed += 1

            if event_type == ATTRIBUTE_CHANGED:
                attribute = event_item.get("attribute")
                # ignore short attributes that are usually used for svgs
                if len(attribute) <= 2:
                    continue
                # skip our custom event for whether element is hidden
                if ANTICV_ANNOTATION_PREFIX in attribute:
                    continue

                key, defining_text = get_attribute_changed_key(event_item)

                # skip our custom block events
                if ABP_BLOCKED_ELEMENT == attribute:
                    tmp_key = key + ABP_BLOCKED_ELEMENT
                    if tmp_key not in variant_blocked_events:
                        variant_blocked_events[tmp_key] = []
                    variant_blocked_events[tmp_key].append(
                        (trial_inst, event, defining_text, 0))
                    continue
                if key not in variant_attribute_changed_events:
                    variant_attribute_changed_events[key] = []
                variant_attribute_changed_events[key].append(
                    (trial_inst, event, defining_text))
            if event_type == TEXT_CHANGED:
                key, text_diff = get_text_changed_key(event_item)
                if key not in variant_text_changed_events:
                    variant_text_changed_events[key] = []
                variant_text_changed_events[key].append(
                    (trial_inst, event, text_diff))

    # turn keys into sets and subtract control and variant
    # find all events not in second_set, remove common ones
    # each dict is key -> list(events)
    def _dom_diff(main_set, second_set):
        main_set_remaining = dict()
        for key in main_set:
            # add all into remaining
            if key not in second_set:
                main_set_remaining[key] = main_set.get(key)
                continue

            main_events = main_set.get(key)
            second_events = second_set.get(key)
            diff = len(main_events) - len(second_events)
            if diff > 0:
                # retrieve the last elements not in common
                main_events_remaining = main_events[-1 * diff:]
                main_set_remaining[key] = main_events_remaining

        return main_set_remaining

    ## Nodes added
    logger.debug("%s - Original node added: control %d , variant  %d" %
                 (str(thread_name), len(control_node_added_events),
                  len(variant_node_added_events)))
    control_only__node_added = _dom_diff(control_node_added_events,
                                         variant_node_added_events)
    variant_only__node_added = _dom_diff(variant_node_added_events,
                                         control_node_added_events)
    logger.debug("%s - After node added subtracted: control %d, variant : %d" %
                 (str(thread_name), len(control_only__node_added),
                  len(variant_only__node_added)))

    ## Nodes removed

    logger.debug("%s - Original node removed: control %d , variant  %d" %
                 (str(thread_name), len(control_node_removed_events),
                  len(variant_node_removed_events)))
    control_only__node_removed = _dom_diff(control_node_removed_events,
                                           variant_node_removed_events)
    variant_only__node_removed = _dom_diff(variant_node_removed_events,
                                           control_node_removed_events)
    logger.debug(
        "%s - After node removed subtracted: control %d, variant : %d" %
        (str(thread_name), len(control_only__node_removed),
         len(variant_only__node_removed)))

    ## Attribute Changed
    logger.debug("%s - Original node attr changed: control %d , variant  %d" %
                 (str(thread_name), len(control_attribute_changed_events),
                  len(variant_attribute_changed_events)))
    control_only__attribute_changed = _dom_diff(
        control_attribute_changed_events, variant_attribute_changed_events)
    variant_only__attribute_changed = _dom_diff(
        variant_attribute_changed_events, control_attribute_changed_events)
    logger.debug(
        "%s - After node attr changed subtracted: control %d, variant : %d" %
        (str(thread_name), len(control_only__attribute_changed),
         len(variant_only__attribute_changed)))

    ## Text Changed
    logger.debug(
        "%s - Original node text node added changed: control %d , variant  %d , url: %s"
        % (str(thread_name), len(control_text_node_added_events),
           len(variant_text_node_added_events), main_url))
    control_only__text_node_added = _dom_diff(control_text_node_added_events,
                                              variant_text_node_added_events)
    variant_only__text_node_added = _dom_diff(variant_text_node_added_events,
                                              control_text_node_added_events)
    logger.debug(
        "%s - After node text node added subtracted: control %d, variant : %d, url: %s"
        % (str(thread_name), len(control_only__text_node_added),
           len(variant_only__text_node_added), main_url))

    logger.debug(
        "%s - Original node text node removed changed: control %d , variant  %d , url: %s"
        % (str(thread_name), len(control_text_node_removed_events),
           len(variant_text_node_removed_events), main_url))
    control_only__text_node_removed = _dom_diff(
        control_text_node_removed_events, variant_text_node_removed_events)
    variant_only__text_node_removed = _dom_diff(
        variant_text_node_removed_events, control_text_node_removed_events)
    logger.debug(
        "%s - After node text node removed subtracted: control %d, variant : %d, url: %s"
        % (str(thread_name), len(control_only__text_node_removed),
           len(variant_only__text_node_removed), main_url))

    logger.debug(
        "%s - Original node text changed: control %d , variant  %d , url: %s" %
        (str(thread_name), len(control_text_changed_events),
         len(variant_text_changed_events), main_url))
    control_only__text_changed = _dom_diff(control_text_changed_events,
                                           variant_text_changed_events)
    variant_only__text_changed = _dom_diff(variant_text_changed_events,
                                           control_text_changed_events)
    logger.debug(
        "%s - After node text changed subtracted: control %d, variant : %d, url: %s"
        % (str(thread_name), len(control_only__text_changed),
           len(variant_only__text_changed), main_url))

    if output_external_logs:
        if len(variant_only__text_changed) > 0:
            control_keys = list(control_text_changed_events.keys())
            variant_keys = list(variant_text_changed_events.keys())
            variant_remaining_values = list(
                variant_only__text_changed.values())
            debug_queue.put(
                "%s - Diff Analysis : Control text events %s , url: %s" %
                (str(thread_name), str(control_keys[:10]), main_url))
            debug_queue.put(
                "%s - Diff Analysis : Variant text events %s , url: %s" %
                (str(thread_name), str(variant_keys[:10]), main_url))
            debug_queue.put(
                "%s - Diff Analysis : Variant text remaining %s , url: %s" %
                (str(thread_name), str(
                    variant_remaining_values[:10]), main_url))

        if len(variant_only__attribute_changed.values()) > 500:
            control_keys = list(control_attribute_changed_events.keys())
            variant_keys = list(variant_attribute_changed_events.keys())
            variant_remaining_values = list(
                variant_only__attribute_changed.values())
            debug_queue.put(
                "%s - Diff Analysis : Control attribute events %s , url: %s" %
                (str(thread_name), str(control_keys[:10]), main_url))
            debug_queue.put(
                "%s - Diff Analysis : Variant attribute events %s , url: %s" %
                (str(thread_name), str(variant_keys[:10]), main_url))
            debug_queue.put(
                "%s - Diff Analysis : Variant attribute remaining %s , url: %s"
                % (str(thread_name), str(
                    variant_remaining_values[:10]), main_url))

    # for each type of leftover keys, get the values of ObjectIds only
    control_only__node_added__objs = []
    control_only__node_added__defining = []

    for key in control_only__node_added:
        values = control_only__node_added.get(key)
        for trial_instance, obj, _, index_obj in values:
            control_only__node_added__objs.append((obj, index_obj))
            control_only__node_added__defining.append(key)

    control_only__node_removed__objs = []
    control_only__node_removed__defining = []

    for key in control_only__node_removed:
        values = control_only__node_removed.get(key)
        for trial_instance, obj, _, index_obj in values:
            control_only__node_removed__objs.append((obj, index_obj))
            control_only__node_removed__defining.append(key)

    control_only__attribute_changed__objs = []
    control_only__attribute_changed__defining = []

    for key in control_only__attribute_changed:
        values = control_only__attribute_changed.get(key)
        for trial_instance, obj, _ in values:
            control_only__attribute_changed__objs.append(obj)
            control_only__attribute_changed__defining.append(key)

    control_only__text_node_added__objs = []

    for key in control_only__text_node_added:
        values = control_only__text_node_added.get(key)
        for _, obj, index_obj in values:
            control_only__text_node_added__objs.append((obj, index_obj))

    control_only__text_node_removed__objs = []

    for key in control_only__text_node_removed:
        values = control_only__text_node_removed.get(key)
        for _, obj, index_obj in values:
            control_only__text_node_removed__objs.append((obj, index_obj))

    control_only__text_changed__objs = []

    for key in control_only__text_changed:
        values = control_only__text_changed.get(key)
        for _, obj, text_diff in values:
            control_only__text_changed__objs.append((obj, text_diff))

    control_row = {
        "crawl_group_name": crawler_group_name,
        "crawl_instance_ids_str": ",".join(control_instance_ids_str),
        "crawl_instance_ids": control_instance_ids,
        "node_added": control_only__node_added__objs,
        "node_removed": control_only__node_removed__objs,
        "attribute_changed": control_only__attribute_changed__objs,
        "text_changed": control_only__text_changed__objs,
        "text_node_added": control_only__text_node_added__objs,
        "text_node_removed": control_only__text_node_removed__objs
    }

    control_row["node_added_defining"] = control_only__node_added__defining
    control_row["node_added_defining_too_large"] = "false"
    control_row["node_removed_defining"] = control_only__node_removed__defining
    control_row["node_removed_defining_too_large"] = "false"
    control_row[
        "attribute_changed_defining"] = control_only__attribute_changed__defining
    control_row["attribute_changed_defining_too_large"] = "false"

    # for each type of leftover keys, get the values of ObjectIds only
    variant_only__node_added__objs = []
    variant_only__node_added__defining = []

    for key in variant_only__node_added:
        values = variant_only__node_added.get(key)
        for trial_instance, obj, _, index_obj in values:
            variant_only__node_added__objs.append((obj, index_obj))
            variant_only__node_added__defining.append(key)

    variant_only__node_removed__objs = []
    variant_only__node_removed__defining = []

    for key in variant_only__node_removed:
        values = variant_only__node_removed.get(key)
        for trial_instance, obj, _, index_obj in values:
            variant_only__node_removed__objs.append((obj, index_obj))
            variant_only__node_removed__defining.append(key)

    variant_only__attribute_changed__objs = []
    variant_only__attribute_changed__defining = []

    for key in variant_only__attribute_changed:
        values = variant_only__attribute_changed.get(key)
        for trial_instance, obj, _ in values:
            variant_only__attribute_changed__objs.append(obj)
            variant_only__attribute_changed__defining.append(key)

    variant_only__text_node_added__objs = []

    for key in variant_only__text_node_added:
        values = variant_only__text_node_added.get(key)
        for trial_instance, obj, index_obj in values:
            variant_only__text_node_added__objs.append((obj, index_obj))

    variant_only__text_node_removed__objs = []

    for key in variant_only__text_node_removed:
        values = variant_only__text_node_removed.get(key)
        for trial_instance, obj, index_obj in values:
            variant_only__text_node_removed__objs.append((obj, index_obj))

    variant_only__text_changed__objs = []

    for key in variant_only__text_changed:
        values = variant_only__text_changed.get(key)
        for trial_instance, obj, text_diff in values:
            variant_only__text_changed__objs.append((obj, text_diff))

    variant_row = {
        "crawl_group_name": crawler_group_name,
        "crawl_instance_ids_str": ",".join(variant_instance_ids_str),
        "crawl_instance_ids": variant_instance_ids,
        "node_added": variant_only__node_added__objs,
        "node_removed": variant_only__node_removed__objs,
        "attribute_changed": variant_only__attribute_changed__objs,
        "text_changed": variant_only__text_changed__objs,
        "text_node_added": variant_only__text_node_added__objs,
        "text_node_removed": variant_only__text_node_removed__objs,
        "blocked_events": variant_blocked_events
    }

    variant_row["node_added_defining"] = variant_only__node_added__defining
    variant_row["node_added_defining_too_large"] = "false"
    variant_row["node_removed_defining"] = variant_only__node_removed__defining
    variant_row["node_removed_defining_too_large"] = "false"

    variant_row[
        "attribute_changed_defining"] = variant_only__attribute_changed__defining
    variant_row["attribute_changed_defining_too_large"] = "false"

    logger.debug("Done WR finding diffs for diff group %s" % main_url)

    return control_row, variant_row
