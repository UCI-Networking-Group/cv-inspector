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
import textdistance

from cvinspector.common.utils import CONTROL, VARIANT, get_webrequests_from_raw_json, get_blocked_webrequests
from cvinspector.common.webrequests_utils import get_domain_only_from_url, get_path_and_query_params, remove_last_path, \
    extract_tld, get_second_level_domain_from_tld, get_domain_only_from_tld
from cvinspector.diff_analysis.utils import contains_important_resource, create_trial_group

logger = logging.getLogger(__name__)
#logger.setLevel("DEBUG")

UNKNOWN_REQ_CONTENT_LENGTH = -1


def get_wr_trial_aggregate(crawl_trial_group,
                           control_or_variant,
                           debug_queue=None,
                           output_external_logs=False,
                           thread_name=None):

    # used for content-type (what type the request contains like images, js, json)
    variant_req_to_type = dict()  
    #used for resource/misc type (what the req will be used for like websocket, xhr, script) (cache-control as well)
    variant_req_to_resource_type = dict()  
    # used to capture domain+path --> list of webrequests
    variant_domain_path_to_requests = dict()  

    variant_instance_ids = []
    variant_instance_ids_str = []
    # we parse all trials (variant)
    variant_trial_plot = dict()
    variant_trial_plot_diff = []
    variant_trial_domains = dict()
    variant_trial_domains_diff = []
    variant_blocked_urls_by_trial = dict()

    trial_count = 0
    variant_prev_set = []
    variant_prev_domain_set = []

    for trial_key in crawl_trial_group[control_or_variant].keys():
        trial_inst = crawl_trial_group[control_or_variant].get(trial_key)
        crawl_instance_id = trial_inst.get("_id")
        variant_instance_ids_str.append(str(crawl_instance_id))
        variant_instance_ids.append(crawl_instance_id)
        variant_webreqs = get_webrequests_from_raw_json(
            trial_inst.get("file_path"), "onCompleted")
        if control_or_variant == VARIANT:
            variant_blocked_urls = get_blocked_webrequests(
                trial_inst.get("file_path"))
            variant_blocked_urls_by_trial[trial_key] = variant_blocked_urls

        for req_item in variant_webreqs:
            #req_item_id = req_item.get("_id")
            req_url = req_item.get("event").get("url")
            req_details = req_item.get("event").get("details")
            req_content_type = "unknown"
            req_resource_type = "unknown"
            req_cache_control_type = "none"
            req_content_length = UNKNOWN_REQ_CONTENT_LENGTH
            response_headers = None
            if req_details:
                try:
                    req_details_json = json.loads(req_details)
                    if req_details_json.get("statusCode") != 200:
                        continue
                    req_resource_type = req_details_json.get("type")
                    response_headers = req_details_json.get("responseHeaders")
                    if response_headers and len(response_headers) > 0:
                        for header_item in response_headers:
                            if header_item.get(
                                    "name").lower() == "content-type":
                                req_content_type = header_item.get("value")
                            if header_item.get(
                                    "name").lower() == "cache-control":
                                req_cache_control_type = header_item.get(
                                    "value")
                            # we need to make that this is an int
                            if header_item.get(
                                    "name").lower() == "content-length":
                                req_content_length = header_item.get("value")
                                if req_content_length is None:
                                    req_content_length = UNKNOWN_REQ_CONTENT_LENGTH
                                if req_content_length is not None:
                                    try:
                                        req_content_length = int(
                                            req_content_length)
                                    except ValueError:
                                        req_content_length = UNKNOWN_REQ_CONTENT_LENGTH

                except:
                    logger.debug(
                        "%s - Could not load json from webrequests json %s, crawl_instance_id %s"
                        % (str(thread_name), req_item.get("_id"),
                           crawl_instance_id))

            req_domain = get_domain_only_from_url(req_url)
            req_path, _ = get_path_and_query_params(req_url)
            domain_path = req_domain + req_path
            if domain_path not in variant_domain_path_to_requests:
                variant_domain_path_to_requests[domain_path] = []
            variant_domain_path_to_requests[domain_path].append(req_url)

            if req_url not in variant_req_to_type:
                variant_req_to_type[req_url] = (trial_inst, req_item,
                                                req_content_type)
            if req_url not in variant_req_to_resource_type:
                variant_req_to_resource_type[req_url] = {
                    "trial": trial_inst,
                    "request": req_item,
                    "data": {}
                }
            # set the values for misc types
            if "resource_type" not in variant_req_to_resource_type[req_url][
                    "data"] or req_resource_type != "unknown":
                variant_req_to_resource_type[req_url]["data"][
                    "resource_type"] = req_resource_type
            if "cache_control" not in variant_req_to_resource_type[
                    req_url] or req_cache_control_type != "none":
                variant_req_to_resource_type[req_url]["data"][
                    "cache_control"] = req_cache_control_type

            if "content_length" not in variant_req_to_resource_type[req_url]:
                variant_req_to_resource_type[req_url]["data"][
                    "content_length"] = req_content_length
                if req_content_length == UNKNOWN_REQ_CONTENT_LENGTH:
                    pass
            elif req_content_length != UNKNOWN_REQ_CONTENT_LENGTH and variant_req_to_resource_type[
                    req_url]["data"]["content_length"] != req_content_length:
                variant_req_to_resource_type[req_url]["data"][
                    "content_length"] = req_content_length

        trial_count += 1
        variant_trial_plot[trial_count] = set(variant_req_to_type.keys())
        if debug_queue and output_external_logs:
            debug_queue.put(
                str(thread_name) + " *********FULL URL VARIANT " + "\n")
            debug_queue.put(
                str(thread_name) + " trial count " + str(trial_count) +
                ", number of variant urls: " +
                str(len(variant_trial_plot[trial_count])) + "\n")

        diff = list(variant_trial_plot[trial_count] - set(variant_prev_set))
        diff.sort()
        if debug_queue and output_external_logs:
            debug_queue.put(str(thread_name) + " " + str(diff) + "\n")
        variant_prev_set = list(variant_trial_plot[trial_count])
        variant_trial_plot_diff.append(len(diff))

        variant_domains = []

        for req in set(variant_req_to_type.keys()):
            domain = get_domain_only_from_url(req)
            variant_domains.append(domain)

        variant_trial_domains[trial_count] = set(variant_domains)
        if debug_queue and output_external_logs:
            debug_queue.put(
                str(thread_name) + " *********DOMAIN VARIANT " + "\n")
            debug_queue.put(
                str(thread_name) + " trial count " + str(trial_count) +
                ", number of variant domains: " +
                str(len(variant_trial_domains[trial_count])) + "\n")

        diff = list(variant_trial_domains[trial_count] -
                    set(variant_prev_domain_set))
        diff.sort()
        if debug_queue and output_external_logs:
            debug_queue.put(str(thread_name) + " " + str(diff) + "\n")
        variant_prev_domain_set = list(variant_trial_domains[trial_count])
        variant_trial_domains_diff.append(len(diff))


    return variant_req_to_type, variant_req_to_resource_type, variant_domain_path_to_requests, variant_instance_ids, \
        variant_instance_ids_str, variant_trial_plot, variant_trial_plot_diff, variant_trial_domains, variant_trial_domains_diff, \
        variant_blocked_urls_by_trial


def get_diff_requests_sets(control_domain_path_to_requests,
                           variant_domain_path_to_requests,
                           debug_queue=None,
                           output_external_logs=False,
                           thread_name=None):
    set_control_domain_paths = set(control_domain_path_to_requests.keys())
    set_variant_domain_paths = set(variant_domain_path_to_requests.keys())
    if debug_queue and output_external_logs:
        debug_queue.put(
            str(thread_name) + " - Domain Paths: Control Count: " +
            str(len(set_control_domain_paths)))
        debug_queue.put(
            str(thread_name) + " - Domain Paths: Variant Count: " +
            str(len(set_variant_domain_paths)))

    set_control_domain_paths_only = set_control_domain_paths - set_variant_domain_paths
    set_variant_domain_paths_only = set_variant_domain_paths - set_control_domain_paths
    if debug_queue and output_external_logs:
        debug_queue.put(
            str(thread_name) +
            " - Domain Paths: After Diff - Control Count: " +
            str(len(set_control_domain_paths_only)))
        debug_queue.put(
            str(thread_name) +
            " - Domain Paths: After Diff - Variant Count: " +
            str(len(set_variant_domain_paths_only)))

    # sld --> subdomain, tld_url, fqdn, domain with path
    control_sld_to_domain_path = dict() 
    # only consider ones that have non-www subdomains
    for domain_path in set_control_domain_paths_only:
        tld_url = extract_tld(domain_path)
        if tld_url.subdomain == "www":
            continue
        sld_url = get_second_level_domain_from_tld(tld_url)
        domain = get_domain_only_from_tld(tld_url)
        if sld_url not in control_sld_to_domain_path:
            control_sld_to_domain_path[sld_url] = []
        control_sld_to_domain_path[sld_url].append(
            (tld_url.subdomain, tld_url, domain, domain_path))

    if debug_queue and output_external_logs:
        debug_queue.put(
            str(thread_name) + " - Control SLD to domain path: " +
            str(control_sld_to_domain_path))

    # find domains from variant set that that still appear in control_sld_to_domain_path
    # sld --> subdomain, tld_url, fqdn, domain with path
    match_variant_sld_to_domain_path = dict()  
    for domain_path in set_variant_domain_paths_only:
        tld_url = extract_tld(domain_path)
        if tld_url.subdomain == "www":
            continue
        sld_url = get_second_level_domain_from_tld(tld_url)
        domain = get_domain_only_from_tld(tld_url)
        if sld_url in control_sld_to_domain_path:
            if sld_url not in match_variant_sld_to_domain_path:
                match_variant_sld_to_domain_path[sld_url] = []
            match_variant_sld_to_domain_path[sld_url].append(
                (tld_url.subdomain, tld_url, domain, domain_path))
    if debug_queue and output_external_logs:
        debug_queue.put(
            str(thread_name) + " - Variant Matching SLD to domain path: " +
            str(match_variant_sld_to_domain_path))

    threshold_match = 0.7
    # sld -> subdomain -> CONTROL|VARIANT -> tuple list (subdomain, tld, domain, domain_path)
    sld_group = dict()  
    # group together with highest match (within variant first)
    for sld_url in match_variant_sld_to_domain_path:
        variant_items = match_variant_sld_to_domain_path.get(sld_url)
        for variant_subdomain, variant_tld, variant_domain, variant_domain_path in variant_items:
            if sld_url not in sld_group:
                sld_group[sld_url] = dict()

            highest_match_subdomain = None
            highest_match_threshold = threshold_match

            for subdomain in sld_group[sld_url]:
                match_ratio = textdistance.levenshtein.normalized_similarity(subdomain, variant_subdomain)
                if match_ratio >= highest_match_threshold:
                    highest_match_threshold = match_ratio
                    highest_match_subdomain = subdomain

            if highest_match_subdomain is not None:
                if debug_queue and output_external_logs:
                    debug_queue.put(
                        str(thread_name) +
                        " - Variant Matching highest threshold: " +
                        str(highest_match_threshold) + ", subdomain: " +
                        str(highest_match_subdomain) + ", orig subdomain " +
                        str(variant_subdomain))

                sld_group[sld_url][highest_match_subdomain][VARIANT].append(
                    (variant_subdomain, variant_tld, variant_domain,
                     variant_domain_path))
            else:
                if variant_subdomain not in sld_group[sld_url]:
                    sld_group[sld_url][variant_subdomain] = dict()
                if VARIANT not in sld_group[sld_url][variant_subdomain]:
                    sld_group[sld_url][variant_subdomain][VARIANT] = []
                    sld_group[sld_url][variant_subdomain][VARIANT].append(
                        (variant_subdomain, variant_tld, variant_domain,
                         variant_domain_path))

    # now add matching subdomains from control
    for sld_url in control_sld_to_domain_path:
        control_items = control_sld_to_domain_path.get(sld_url)
        for control_subdomain, control_tld, control_domain, control_domain_path in control_items:
            if sld_url in sld_group:
                highest_match_subdomain = None
                highest_match_threshold = threshold_match

                for subdomain in sld_group[sld_url]:
                    match_ratio = textdistance.levenshtein.normalized_similarity(subdomain, control_subdomain)
                    if match_ratio >= highest_match_threshold:
                        highest_match_threshold = match_ratio
                        highest_match_subdomain = subdomain

                if highest_match_subdomain is not None:
                    if debug_queue and output_external_logs:
                        debug_queue.put(
                            str(thread_name) +
                            " - Control Matching highest threshold: " +
                            str(highest_match_threshold) + ", subdomain: " +
                            str(highest_match_subdomain) +
                            ", orig subdomain " + str(control_subdomain))
                    if CONTROL not in sld_group[sld_url][
                            highest_match_subdomain]:
                        sld_group[sld_url][highest_match_subdomain][
                            CONTROL] = []
                    sld_group[sld_url][highest_match_subdomain][
                        CONTROL].append((control_subdomain, control_tld,
                                         control_domain, control_domain_path))

    if debug_queue and output_external_logs:
        debug_queue.put(str(thread_name) + " - SLD GROUP: " + str(sld_group))

    # treat the domain as one group instead of individual subdomains.
    # Then get the intersection between control and variant.
    control_paths = []
    variant_paths = []
    # flatten into paths
    for sld_url in sld_group:
        for subdomain in sld_group[sld_url]:
            for control_or_variant in sld_group[sld_url][subdomain]:
                for _, _, _, domain_path in sld_group[sld_url][subdomain][
                        control_or_variant]:
                    path, _ = get_path_and_query_params(domain_path)
                    if control_or_variant == CONTROL:
                        control_paths.append(path)
                    else:
                        variant_paths.append(path)

    control_paths = set(control_paths)
    variant_paths = set(variant_paths)

    # get the intersection of the paths
    intersection_paths = variant_paths.intersection(control_paths)
    if debug_queue and output_external_logs:
        debug_queue.put(
            str(thread_name) + " - Intersected Paths: " +
            str(intersection_paths))
        debug_queue.put(
            str(thread_name) + " - Domain Paths: Control Before: " +
            str(len(set_control_domain_paths_only)))
        debug_queue.put(
            str(thread_name) + " - Domain Paths: Variant Before: " +
            str(len(set_variant_domain_paths_only)))

    # remove the domain_paths that have the same paths (from the intersection paths)
    for intersected_path in intersection_paths:
        for domain_path in list(set_control_domain_paths_only):
            if intersected_path in domain_path:
                set_control_domain_paths_only.remove(domain_path)

        for domain_path in list(set_variant_domain_paths_only):
            if intersected_path in domain_path:
                set_variant_domain_paths_only.remove(domain_path)
    if debug_queue and output_external_logs:
        debug_queue.put(
            str(thread_name) + " - Domain Paths: Control After: " +
            str(len(set_control_domain_paths_only)))
        debug_queue.put(
            str(thread_name) + " - Domain Paths: Variant After: " +
            str(len(set_variant_domain_paths_only)))

    # translate it back to requests
    set_control_webreqs = []
    for key in set_control_domain_paths_only:
        set_control_webreqs += control_domain_path_to_requests.get(key)
    set_control_webreqs = set(set_control_webreqs)

    set_variant_webreqs = []
    for key in set_variant_domain_paths_only:
        set_variant_webreqs += variant_domain_path_to_requests.get(key)
    set_variant_webreqs = set(set_variant_webreqs)

    return set_control_webreqs, set_variant_webreqs


def get_wr_differences_only(diff_group,
                            crawler_group_name,
                            crawl_collection,
                            debug_queue=None,
                            urls_collector_queue=None,
                            thread_name=None,
                            adblock_parser=None,
                            debug_collect_urls=False,
                            output_external_logs=True):

    # create custom structure
    crawl_trial_group = create_trial_group(diff_group, crawl_collection)

    main_url = ""
    for trial_key in crawl_trial_group[CONTROL].keys():
        trial_inst = crawl_trial_group[CONTROL].get(trial_key)
        main_url = trial_inst.get("url")
        if main_url:
            break

    # get necessary request info before diff analysis
    control_req_to_type, control_req_to_resource_type, control_domain_path_to_requests, control_instance_ids, \
    control_instance_ids_str, control_trial_plot, control_trial_plot_diff, control_trial_domains, control_trial_domains_diff,\
    _ = get_wr_trial_aggregate(crawl_trial_group, CONTROL, debug_queue=debug_queue, output_external_logs=output_external_logs, thread_name=thread_name)

    variant_req_to_type, variant_req_to_resource_type, variant_domain_path_to_requests, variant_instance_ids, \
    variant_instance_ids_str, variant_trial_plot, variant_trial_plot_diff, variant_trial_domains, variant_trial_domains_diff, \
    variant_blocked_urls_by_trial = get_wr_trial_aggregate(crawl_trial_group, VARIANT, debug_queue=debug_queue, output_external_logs=output_external_logs, thread_name=thread_name)

    # do diff analysis and return the sets of webrequests that we want to consider
    control_only_webreqs, variant_only_webreqs = get_diff_requests_sets(
        control_domain_path_to_requests,
        variant_domain_path_to_requests,
        debug_queue=debug_queue,
        output_external_logs=output_external_logs,
        thread_name=thread_name)

    # collect requests that we want to parse for tracking later
    if debug_collect_urls and urls_collector_queue:
        for req in variant_only_webreqs:
            urls_collector_queue.put(req)

    def _get_trimmed_path(path, lvl=3, delimiter="/"):
        path_split = path.split(delimiter)
        if len(path_split) > lvl:
            trim_path_split = path_split[:lvl]
            return delimiter.join(trim_path_split)
        return path

    control_path_to_request = dict()
    control_trim_paths = []
    for req in control_only_webreqs:
        domain = get_domain_only_from_url(req)
        # we keep track of potential requests that may be a problem
        path, _ = get_path_and_query_params(req)
        if contains_important_resource(path):
            path = remove_last_path(path)
            if len(path) > 0:
                trim_path = _get_trimmed_path(path)
                if trim_path not in control_trim_paths:
                    control_trim_paths.append(trim_path)
                if (domain, path) not in control_path_to_request:
                    control_path_to_request[(domain, path)] = []
                control_path_to_request[(domain, path)].append(req)

    logger.debug("%s - Control Trimmed paths: %s" %
                 (str(thread_name), str(control_trim_paths)))

    variant_path_to_request = dict()
    variant_trim_paths = []
    for req in variant_only_webreqs:
        domain = get_domain_only_from_url(req)
        # we keep track of potential requests that may be a problem
        path, _ = get_path_and_query_params(req)
        if contains_important_resource(path):
            path = remove_last_path(path)
            trim_path = _get_trimmed_path(path)
            if len(path) > 0:
                if trim_path not in variant_trim_paths:
                    variant_trim_paths.append(trim_path)
                if (domain, path) not in variant_path_to_request:
                    variant_path_to_request[(domain, path)] = []
                variant_path_to_request[(domain, path)].append(req)

    logger.debug("%s - Variant Trimmed paths: %s" %
                 (str(thread_name), str(variant_trim_paths)))

    path_resource_mismatch_found = []
    for key in control_path_to_request.keys():
        if key in variant_path_to_request:
            if len(control_path_to_request.get(key)) != len(
                    variant_path_to_request.get(key)):
                path_resource_mismatch_found.append(key)

    control_path_resource_mismatch = []
    variant_path_resource_mismatch = []
    for key in path_resource_mismatch_found:
        control_path_resource_mismatch += control_path_to_request.get(key)
        variant_path_resource_mismatch += variant_path_to_request.get(key)

    # filter down the types that we only care about
    control_req_to_type_final = dict()
    control_req_to_resource_type_final = dict()

    # here we find only
    for req_item in control_req_to_type:
        #if req_item in control_path_resource_mismatch.get("mismatch") or req_item in control_only_webreqs:
        if req_item in control_path_resource_mismatch or req_item in control_only_webreqs:

            ctr_content_type = control_req_to_type.get(req_item)
            if ctr_content_type is not None:
                trial_instance, req_obj, content_type_tmp = control_req_to_type.get(
                    req_item)
                control_req_to_type_final[
                    req_item] = trial_instance, req_obj, content_type_tmp  #content types

            ctr_resource_type = control_req_to_resource_type.get(req_item)
            if ctr_resource_type is not None:
                trial_instance = ctr_resource_type.get("trial")
                req_obj = ctr_resource_type.get("request")
                resource_type_tmp = ctr_resource_type.get("data")
                control_req_to_resource_type_final[
                    req_item] = trial_instance, req_obj, resource_type_tmp  # misc types

    # filter down the types that we only care about
    variant_req_to_type_final = dict()
    variant_req_to_resource_type_final = dict()

    for req_item in variant_req_to_type:
        #if req_item in variant_path_resource_mismatch.get("mismatch") or req_item in variant_only_webreqs:
        if req_item in variant_path_resource_mismatch or req_item in variant_only_webreqs:

            var_content_type = variant_req_to_type.get(req_item)
            if var_content_type is not None:
                trial_instance, req_obj, content_type_tmp = variant_req_to_type.get(
                    req_item)
                variant_req_to_type_final[
                    req_item] = trial_instance, req_obj, content_type_tmp  #content types
            var_resource_type = variant_req_to_resource_type.get(req_item)
            if var_resource_type is not None:
                trial_instance = var_resource_type.get("trial")
                req_obj = var_resource_type.get("request")
                resource_type_tmp = var_resource_type.get("data")
                variant_req_to_resource_type_final[
                    req_item] = trial_instance, req_obj, resource_type_tmp  # misc types

    # flatten the dict for trials related urls and domains
    sorted_keys = sorted(control_trial_plot.keys())
    control_trial_plot_flatten = []
    for key in sorted_keys:
        control_trial_plot_flatten.append(len(control_trial_plot.get(key)))

    sorted_keys = sorted(control_trial_domains.keys())
    control_trial_domains_flatten = []
    for key in sorted_keys:
        control_trial_domains_flatten.append(
            len(control_trial_domains.get(key)))

    sorted_keys = sorted(variant_trial_plot.keys())
    variant_trial_plot_flatten = []
    for key in sorted_keys:
        variant_trial_plot_flatten.append(len(variant_trial_plot.get(key)))

    sorted_keys = sorted(variant_trial_domains.keys())
    variant_trial_domains_flatten = []
    for key in sorted_keys:
        variant_trial_domains_flatten.append(
            len(variant_trial_domains.get(key)))

    control_only_diff = {
        "crawl_group_name": crawler_group_name,
        "crawl_instance_ids_str": ",".join(control_instance_ids_str),
        "crawl_instance_ids": control_instance_ids,
        "mismatch_resources": control_path_resource_mismatch,
        "urls": list(control_only_webreqs),
        "content_types": control_req_to_type_final,
        "misc_types": control_req_to_resource_type_final,
        "trials_urls_cumulative": control_trial_plot_flatten,
        "trials_domains_cumulative": control_trial_domains_flatten,
        "trials_urls_diff": control_trial_plot_diff,
        "trials_domain_diff": control_trial_domains_diff
    }

    variant_only_diff = {
        "crawl_group_name": crawler_group_name,
        "crawl_instance_ids_str": ",".join(variant_instance_ids_str),
        "crawl_instance_ids": variant_instance_ids,
        "mismatch_resources": variant_path_resource_mismatch,
        "urls": list(variant_only_webreqs),
        "content_types": variant_req_to_type_final,
        "misc_types": variant_req_to_resource_type_final,
        "trials_urls_cumulative": variant_trial_plot_flatten,
        "trials_domains_cumulative": variant_trial_domains_flatten,
        "trials_urls_diff": variant_trial_plot_diff,
        "trials_domain_diff": variant_trial_domains_diff,
        "variant_blocked_urls_by_trial": variant_blocked_urls_by_trial
    }

    logger.debug("Done WR finding diffs for diff group %s" % main_url)

    return control_only_diff, variant_only_diff
