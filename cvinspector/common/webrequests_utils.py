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
import re
from urllib.parse import urlparse

import numpy as np
import tldextract
from bson.objectid import ObjectId

from cvinspector.common.utils import _get_common_stats_default, _get_common_stats_for_number_list, get_entropy

logger = logging.getLogger(__name__)
#logger.setLevel("DEBUG")


def extract_tld(url):
    return tldextract.extract(url)


def get_second_level_domain_from_tld(url_tld):
    return url_tld.domain + "." + url_tld.suffix


def get_domain_only_from_tld(url_tld, remove_www=True):
    sld = get_second_level_domain_from_tld(url_tld)
    if url_tld.subdomain and len(url_tld.subdomain) > 0:
        sld = url_tld.subdomain + "." + sld

    if remove_www:
        sld = sld.replace("www.", "")

    return sld


def get_domain_only_from_url(url):
    tld = extract_tld(url)
    domain = get_domain_only_from_tld(tld)
    return domain


def get_keys_from_query(url):
    keys = []
    _, query_params = get_path_and_query_params(url)

    if query_params:
        # split into keys
        query_params = query_params.replace("?", "", 1)
        if "&amp;" in query_params:
            split_params = query_params.split("&amp;")
        elif ";" in query_params:
            split_params = query_params.split(";")
        else:
            split_params = query_params.split("&")

        for pair in split_params:
            pair_split = pair.split("=")
            if len(pair_split) > 0 and pair_split[0]:
                keys.append(pair_split[0])

    return keys


def get_url_without_query(url):
    remaining_url = url
    if "?" in url:
        # split at first occurence of "?"
        url_split = url.split("?", 1)
        if len(url_split) == 2:
            remaining_url = url_split[0]
    # remove hash as well
    if "#" in remaining_url:
        url_split = remaining_url.split("#", 1)
        if len(url_split) == 2:
            remaining_url = url_split[0]

    return remaining_url


def get_values_from_query(url):
    values = []
    _, query_params = get_path_and_query_params(url)

    if query_params:
        # split into keys
        query_params = query_params.replace("?", "", 1)
        if "&amp;" in query_params:
            split_params = query_params.split("&amp;")
        elif ";" in query_params:
            split_params = query_params.split(";")
        else:
            split_params = query_params.split("&")
        for pair in split_params:
            pair_split = pair.split("=")
            if len(pair_split) > 1 and pair_split[1]:
                values.append(pair_split[1])

    return values


def remove_last_path(path):
    if path == "/":
        return path
    path_split = path.split("/")
    if len(path_split) > 1:
        # get rid of empty strings
        while len(path_split[-1]) == 0 and len(path_split) > 1:
            path_split = path_split[0:-1]

        if len(path_split) > 1 and len(path_split[-1]) > 0:
            path_split = path_split[0:-1]
    return "/".join(path_split)


def get_path_and_query_params(url):
    parsed_url = urlparse(url)
    return parsed_url.path, parsed_url.query


def is_first_party_webrequest(webreq, crawled_url_domain_only):
    webreq_url_tld = extract_tld(webreq)
    webreq_url_sld = get_second_level_domain_from_tld(webreq_url_tld)
    return crawled_url_domain_only in webreq_url_sld or crawled_url_domain_only in webreq_url_tld.subdomain


def split_info_first_and_third_party_requests(webrequests, crawled_url):
    crawled_url_tld = extract_tld(crawled_url)
    crawled_url_domain = crawled_url_tld.domain

    first_party_requests = []
    third_party_requests = []
    for webreq in webrequests:
        webreq_url_tld = extract_tld(webreq)
        if is_first_party_webrequest(webreq, crawled_url_domain):
            first_party_requests.append((webreq, webreq_url_tld))
        else:
            third_party_requests.append((webreq, webreq_url_tld))

    return first_party_requests, third_party_requests


# splits the webrequests into first and third party lists
# returns tld extracted as well
def find_all_first_and_third_party_webrequests(crawled_url,
                                               webrequests,
                                               db=None,
                                               collection_name=None):
    collection = None
    if db and collection_name:
        collection = db[collection_name]

    crawled_url_tld = extract_tld(crawled_url)

    # get second level domain party without the top lvl domain.
    # example: if google.com, then retrieve google
    crawled_url_sld = crawled_url_tld.domain

    first_party_requests = []
    third_party_requests = []
    for webreq_id in webrequests:
        if collection:
            webreq = get_url_from_instance_id(webreq_id, collection)
        else:
            webreq = webreq_id

        webreq_url_tld = extract_tld(webreq)
        webreq_url_sld = get_second_level_domain_from_tld(webreq_url_tld)

        # this logic should deal with first party urls that have slightly different domains
        # example: if domain = chowhound:
        #   - chowhound1.cbsistatic.com --> first party
        #   - chowhound1.com --> first party
        if crawled_url_sld in webreq_url_sld or crawled_url_sld in webreq_url_tld.subdomain:
            first_party_requests.append((webreq, webreq_url_tld))
        else:
            third_party_requests.append((webreq, webreq_url_tld))

    return crawled_url_tld, first_party_requests, third_party_requests


# a list of extracted tlds and returns True if it finds one larger than n
# also checks whether it has a number
def has_subdomain_larger_than_n(url_tlds, n=5, check_digits=True):
    for tld in url_tlds:
        if tld.subdomain and len(tld.subdomain) >= n:
            if check_digits:
                numbers_found = re.findall('[0-9]', tld.subdomain)
                if len(numbers_found):
                    return True
            else:
                return True
    return False


def has_short_special_character_path(urls):
    for url in urls:
        path, _ = get_path_and_query_params(url)
        split_paths = path.split("/")
        # for every part of the path, check if it contains a short 1 character that is not a word
        for tmp_path in split_paths:
            if tmp_path and len(tmp_path) == 1:
                # this should match things like "/?/"
                result = re.findall(r'\W', tmp_path)
                if result and len(result) > 0:
                    return True
    return False


# figures out if a subdomain appears in path
# min_length confines the common values between domain and path to a certain length
def has_subdomain_as_path(url, url_tld=None, min_length=0):
    if url_tld is None:
        url_tld = extract_tld(url)
    path, _ = get_path_and_query_params(url)

    subdomain = url_tld.subdomain
    split_subdomains = subdomain.split(".")
    split_subdomains = [x for x in split_subdomains if x and len(x) > 0]
    split_paths = path.split("/")
    split_paths = [x for x in split_paths if x and len(x) > 0]

    # get intersection by using "set & set"
    common_values = set(split_paths) & set(split_subdomains)
    if min_length > 0:
        for val in common_values:
            if val and len(val) >= min_length:
                return True
    else:
        # if no restriction, then just see if there are common_values
        return len(list(common_values)) > 0

    return False


def get_subdomain_stats(url_tlds, split_subdomain=True):
    subdomain_lengths = []
    for tld in url_tlds:
        if tld.subdomain:
            if split_subdomain:
                subdomain_split = tld.subdomain.split(".")
                subdomain_split = [
                    len(x) for x in subdomain_split
                    if len(x.strip()) > 0 and x != "www"
                ]
                subdomain_lengths += subdomain_split
            else:
                subdomain_lengths.append(len(tld.subdomain))

    return _get_common_stats_for_number_list(subdomain_lengths)


def get_subdomain_entropy_stats(url_tlds):
    subdomains_list = []
    for tld in url_tlds:
        if tld.subdomain:
            subdomains_list.append(tld.subdomain)

    subdomain_entropy_list = []
    for subdomain in subdomains_list:
        if len(subdomain) > 0:
            entropy = get_entropy(subdomain)
            subdomain_entropy_list.append(entropy)

    if len(subdomain_entropy_list) > 0:
        return entropy, subdomains_list
    return 0, subdomains_list


def get_subdomain_entropy_from_set(url_tlds):

    avg_entropy, subdomains_list = get_subdomain_entropy_stats(url_tlds)
    return avg_entropy


def get_path_and_query_stats(urls):

    path_length_stats = get_path_len_stats(urls)
    query_length_stats = get_query_key_len_stats(urls)
    path_entropy, _ = get_path_entropy_stats(urls, split_path=True)
    query_entropy, _ = get_query_key_entropy_stats(urls)
    query_val_entropy, _ = get_query_value_entropy_stats(urls)
    return path_length_stats, query_length_stats, path_entropy, query_entropy, query_val_entropy


# cache_control_map is a dictionary of cache_control_type --> [list of urls]
def get_path_and_query_stats_with_cache_control(cache_control_map):
    path_and_query_with_cache = dict()
    for key in cache_control_map:
        urls = cache_control_map.get(key)
        path_length_stats = get_path_len_stats(urls)
        query_length_stats = get_query_key_len_stats(urls)
        path_entropy, _ = get_path_entropy_stats(urls, split_path=True)
        query_entropy, _ = get_query_key_entropy_stats(urls)
        query_val_entropy, _ = get_query_value_entropy_stats(urls)

        path_and_query_with_cache[
            key] = path_length_stats, query_length_stats, path_entropy, query_entropy, query_val_entropy
    return path_and_query_with_cache


# misc_resources is a dictionary of url -> {"resource_type":"", "cache_control":""}
# requests defines which ones we care about
def build_cache_control_mapping(requests, misc_resources):
    cache_mapping = dict()
    cache_keys = [
        "no-cache", "no-store", "must-revalidate", "public", "private",
        "max-age", "proxy-revalidate", "none"
    ]
    #init
    for key in cache_keys:
        cache_mapping[key] = []

    for webreq_url in misc_resources:
        url = webreq_url
        if url in requests:
            _, _, misc_values = misc_resources.get(webreq_url)
            if "cache_control" in misc_values:
                cache_control = misc_values.get("cache_control")
                if cache_control:
                    # for every cache key we care about
                    for key in cache_keys:
                        # does cache control value contain that cache key? If it does, save the url to correspond with that key
                        if key in cache_control and url not in cache_mapping[
                                key]:
                            cache_mapping[key].append(url)

    # currently URL list is a set (we may consider using non-sets)
    return cache_mapping


# 10800 = 3hrs, 18000 = 5hrs
# 40days = 40*86400
# 2KB = 1024B * 2 = 2048
def filter_requests_by_header(requests,
                              misc_resources,
                              content_types_dict,
                              crawl_url,
                              content_length_min=100,
                              content_length_image_min=2048,
                              cache_control_max_age=3456000,
                              log_prefix=""):

    UNKNOWN_REQ_CONTENT_LENGTH = -1
    remaining_requests = []
    MAX_AGE = "max-age"

    crawl_url_tld = extract_tld(crawl_url)

    for webreq_url in misc_resources:
        req = webreq_url
        _, _, header_items = misc_resources.get(webreq_url)
        if req not in requests:
            ## NOTE: here we do not include the req if it was not originally part of the list
            continue

        is_first_party = is_first_party_webrequest(req, crawl_url_tld.domain)
        if is_first_party:
            remaining_requests.append(req)
            logger.debug(
                "%s - Warning: add resource for crawl url %s , first party: %s",
                str(log_prefix), crawl_url, req)
            continue

        satisfied_max_age = False
        satisfied_content_len = False
        _, _, content_type = content_types_dict.get(webreq_url)
        content_type = content_type.lower()
        cache_control = header_items.get("cache_control") or None
        if cache_control is not None and MAX_AGE in cache_control:
            cache_control_split = re.split(',|:', cache_control)
            for cache_item in cache_control_split:
                if MAX_AGE in cache_item:
                    max_age_time_str = cache_item.replace(MAX_AGE + "=", "")
                    try:
                        max_age_time = int(max_age_time_str)
                        if max_age_time < cache_control_max_age:
                            satisfied_max_age = True
                    except:
                        logger.debug(
                            str(log_prefix) + " " +
                            "Warning: could not change max age into int:" +
                            max_age_time_str)
        else:
            satisfied_max_age = True

        content_length = header_items.get("content_length")
        if content_length is None:
            content_length = UNKNOWN_REQ_CONTENT_LENGTH
        if content_length != UNKNOWN_REQ_CONTENT_LENGTH:
            if content_type and "image" in content_type:
                if content_length > content_length_image_min:
                    satisfied_content_len = True
            elif content_length > content_length_min:
                satisfied_content_len = True
        else:
            satisfied_content_len = True

        # does it satisfy both conditions?
        if satisfied_content_len and satisfied_max_age:
            logger.debug(
                "%s - %s Found requests that satisfied content length and max age requirements: %s"
                % (str(log_prefix), crawl_url, req))
            remaining_requests.append(req)

    return remaining_requests


def get_query_key_entropy_stats(urls):
    query_keys = []
    for url in urls:
        keys = get_keys_from_query(url)
        query_keys += keys

    query_keys_entropy_list = []
    for query_key in query_keys:
        if len(query_key) > 0:
            entropy = get_entropy(query_key)
            query_keys_entropy_list.append(entropy)

    if len(query_keys_entropy_list) > 0:
        avg_entropy = round(np.average(query_keys_entropy_list), 2)
        return avg_entropy, query_keys

    return 0, query_keys


def get_query_value_entropy_stats(urls):
    query_values = []
    for url in urls:
        values = get_values_from_query(url)
        query_values += values

    query_values_entropy_list = []
    for query_value in query_values:
        if len(query_value) > 0:
            entropy = get_entropy(query_value)
            query_values_entropy_list.append(entropy)

    if len(query_values_entropy_list) > 0:
        avg_entropy = round(np.average(query_values_entropy_list), 2)
        return avg_entropy, query_values

    return 0, query_values


def get_query_key_len_stats(urls):
    query_keys = []
    for url in urls:
        keys = get_keys_from_query(url)
        keys = [len(x) for x in keys]
        query_keys += keys

    if len(query_keys) > 0:
        return _get_common_stats_for_number_list(query_keys)

    return _get_common_stats_default()


def get_path_entropy_stats(urls, split_path=True):

    paths = []
    for url in urls:
        path, _ = get_path_and_query_params(url)

        if path:
            if split_path:
                path_subsection = path.split("/")
                paths += path_subsection
            else:
                paths.append(path)

    path_entropy_list = []
    for path in paths:
        if len(path) > 0:
            path_entropy = get_entropy(path)
            path_entropy_list.append(path_entropy)

    if len(path_entropy_list) > 0:
        avg_path_entropy = round(np.average(path_entropy_list), 2)
        return avg_path_entropy, paths
    return 0, paths


def get_path_len_stats(urls, split_path=True):

    paths = []
    for url in urls:
        path, _ = get_path_and_query_params(url)

        if path:
            if split_path:
                path_subsection = path.split("/")
                path_subsection = [
                    len(x) for x in path_subsection if len(x.strip()) > 0
                ]
                paths += path_subsection
            else:
                paths.append(len(path))

    if len(paths) > 0:
        return _get_common_stats_for_number_list(paths)
    return _get_common_stats_default()


OBJID_TO_URL = dict()


def get_url_from_instance_id(objId, collection):

    if objId in OBJID_TO_URL:
        return OBJID_TO_URL.get(objId)
    try:
        instance = collection.find_one({"_id": ObjectId(objId)},
                                       {"event.url": 1})
        if instance is not None:
            url = instance["event"]["url"]
            OBJID_TO_URL[objId] = url
            return url
    except:
        pass

    return objId


# content_type_resources is a dictionary of url -> trial_instance, webrequest_data, content type
# the keys of content_type_resources must match exactly what is given in the requests list in order to do correct calculation
# this compares actual path with content-type value from header
def get_content_type_mismatch(requests, content_type_resources,
                              media_extensions):
    counts = {
        "unknown": 0,
        "json": 0,
        "js": 0,
        "media": 0,
        "html": 0,
        "css": 0
    }
    for webreq_url in content_type_resources:
        url = webreq_url
        trial_instance, web_req, content_type = content_type_resources.get(
            webreq_url)
        content_type = content_type.lower()
        if url in requests:
            # content type is optional and treat octet as unknown
            if content_type == "unknown" or "octet" in content_type:
                counts["unknown"] += 1
                continue
            path, _ = get_path_and_query_params(url)
            if path.lower().endswith(".js") and (
                    "javascript" not in content_type and "html"
                    not in content_type and "xml" not in content_type):
                counts["js"] += 1
            else:
                for media_key in media_extensions:
                    if path.lower().endswith(
                            media_key) and "octet" not in content_type and (
                                "image" not in content_type
                                and "font" not in content_type):
                        counts["media"] += 1
            if path.lower().endswith("html") and "html" not in content_type:
                counts["html"] += 1
            if path.lower().endswith("css") and "css" not in content_type:
                counts["css"] += 1
            if path.lower().endswith("json") and \
                ("json" not in content_type and "javascript" not in content_type) :
                counts["json"] += 1

    return counts


# this only consideres content type without matching without path
def get_content_type_mapping(requests, content_type_resources):
    mapping = {
        "unknown": [],
        "octet": [],
        "json": [],
        "js": [],
        "media": [],
        "html": [],
        "css": [],
        "xml": [],
        "other": [],
        "font": []
    }
    for webreq_url in content_type_resources:
        url = webreq_url
        trial_instance, web_req, content_type = content_type_resources.get(
            webreq_url)
        content_type = content_type.lower()
        if url in requests:
            # content type is optional and treat octet as unknown
            if content_type == "unknown" or "octet" in content_type:
                mapping["unknown"].append(url)
            elif "javascript" in content_type:
                mapping["js"].append(url)
            elif "html" in content_type:
                mapping["html"].append(url)
            elif "xml" in content_type:
                mapping["xml"].append(url)
            elif "css" in content_type:
                mapping["css"].append(url)
            elif "json" in content_type:
                mapping["json"].append(url)
            elif "image" in content_type:
                mapping["media"].append(url)
            elif "font" in content_type:
                mapping["font"].append(url)
            else:
                mapping["other"].append(url)
    return mapping


def get_webrequest_detail_value(webrequest, detail_name):
    import json
    if webrequest and webrequest.get("event"):
        req_details = webrequest.get("event").get("details")
        if req_details:
            try:
                req_details_json = json.loads(req_details)
                if req_details_json.get("statusCode") != 200:
                    return None
                if detail_name in req_details_json:
                    return req_details_json.get(detail_name)
            except Exception:
                logger.debug("Could not get detail value %s : %s" %
                             (webrequest, detail_name))
    return None
