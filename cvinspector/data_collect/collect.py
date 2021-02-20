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

import datetime
import datetime
import logging
import os
import random
import shutil
import time
from urllib.parse import urlparse

import tldextract
from pyvirtualdisplay import Display

from cvinspector.data_collect.chrome import get_scroll_width_and_height

logger = logging.getLogger(__name__)

PROFILE_TOP_LEVEL_PATH = "./chromeprofiles" + os.sep
PROFILE_TOP_LEVEL_PATH_MONITORING = "./chromeprofiles_monitoring" + os.sep

###########################
# DATA COLLECTION ONLY (frozen list that is provided by the setup)

# anticv list OFF (control)
PROFILE_STARTING_POINT__CONTROL = PROFILE_TOP_LEVEL_PATH + "default_profile_anticv_off__control" + os.sep
PROFILE_TEMP_PATH__CONTROL = PROFILE_TOP_LEVEL_PATH + "tempprofiles_anticv_off__control" + os.sep

# anticv list ON (control)
PROFILE_STARTING_POINT__ANTICV_ON__CONTROL = PROFILE_TOP_LEVEL_PATH + "default_profile_anticv_on__control" + os.sep
PROFILE_TEMP_PATH__ANTICV_ON__CONTROL = PROFILE_TOP_LEVEL_PATH + "tempprofiles_anticv_on__control" + os.sep

# anticv list OFF (variant)
PROFILE_STARTING_POINT = PROFILE_TOP_LEVEL_PATH + "default_profile_anticv_off" + os.sep
PROFILE_TEMP_PATH = PROFILE_TOP_LEVEL_PATH + "tempprofiles_anticv_off" + os.sep

# anticv list ON (variant)
PROFILE_STARTING_POINT__ANTICV_ON = PROFILE_TOP_LEVEL_PATH + "default_profile_anticv_on" + os.sep
PROFILE_TEMP_PATH__ANTICV_ON = PROFILE_TOP_LEVEL_PATH + "tempprofiles_anticv_on" + os.sep

###########################

# MONITORING (uses newest rules from both EasyList and Anti-CV list)

# anticv list OFF (control)
PROFILE_STARTING_POINT_MONITORING__CONTROL = PROFILE_TOP_LEVEL_PATH_MONITORING + "default_profile_anticv_off__control" + os.sep
PROFILE_TEMP_PATH_MONITORING__CONTROL = PROFILE_TOP_LEVEL_PATH_MONITORING + "tempprofiles_anticv_off__control" + os.sep

# anticv list ON (control)
PROFILE_STARTING_POINT_MONITORING__ANTICV_ON__CONTROL = PROFILE_TOP_LEVEL_PATH_MONITORING + "default_profile_anticv_on__control" + os.sep
PROFILE_TEMP_PATH_MONITORING__ANTICV_ON__CONTROL = PROFILE_TOP_LEVEL_PATH_MONITORING + "tempprofiles_anticv_on__control" + os.sep

# anticv list OFF (variant)
PROFILE_STARTING_POINT_MONITORING = PROFILE_TOP_LEVEL_PATH_MONITORING + "default_profile_anticv_off" + os.sep
PROFILE_TEMP_PATH_MONITORING = PROFILE_TOP_LEVEL_PATH_MONITORING + "tempprofiles_anticv_off" + os.sep

# anticv list ON (variant)
PROFILE_STARTING_POINT_MONITORING__ANTICV_ON = PROFILE_TOP_LEVEL_PATH_MONITORING + "default_profile_anticv_on" + os.sep
PROFILE_TEMP_PATH_MONITORING__ANTICV_ON = PROFILE_TOP_LEVEL_PATH_MONITORING + "tempprofiles_anticv_on" + os.sep

###########################

PAGE_SOURCE_TOP_LEVEL_PATH = "./pagesource" + os.sep
SCREENSHOT_TOP_LEVEL_PATH = "./screenshot" + os.sep

MEASUREMENT_TIMER = 25  # in seconds
EXCEPTION_SUFFIX = ".exception"  # used for exceptions
SIMPLE_SUFFIX = ".simple"  # used to map domain name with random string to real domain

BLANK_CHROME_PAGE = "about:blank"


def get_downloads_directory(output_directory, crawler_group_name):
    return output_directory + os.sep + "downloads_" + crawler_group_name + os.sep


def get_pagesource_directory(output_directory, crawler_group_name):
    return output_directory + os.sep + "pagesource_" + crawler_group_name + os.sep


def get_page_source_filename(domain,
                             driver_name,
                             output_directory=PAGE_SOURCE_TOP_LEVEL_PATH):
    file_name = domain.replace("/", "_").replace(
        ":", "_") + '__' + driver_name + '__pagesource.html'
    return output_directory + file_name


def get_screenshot_directory(output_directory, crawler_group_name):
    return output_directory + os.sep + "screenshot_" + crawler_group_name + os.sep


def get_screenshot_filename(domain,
                            driver_name,
                            output_directory=SCREENSHOT_TOP_LEVEL_PATH,
                            with_directory=False):
    file_name = domain.replace("/", "_").replace(
        ":", "_") + '__' + driver_name + '__screenshot.png'
    if with_directory:
        return output_directory + file_name
    return file_name


def create_profile_directories():
    # variant
    if not os.path.isdir(PROFILE_STARTING_POINT):
        os.makedirs(PROFILE_STARTING_POINT)
    if not os.path.isdir(PROFILE_TEMP_PATH):
        os.makedirs(PROFILE_TEMP_PATH)
    if not os.path.isdir(PROFILE_STARTING_POINT__ANTICV_ON):
        os.makedirs(PROFILE_STARTING_POINT__ANTICV_ON)
    if not os.path.isdir(PROFILE_TEMP_PATH__ANTICV_ON):
        os.makedirs(PROFILE_TEMP_PATH__ANTICV_ON)

    # control
    if not os.path.isdir(PROFILE_STARTING_POINT__CONTROL):
        os.makedirs(PROFILE_STARTING_POINT__CONTROL)
    if not os.path.isdir(PROFILE_TEMP_PATH__CONTROL):
        os.makedirs(PROFILE_TEMP_PATH__CONTROL)
    if not os.path.isdir(PROFILE_STARTING_POINT__ANTICV_ON__CONTROL):
        os.makedirs(PROFILE_STARTING_POINT__ANTICV_ON__CONTROL)
    if not os.path.isdir(PROFILE_TEMP_PATH__ANTICV_ON__CONTROL):
        os.makedirs(PROFILE_TEMP_PATH__ANTICV_ON__CONTROL)


def create_profile_directories_monitoring():
    # variant
    if not os.path.isdir(PROFILE_STARTING_POINT_MONITORING):
        os.makedirs(PROFILE_STARTING_POINT_MONITORING)
    if not os.path.isdir(PROFILE_TEMP_PATH_MONITORING):
        os.makedirs(PROFILE_TEMP_PATH_MONITORING)
    if not os.path.isdir(PROFILE_STARTING_POINT_MONITORING__ANTICV_ON):
        os.makedirs(PROFILE_STARTING_POINT_MONITORING__ANTICV_ON)
    if not os.path.isdir(PROFILE_TEMP_PATH_MONITORING__ANTICV_ON):
        os.makedirs(PROFILE_TEMP_PATH_MONITORING__ANTICV_ON)

    # control
    if not os.path.isdir(PROFILE_STARTING_POINT_MONITORING__CONTROL):
        os.makedirs(PROFILE_STARTING_POINT_MONITORING__CONTROL)
    if not os.path.isdir(PROFILE_TEMP_PATH_MONITORING__CONTROL):
        os.makedirs(PROFILE_TEMP_PATH_MONITORING__CONTROL)
    if not os.path.isdir(
            PROFILE_STARTING_POINT_MONITORING__ANTICV_ON__CONTROL):
        os.makedirs(PROFILE_STARTING_POINT_MONITORING__ANTICV_ON__CONTROL)
    if not os.path.isdir(PROFILE_TEMP_PATH_MONITORING__ANTICV_ON__CONTROL):
        os.makedirs(PROFILE_TEMP_PATH_MONITORING__ANTICV_ON__CONTROL)


def create_directories(output_directory, crawler_group_name):
    # first make sure we have the right directory for pagesource saving
    pagesource_dir = get_pagesource_directory(output_directory,
                                              crawler_group_name)
    if not os.path.isdir(pagesource_dir):
        os.makedirs(pagesource_dir)
        logger.debug("Created new Pagesource directory")
    else:
        logger.debug("Pagesource directory already exists")

    logger.debug("NOTE: Page source of crawled pages will be saved in " +
                 pagesource_dir)

    # first make sure we have the right directory for screenshot saving
    screenshot_dir = get_screenshot_directory(output_directory,
                                              crawler_group_name)
    if not os.path.isdir(screenshot_dir):
        os.makedirs(screenshot_dir)
        logger.debug("Created new Screenshot directory")
    else:
        logger.debug("Screenshot directory already exists")

    logger.debug("NOTE: Screenshot of crawled pages will be saved in " +
                 screenshot_dir)

    # first make sure we have the right directory for downloads saving
    downloads_dir = get_downloads_directory(output_directory,
                                            crawler_group_name)
    if not os.path.isdir(downloads_dir):
        os.makedirs(downloads_dir)
        logger.debug("Created new Downloads directory")
    else:
        logger.debug("Downloads directory already exists")

    logger.debug("NOTE: Downloads of crawled pages will be saved in " +
                 downloads_dir)

    return pagesource_dir, screenshot_dir, downloads_dir


# we fake an event so that the extensions know which names to save it as
def trigger_js_event_for_filename(driver, file_name):
    js_str = "var evt = new CustomEvent('AnticvFileNameEvent', {detail:{filename: '" + file_name + "'}}); window.dispatchEvent(evt);"
    driver.execute_script(js_str)


def _get_second_level_domain_from_tld(url_tld):
    return url_tld.domain + "." + url_tld.suffix


def delete_profile(profile_path, thread_name=None):
    if profile_path is not None and os.path.isdir(profile_path):
        logger.debug("%s - Deleting profile path: %s" %
                     (str(thread_name), profile_path))
        try:
            shutil.rmtree(profile_path)
        except Exception:
            logger.warn("%s - Could not delete profile path: %s " %
                        (str(thread_name), profile_path))


def is_first_party(url, original_domain, tld_result_orig=None):
    if tld_result_orig is None:
        tld_result_orig = tldextract.extract(original_domain)

    tld_result_orig_sld = _get_second_level_domain_from_tld(tld_result_orig)

    tld_result_url = tldextract.extract(url)
    tld_result_url_sld = _get_second_level_domain_from_tld(tld_result_url)

    if tld_result_orig_sld == tld_result_url_sld:
        return True, tld_result_url, tld_result_orig

    return False, tld_result_url, tld_result_orig


def is_diff_path(url, original_domain, url_parse_original=None):
    parse_url = urlparse(url)
    if url_parse_original is None:
        temp_domain = original_domain
        if "http" not in temp_domain:
            temp_domain = "http://" + temp_domain
        url_parse_original = urlparse(temp_domain)

    _is_diff_path = parse_url.path != url_parse_original.path
    return is_diff_path, parse_url, url_parse_original


def get_all_links_with_href(driver):
    links = driver.execute_script('return document.links')
    hrefs = []
    for link in links:
        rel = link.get_attribute("rel")
        if rel is None or rel == "":
            href_temp = link.get_attribute("href")
            if href_temp is not None and len(
                    href_temp
            ) > 0 and "javascript" not in href_temp and not href_temp.startswith(
                "#"):
                hrefs.append(href_temp)
    return hrefs


def process_beyond_landing_page(domain, hrefs):
    # disregard ones like "contact", "login", "terms", and no query params
    def _filter_href(potential_href):
        potential_url_parse = urlparse(potential_href)
        path = potential_url_parse.path
        fragment = potential_url_parse.fragment
        should_filter = len(fragment) > 0
        if not should_filter:
            # remove sites that may not have ads and sites that have extensions.
            should_filter = ("contact" in path or "login" in path
                             or "terms" in path or "help" in path
                             or "account" in path or "privacy" in path
                             or "policies" in path or "#" in path
                             or "iframe" in path or "faq" in path
                             or "about" in path or "upload" in path
                             or "download" in path or "subscribe" in path
                             or "mailto" in path or "signup" in path
                             or "." in path)

        if not should_filter:
            if path.strip() == "/":
                should_filter = True

        return should_filter

    potential_crawl_pages = []
    if len(hrefs) > 0:
        tld_result_orig = tldextract.extract(domain)
        tld_result_orig_sld = _get_second_level_domain_from_tld(
            tld_result_orig)
        logger.debug("tld_result_orig %s" % str(tld_result_orig))
        urlparse_orig = None
        for href in hrefs:
            # print("potential link %s" % href)
            is_diff_subdomain = False
            is_diff_path_and_no_query = False
            if not href.startswith("//") and href.startswith("/"):
                # if the href is a first party already, then add in the orginal domain
                original_href = href
                href = tld_result_orig_sld + href
                logger.debug("Updated href from %s to %s " %
                             (original_href, href))

            if not _filter_href(href):
                is_fp, tld_result_href, tld_result_orig = is_first_party(
                    href, domain, tld_result_orig=tld_result_orig)

                is_potential = False
                # look at first party with different subdomains and paths
                if is_fp:
                    # if diff subdomains, then add it to our list
                    # treat empty subdomains as wwww
                    href_subdomain = tld_result_href.subdomain
                    orig_subdomain = tld_result_orig.subdomain
                    if len(href_subdomain) == 0:
                        href_subdomain = "www"
                    if len(orig_subdomain) == 0:
                        orig_subdomain = "www"
                    if href_subdomain != orig_subdomain:
                        is_potential = True
                        is_diff_subdomain = True

                    diff_path, urlparse_href, urlparse_orig = is_diff_path(
                        href, domain, url_parse_original=urlparse_orig)

                    # if diff path and no query
                    if diff_path and len(urlparse_href.query) == 0:
                        is_potential = True
                        is_diff_path_and_no_query = True

                    sub_path_counts__href = 0
                    if urlparse_href.path:
                        sub_path_counts__href = urlparse_href.path.count("/")

                    if is_potential:
                        potential_crawl_pages.append(
                            (href, is_fp, is_diff_subdomain,
                             is_diff_path_and_no_query, sub_path_counts__href))

                if not is_fp and not is_potential:
                    pass

    # sort by len. Here we assume that longer urls have more potential of being a valid url to crawl
    potential_crawl_pages = sorted(potential_crawl_pages,
                                   key=lambda i:
                                   (i[4], 1 if i[2] == True else 0, 1
                                   if i[3] == True else 0, len(i[0])),
                                   reverse=True)

    # return a set of tuples
    potential_hrefs = []
    potential_crawl_pages_set = []
    for tmp in potential_crawl_pages:
        href, is_fp, is_diff_subdomain, is_diff_path_and_no_query, sub_path_counts__href = tmp
        if href in potential_hrefs:
            continue
        potential_hrefs.append(href)
        potential_crawl_pages_set.append(tmp)

    return potential_crawl_pages_set


# using heuristics
def find_beyond_landing_page(driver, domain):
    # this already removes links with "rel" attributes which are used for more ads
    hrefs = get_all_links_with_href(driver)
    return process_beyond_landing_page(domain, hrefs)


def _visit_domain(driver,
                  driver_name,
                  domain,
                  rank,
                  pagesource_directory,
                  thread_name,
                  log_prefix="",
                  check_source_file=True,
                  use_https=True):
    SITE_CANT_BE_REACHED = "this site can"
    should_visit = True
    is_https = True

    if check_source_file and pagesource_directory:
        should_visit = not _check_pagesource(domain, driver_name,
                                             pagesource_directory)

    should_sleep = False

    retry = True
    retry_with_http = False
    tried_with_http = False
    if should_visit:
        while retry:

            if retry_with_http:
                tried_with_http = True

            driver.delete_all_cookies()
            # if one of the drivers need to load a domain, then we need to sleep
            should_sleep = True
            if domain == BLANK_CHROME_PAGE:
                driver.get(BLANK_CHROME_PAGE)
            elif "http" in domain:
                is_https = "https" in domain
                logger.debug("%s - http already in url, trying: %s" %
                             (str(thread_name), domain))
                driver.get(domain)
            elif domain and len(domain) > 0:
                if retry_with_http or not use_https:
                    logger.debug("%s - %s - Retrying url %s with http" %
                                 (str(thread_name), driver_name, domain))
                    url = 'http://' + domain
                    is_https = False
                else:
                    url = 'https://' + domain
                    is_https = True

                logger.debug("%s - Trying url: %s" % (str(thread_name), url))
                driver.get(url)

            # catch timeout
            if "Connection timed out" in driver.title:
                raise Exception("Connection time out")

            logger.debug("%s - %s - %s - Page title: %s" %
                         (str(thread_name), str(datetime.datetime.now()),
                          log_prefix + " " + driver_name, driver.title))

            # see if site cannot be reached
            if not retry_with_http:
                logger.debug("%s - Trying to find first span" %
                             str(thread_name))
                # try to see if we need to retry with http instead
                spans = driver.find_elements_by_tag_name("span")
                if spans and len(spans) > 0:
                    first_span = spans[0]
                    span_text = first_span.text.strip().lower()
                    logger.debug("%s - First Span Text %s" %
                                 (str(thread_name), span_text))

                    if SITE_CANT_BE_REACHED in span_text:
                        logger.debug("%s - Setting retry_with_http to True" %
                                     str(thread_name))
                        retry_with_http = True
                else:
                    logger.debug("%s - No spans found" % str(thread_name))

            # if we still don't need to retry
            if not retry_with_http or tried_with_http:
                # we are done
                retry = False
                retry_with_http = False

    else:
        logger.debug("%s - Skipping url %s with driver %s  " %
                     (str(thread_name), domain, driver_name))

    return should_sleep, is_https


def _scroll_page(scrollto_height,
                 driver,
                 driver_name,
                 domain,
                 thread_name,
                 log_prefix=""):
    js_down = 'scrollTo(0,%s)' % str(scrollto_height)
    driver.execute_script(js_down)
    time.sleep(1)
    js_up = 'scrollTo(0,0)'
    driver.execute_script(js_up)
    time.sleep(1)
    logger.debug("%s - %s - %s - Done simulating scrolling: %s" %
                 (str(thread_name), str(datetime.datetime.now()),
                  log_prefix + " " + driver_name, domain))


def _simulate_scrolling(driver,
                        driver_name,
                        domain,
                        thread_name,
                        log_prefix="",
                        scrollto_height=None):
    _, scroll_height = get_scroll_width_and_height(driver)
    if scrollto_height:
        if scrollto_height <= scroll_height:
            logger.debug("ScrollTo height passed in, scrolling to %s" %
                         str(scrollto_height))
            _scroll_page(scrollto_height,
                         driver,
                         driver_name,
                         domain,
                         thread_name,
                         log_prefix="")
    else:
        if scroll_height > 0:
            mid = int(scroll_height / 2)
            scrollto_height = random.randrange(mid, scroll_height)
            _scroll_page(scrollto_height,
                         driver,
                         driver_name,
                         domain,
                         thread_name,
                         log_prefix="")

    return scrollto_height


def _force_save_data(drivers, thread_name=None, should_sleep=True):
    for driver_name, driver in drivers:
        if thread_name:
            logger.debug(thread_name + " - Testing rank - " + "None" +
                         ", domain - " + "about:blank" + ", driver_name - " +
                         driver_name)
        else:
            logger.debug("Testing rank - " + "None" + ", domain - " +
                         "about:blank" + ", driver_name - " + driver_name)
        driver.get(BLANK_CHROME_PAGE)

    if should_sleep:
        # sleep again to have time to save
        END_TIMER = 2
        logger.debug("%s - Sleeping for %d seconds" %
                     (str(thread_name), END_TIMER))
        time.sleep(END_TIMER)


def _get_all_urls_in_pagesource_directory(pagesource_directory):
    ignore_domains = []
    for root, _, files in os.walk(pagesource_directory):
        for file_name in files:
            # ignore MAC OS files
            if file_name != ".DS_Store" and (
                    file_name.endswith(SIMPLE_SUFFIX)
                    or file_name.endswith(EXCEPTION_SUFFIX)):
                file_name_path = root + os.sep + file_name
                with open(file_name_path, 'r') as file_opened:
                    ignore_domains.append(file_opened.readline())
        break
    return ignore_domains


def _check_pagesource(domain, driver_name, pagesource_directory):
    # do not try this domain if we already have the source for it
    source_file_name = get_page_source_filename(
        domain, driver_name, output_directory=pagesource_directory)
    source_file_name_exception = source_file_name + EXCEPTION_SUFFIX
    source_file_name_simple = source_file_name + SIMPLE_SUFFIX

    # don't crawl duplicate domains
    return os.path.isfile(source_file_name_simple) or os.path.isfile(
        source_file_name_exception)


def _save_page_source(drivers,
                      domain,
                      pagesource_directory,
                      thread_name=None,
                      original_domain=None):
    # save page source
    for driver_name, driver in drivers:
        source_file_name = get_page_source_filename(
            domain, driver_name, output_directory=pagesource_directory)
        source_file_name_exception = source_file_name + EXCEPTION_SUFFIX
        source_file_name_simple = source_file_name + SIMPLE_SUFFIX
        # don't create duplicate source files
        if not os.path.isfile(source_file_name) and not os.path.isfile(
                source_file_name_exception):
            with open(source_file_name, 'w') as page_source_file:
                logger.debug("%s - Saving Source file %s" %
                             (str(thread_name), source_file_name))
                page_source_file.write(driver.page_source)
            # create simple page source too
            with open(source_file_name_simple, 'w') as page_source_file_simple:
                logger.debug("%s - Saving Simple Source file %s" %
                             (str(thread_name), source_file_name))
                if original_domain:
                    page_source_file_simple.write(original_domain)
                else:
                    page_source_file_simple.write(domain)
        else:
            logger.debug("%s - Source file already exists %s" %
                         (str(thread_name), source_file_name))


def _save_page_source_exception(drivers,
                                domain,
                                pagesource_directory,
                                thread_name=None,
                                original_domain=None):
    # save page source for .exception so we know to skip
    for driver_name, driver in drivers:
        source_file_name_exception = get_page_source_filename(
            domain, driver_name,
            output_directory=pagesource_directory) + EXCEPTION_SUFFIX
        # don't create duplicate source files
        if not os.path.isfile(source_file_name_exception):
            with open(source_file_name_exception, 'w') as page_source_file:
                if original_domain:
                    page_source_file.write(original_domain)
                else:
                    page_source_file.write(domain)


def _get_sleep_time(before_time, default_sleep_time):
    later_time = time.time()
    time_passed = later_time - before_time
    sleep_time = 0
    if time_passed < default_sleep_time:
        sleep_time = int(default_sleep_time - time_passed)
    return sleep_time


def start_virtual_screen(virtual_display_size=(1920, 3000)):
    width, height = virtual_display_size
    logger.debug("Creating virtual display with width %d and height %d" %
                 (width, height))
    virtual_display = Display(visible=0, size=virtual_display_size)
    virtual_display.start()
    return virtual_display


def stop_virtual_screen(virtual_display):
    if virtual_display:
        virtual_display.stop()
        logger.debug("Stopped virtual display")
