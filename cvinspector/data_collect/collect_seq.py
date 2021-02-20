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
import logging
import os
import time

from selenium.common.exceptions import WebDriverException

import cvinspector.data_collect.collect as collect_core
from cvinspector.common.utils import randomword
from cvinspector.data_collect.chrome import create_control_driver, create_variant_driver, \
    quit_drivers, save_screenshot_headless, set_all_hidden_imgs_iframes, \
    create_new_profile, update_filter_list_adblock_plus_through_options

logger = logging.getLogger(__name__)
#logger.setLevel("DEBUG")


def create_control_dyn_profiles(anticv_on=False, thread_name=""):
    # create profiles (used for control only)
    if not anticv_on:
        dyn_profile_path__control = create_new_profile(
            collect_core.PROFILE_STARTING_POINT__CONTROL,
            collect_core.PROFILE_TEMP_PATH__CONTROL,
            new_profile_prefix="control_")
    else:
        dyn_profile_path__control = create_new_profile(
            collect_core.PROFILE_STARTING_POINT__ANTICV_ON__CONTROL,
            collect_core.PROFILE_TEMP_PATH__ANTICV_ON__CONTROL,
            new_profile_prefix="control_anticv_on_")
    logger.debug("%s - Creating Control Dynamic Profile %s" %
                 (thread_name, dyn_profile_path__control))

    return dyn_profile_path__control


def create_variant_dyn_profiles(anticv_on=False, thread_name=""):
    # create profiles (used for variant only)
    if not anticv_on:
        dyn_profile_path__variant = create_new_profile(
            collect_core.PROFILE_STARTING_POINT,
            collect_core.PROFILE_TEMP_PATH,
            new_profile_prefix="variant_")
    else:
        dyn_profile_path__variant = create_new_profile(
            collect_core.PROFILE_STARTING_POINT__ANTICV_ON,
            collect_core.PROFILE_TEMP_PATH__ANTICV_ON,
            new_profile_prefix="variant_anticv_on_")
    logger.debug("%s - Creating Variant Dynamic Profile %s" %
                 (thread_name, dyn_profile_path__variant))

    return dyn_profile_path__variant


def _run_measurement_for_beyond_pages_only(
        driver,
        driver_name,
        domain,
        rank,
        pagesource_directory,
        screenshot_directory,
        profile_path=None,
        thread_name=None,
        sleep_time_sec=2,
        chrome_default_download_directory=None,
        is_control=True,
        scrollto_height=None,
        find_more_pages=False,
        random_suffix_input=None,
        domain_separator="__",
        trial_suffix="trial0",
        use_https=True,
        **kwargs):

    logger.debug("%s - Running measurements..." % str(thread_name))

    potential_pages = []
    success = False
    original_domain = domain
    trunc_domain = domain
    random_suffix = random_suffix_input
    is_https = True
    if random_suffix is None:
        random_suffix = randomword(15)

    logger.debug("\t%s - Attempting to process domain %s and rank %s",
                str(thread_name), domain, rank)
    try:
        should_sleep = False

        # create drivers
        if driver is None:
            if is_control:
                driver_name, driver = create_control_driver(
                    profile_path=profile_path,
                    chrome_default_download_directory=
                    chrome_default_download_directory,
                    **kwargs)
            else:
                driver_name, driver = create_variant_driver(
                    profile_path=profile_path,
                    chrome_default_download_directory=
                    chrome_default_download_directory,
                    **kwargs)

        should_sleep, is_https = collect_core._visit_domain(
            driver,
            driver_name,
            domain,
            rank,
            pagesource_directory,
            thread_name,
            log_prefix="Main Driver",
            check_source_file=False,
            use_https=use_https)

        before_time = time.time()

        # simulate scrolling down and up
        scrollto_height = collect_core._simulate_scrolling(
            driver,
            driver_name,
            domain,
            thread_name,
            log_prefix="Main Driver",
            scrollto_height=scrollto_height)

        # sleeping
        if should_sleep:
            sleep_time = collect_core._get_sleep_time(
                before_time, collect_core.MEASUREMENT_TIMER)
            if sleep_time > 0:
                logger.debug("%s - Sleeping for %d seconds" %
                             (str(thread_name), sleep_time))
                time.sleep(sleep_time)
            else:
                time.sleep(1)

        # get domain from driver if it is really different
        domain_from_driver = driver.current_url
        logger.debug("%s - Original Domain: %s, Domain from Driver: %s" %
                     (str(thread_name), domain, domain_from_driver))
        if domain not in domain_from_driver:
            domain = domain_from_driver
            logger.debug("%s - Updated domain to %s " %
                         (str(thread_name), domain))

        # get more pages from this domain
        if find_more_pages:
            potential_pages = collect_core.find_beyond_landing_page(
                driver, domain)

        # we need to change the domain to a diff value we can use to save

        trunc_domain = domain[:
                              50] + domain_separator + random_suffix + domain_separator + trial_suffix
        logger.debug("%s - Using truncated domain %s" %
                     (str(thread_name), trunc_domain))
        # trigger an event for custom extensions to pick up
        collect_core.trigger_js_event_for_filename(driver, trunc_domain)

        # quit regular drivers
        quit_drivers([(driver_name, driver)])
        driver = None
        driver_name = None
        success = True

    except Exception as e:
        logger.warn(str(e))
        logger.warn(
            str(thread_name) + " - Could not crawl: " + original_domain)

        if driver:
            if len(trunc_domain) > 150:
                trunc_domain = trunc_domain[:
                                            50] + domain_separator + random_suffix + domain_separator
                logger.debug("truncing domain name to %s" % trunc_domain)

            collect_core._save_page_source_exception(
                [(driver_name, driver)],
                trunc_domain,
                pagesource_directory,
                thread_name=thread_name,
                original_domain=original_domain)

        logger.debug("%s - Sleeping before quitting drivers" % str(thread_name))
        time.sleep(sleep_time_sec)

        # cleanup
        quit_drivers([(driver_name, driver)])
        driver = None
        driver_name = None
        success = False

    time.sleep(1)

    logger.info("%s - Done with running measurements, domain %s, success: %s" %
                (str(thread_name), original_domain, str(success)))

    # return the scrollto_height to reuse later
    return success, scrollto_height, potential_pages, random_suffix, is_https


def _run_measurement(driver,
                     driver_name,
                     domain,
                     rank,
                     pagesource_directory,
                     screenshot_directory,
                     profile_path=None,
                     thread_name=None,
                     sleep_time_sec=2,
                     chrome_default_download_directory=None,
                     is_control=True,
                     scrollto_height=None,
                     find_more_pages=False,
                     random_suffix_input=None,
                     domain_separator="__",
                     trial_suffix="trial0",
                     use_https=True,
                     **kwargs):

    logger.debug("%s - Running measurements..." % str(thread_name))

    potential_pages = []
    success = False
    original_domain = domain
    trunc_domain = domain
    random_suffix = random_suffix_input
    is_https = True
    if random_suffix is None:
        random_suffix = randomword(15)

    logger.debug("\t%s - Attempting to process domain %s and rank %s",
                str(thread_name), domain, rank)
    try:
        should_sleep = False

        # create drivers
        if driver is None:
            if is_control:
                driver_name, driver = create_control_driver(
                    profile_path=profile_path,
                    chrome_default_download_directory=
                    chrome_default_download_directory,
                    **kwargs)
            else:
                driver_name, driver = create_variant_driver(
                    profile_path=profile_path,
                    chrome_default_download_directory=
                    chrome_default_download_directory,
                    **kwargs)

        should_sleep, is_https = collect_core._visit_domain(
            driver,
            driver_name,
            domain,
            rank,
            pagesource_directory,
            thread_name,
            log_prefix="Main Driver",
            check_source_file=False,
            use_https=use_https)

        before_time = time.time()

        # simulate scrolling down and up
        scrollto_height = collect_core._simulate_scrolling(
            driver,
            driver_name,
            domain,
            thread_name,
            log_prefix="Main Driver",
            scrollto_height=scrollto_height)

        # sleeping
        if should_sleep:
            sleep_time = collect_core._get_sleep_time(
                before_time, collect_core.MEASUREMENT_TIMER)
            if sleep_time > 0:
                logger.debug("%s - Sleeping for %d seconds" %
                             (str(thread_name), sleep_time))
                time.sleep(sleep_time)
            else:
                time.sleep(1)

        # get more pages from this domain
        if find_more_pages:
            potential_pages = collect_core.find_beyond_landing_page(
                driver, domain)

        # get domain from driver if it is really different
        domain_from_driver = driver.current_url
        logger.debug("%s - Original Domain: %s, Domain from Driver: %s" %
                     (str(thread_name), domain, domain_from_driver))
        if domain not in domain_from_driver:
            domain = domain_from_driver
            logger.debug("%s - Updated domain to %s " %
                         (str(thread_name), domain))

        # we need to change the domain to a diff value we can use to save

        trunc_domain = domain[:
                              50] + domain_separator + random_suffix + domain_separator + trial_suffix
        logger.debug("%s - Using truncated domain %s" %
                     (str(thread_name), trunc_domain))
        # trigger an event for custom extensions to pick up
        collect_core.trigger_js_event_for_filename(driver, trunc_domain)

        # find additional hidden elements before saving page source
        set_all_hidden_imgs_iframes(driver, thread_name=thread_name)

        # save page source
        collect_core._save_page_source([(driver_name, driver)],
                                       trunc_domain,
                                       pagesource_directory,
                                       thread_name=thread_name,
                                       original_domain=original_domain)

        # take screenshot
        source_file_name_directory = collect_core.get_screenshot_filename(
            trunc_domain,
            driver_name,
            output_directory=screenshot_directory,
            with_directory=True)
        source_file_name = collect_core.get_screenshot_filename(
            trunc_domain,
            driver_name,
            output_directory=screenshot_directory,
            with_directory=False)

        # don't create duplicate source files
        if not os.path.isfile(source_file_name_directory):
            logger.debug("%s - Saving screenshot" % (str(thread_name)))
            save_screenshot_headless(driver,
                                     source_file_name_directory,
                                     thread_name=thread_name)

        # save raw data
        collect_core._force_save_data([(driver_name, driver)],
                                      thread_name=thread_name,
                                      should_sleep=should_sleep)

        # quit regular drivers
        quit_drivers([(driver_name, driver)])
        driver = None
        driver_name = None
        success = True

    except Exception as e:
        logger.warn(str(e))
        #print(str(thread_name) + " - Recreating driver due to selenium problems")
        logger.warn(
            str(thread_name) + " - Could not crawl: " + original_domain)

        if driver:
            if len(trunc_domain) > 150:
                trunc_domain = trunc_domain[:
                                            50] + domain_separator + random_suffix + domain_separator
                logger.debug("truncing domain name to %s" % trunc_domain)

            collect_core._save_page_source_exception(
                [(driver_name, driver)],
                trunc_domain,
                pagesource_directory,
                thread_name=thread_name,
                original_domain=original_domain)

        logger.warn("%s - Sleeping before quitting drivers" % str(thread_name))
        time.sleep(sleep_time_sec)

        # cleanup
        quit_drivers([(driver_name, driver)])
        driver = None
        driver_name = None
        success = False

    time.sleep(1)

    logger.info("%s - Done with running measurements, domain %s, success: %s" %
                (str(thread_name), original_domain, str(success)))

    # return the scrollto_height to reuse later
    return success, scrollto_height, potential_pages, random_suffix, is_https


def _clean_profile(profile_path, thread_name):
    # clean up profile path (variant only)
    collect_core.delete_profile(profile_path, thread_name=thread_name)


# Process one domain only with control and variant sequentially
# Don't do variant if control did not work
def process_control_and_variant(domain,
                                rank,
                                pagesource_dir,
                                screenshot_dir,
                                downloads_dir,
                                find_more_pages=False,
                                thread_name=None,
                                trials=4,
                                anticv_on=False,
                                use_https=True,
                                **kwargs):


    logger.info("%s - Starting control: %s", str(thread_name), str(domain))
    logger.info("===============================")

    random_suffix = None
    scrollto_height = None
    potential_pages = []
    control_success = True
    is_https = use_https
    for trial_number in range(trials):
        logger.info("\t%s - Starting control trial %d: %s" %
                    (str(thread_name), trial_number, str(domain)))

        # make new profiles per domain
        dyn_profile_path__control = create_control_dyn_profiles(
            anticv_on=anticv_on, thread_name=thread_name)
        logger.debug("\t%s - Creating control profile %s" %
                     (str(thread_name), dyn_profile_path__control))

        # create control driver
        driver_name, control_driver = create_control_driver(
            profile_path=dyn_profile_path__control,
            chrome_default_download_directory=downloads_dir,
            **kwargs)

        # run measurement for control
        control_success_temp, scrollto_height_temp, potential_pages_temp, random_suffix_temp, is_https = _run_measurement(
            control_driver,
            driver_name,
            domain,
            rank,
            pagesource_dir,
            screenshot_dir,
            profile_path=dyn_profile_path__control,
            thread_name=thread_name,
            is_control=True,
            find_more_pages=find_more_pages,
            random_suffix_input=random_suffix,
            trial_suffix="trial" + str(trial_number),
            use_https=is_https)

        _clean_profile(dyn_profile_path__control, thread_name)

        # update values
        control_success = control_success and control_success_temp
        if scrollto_height is None:
            scrollto_height = scrollto_height_temp
        if random_suffix is None:
            random_suffix = random_suffix_temp
        if len(potential_pages) == 0:
            potential_pages = potential_pages_temp

    logger.info("\t%s - Done with control: %s, success %s" %
                (str(thread_name), str(domain), str(control_success)))

    variant_success = True

    if control_success:
        logger.info("%s - Starting variant: %s",
                    str(thread_name), str(domain))
        logger.info("===============================")

        for trial_number in range(trials):
            logger.info("\t%s - Starting variant trial %d: %s" %
                        (str(thread_name), trial_number, str(domain)))

            # make new profiles per domain
            dyn_profile_path__variant = create_variant_dyn_profiles(
                anticv_on=anticv_on, thread_name=thread_name)
            logger.debug("\t%s - Creating variant profile %s" %
                         (str(thread_name), dyn_profile_path__variant))

            # move on to variant
            variant_driver_name, variant_driver = create_variant_driver(
                chrome_default_download_directory=downloads_dir,
                profile_path=dyn_profile_path__variant,
                **kwargs)

            # run measurement for control
            # make sure we scroll to same height for variant
            # make sure we use the same random suffix for variant
            # use is_https that we found from control
            variant_success_temp, _, _, _, _ = _run_measurement(
                variant_driver,
                variant_driver_name,
                domain,
                rank,
                pagesource_dir,
                screenshot_dir,
                profile_path=dyn_profile_path__variant,
                thread_name=thread_name,
                is_control=False,
                scrollto_height=scrollto_height,
                random_suffix_input=random_suffix,
                trial_suffix="trial" + str(trial_number),
                use_https=is_https,
                **kwargs)

            _clean_profile(dyn_profile_path__variant, thread_name)

            # update success
            variant_success = variant_success and variant_success_temp

        logger.info("\t%s - Done with variant: %s, success %s" %
                    (str(thread_name), str(domain), str(variant_success)))

    logger.debug("Control Success %s, Variant Success %s" %
                 (str(control_success), str(variant_success)))

    overall_success = control_success and variant_success
    return overall_success, potential_pages, is_https


# Processes multiple sites and the beyond pages
def process_sites(file_data,
                  pagesource_dir,
                  screenshot_dir,
                  downloads_dir,
                  anticv_on=False,
                  use_dynamic_profile=True,
                  thread_name=None,
                  beyond_landing_pages=True,
                  trials=4,
                  beyond_landing_pages_only=False,
                  **kwargs):

    BEYOND_LANDING_PAGE_LIMIT = 1

    domains_ignored = collect_core._get_all_urls_in_pagesource_directory(
        pagesource_dir)
    logger.debug("%s - Domains ignored length: %d" %
                 (str(thread_name), len(domains_ignored)))

    chrome_failed_count = 0
    for rank, domain in file_data:
        if domain not in domains_ignored:
            try:
                if not beyond_landing_pages_only:
                    success, potential_pages, is_https = process_control_and_variant(
                        domain,
                        rank,
                        pagesource_dir,
                        screenshot_dir,
                        downloads_dir,
                        find_more_pages=True,
                        thread_name=thread_name,
                        trials=trials,
                        anticv_on=anticv_on,
                        **kwargs)
                else:
                    # make new profiles per domain
                    dyn_profile_path__control = create_control_dyn_profiles(
                        anticv_on=anticv_on, thread_name=thread_name)
                    logger.debug("%s - Creating control profile %s" %
                                 (str(thread_name), dyn_profile_path__control))

                    # create control driver
                    driver_name, control_driver = create_control_driver(
                        profile_path=dyn_profile_path__control,
                        chrome_default_download_directory=downloads_dir,
                        **kwargs)

                    # run measurement for control
                    success, _, potential_pages, _, is_https = _run_measurement_for_beyond_pages_only(
                        control_driver,
                        driver_name,
                        domain,
                        rank,
                        pagesource_dir,
                        screenshot_dir,
                        profile_path=dyn_profile_path__control,
                        thread_name=thread_name,
                        is_control=True,
                        find_more_pages=True,
                        trial_suffix="trial" + str(0),
                        **kwargs)

                    _clean_profile(dyn_profile_path__control, thread_name)

                # get beyond landing pages, we reuse the profile
                # make sure we use the is_https that we found
                if success and beyond_landing_pages:
                    logger.debug("%s - Retrieving beyond landing pages" %
                                 str(thread_name))
                    more_pages = potential_pages[:BEYOND_LANDING_PAGE_LIMIT]
                    logger.debug(more_pages)
                    for url, is_first_party, is_diff_subdomain, is_diff_path_and_no_query, _ in more_pages:
                        logger.debug(
                            "%s - Processing beyond landing page: %s" %
                            (str(thread_name), url))
                        process_control_and_variant(url,
                                                    rank,
                                                    pagesource_dir,
                                                    screenshot_dir,
                                                    downloads_dir,
                                                    find_more_pages=False,
                                                    thread_name=thread_name,
                                                    trials=trials,
                                                    anticv_on=anticv_on,
                                                    use_https=is_https,
                                                    **kwargs)

            except WebDriverException as e:
                logger.warn("%s - Completely Done with : %s" %
                            (str(thread_name), domain))

                if "Chrome failed to start" not in str(e):
                    raise e
                else:
                    chrome_failed_count += 1
                    if chrome_failed_count > 3:
                        raise e

                    time.sleep(10)
                    logger.debug("%s - Completely Done with : %s" %
                                 (str(thread_name), domain))
                    continue

            logger.info("%s - Completely Done with : %s" %
                        (str(thread_name), domain))
        else:
            logger.debug("%s - PRE-Skipping url %s" % (thread_name, domain))


# main method to do data collection
def run_data_collection(csv_file_path,
                        output_directory,
                        crawler_group_name,
                        anticv_on=False,
                        csv_delimiter=',',
                        start_index=0,
                        end_index=50,
                        sleep_time_sec=2,
                        use_dynamic_profile=True,
                        trials=4,
                        beyond_landing_pages=True,
                        beyond_landing_pages_only=False,
                        by_rank=True,
                        **kwargs):

    thread_name = "Process-" + randomword(5)
    logger.info("%s - Created thread name " % thread_name)

    # prepare directories
    pagesource_dir, screenshot_dir, downloads_dir = collect_core.create_directories(
        output_directory, crawler_group_name)

    site_file_path = csv_file_path

    logger.info("%s - Using list of websites from %s" %
                (thread_name, site_file_path))
    logger.info("%s - Parsing start_index %d to end index %d" %
                (thread_name, start_index, end_index))

    f = open(site_file_path)
    reader = csv.reader(f, delimiter=csv_delimiter)

    logger.debug("%s - Reading in data from file" % thread_name)
    file_data = []
    CSV_RANK_INDEX = 0
    CSV_URL_INDEX = 1
    file_data_chunk = []
    for row in reader:
        rank = int(row[CSV_RANK_INDEX])
        domain = row[CSV_URL_INDEX]
        if by_rank:
            if start_index <= rank <= end_index:
                file_data_chunk.append((rank, domain))
        else:
            file_data.append((rank, domain))

    #print("Done reading in data from file")
    if not by_rank:
        # go by file index order and not rank
        file_data_chunk = file_data[start_index:end_index]

    retry = True
    max_retry = 3
    retry_count = 0

    while retry_count <= max_retry and retry:
        # start virtual display
        virtual_display = collect_core.start_virtual_screen()

        try:
            process_sites(file_data_chunk,
                          pagesource_dir,
                          screenshot_dir,
                          downloads_dir,
                          anticv_on=anticv_on,
                          use_dynamic_profile=use_dynamic_profile,
                          thread_name=thread_name,
                          trials=trials,
                          beyond_landing_pages=beyond_landing_pages,
                          beyond_landing_pages_only=beyond_landing_pages_only,
                          **kwargs)

            retry = False
        except WebDriverException as e:
            retry_count += 1
            logger.warn(e)
            logger.warn("%s - Major exception: Retrying crawling again %d" %
                        (str(thread_name), retry_count))

        # stop the virtual display
        if virtual_display is not None:
            collect_core.stop_virtual_screen(virtual_display)

        time.sleep(30)

    logger.info("%s - Done" % str(thread_name))


def run_default_profile_creation(anticv_on=False, is_monitoring=False, **kwargs):

    thread_name = "Process-" + randomword(5)
    logger.debug("%s - Created thread name " % thread_name)

    profile_path = collect_core.PROFILE_STARTING_POINT
    if is_monitoring:
        profile_path = collect_core.PROFILE_STARTING_POINT_MONITORING
    if anticv_on:
        profile_path = collect_core.PROFILE_STARTING_POINT__ANTICV_ON
        if is_monitoring:
            profile_path = collect_core.PROFILE_STARTING_POINT_MONITORING__ANTICV_ON

    # make profile directories
    if not is_monitoring:
        collect_core.create_profile_directories()
    else:
        collect_core.create_profile_directories_monitoring()

    # move on to variant
    variant_driver_name, variant_driver = create_variant_driver(
        profile_path=profile_path, **kwargs)

    collect_core._visit_domain(variant_driver,
                               variant_driver_name,
                               "https://google.com",
                               0,
                               None,
                               thread_name,
                               log_prefix="Main Driver")

    logger.debug("%s  - Thread will run forever" % str(thread_name))

    while (True):
        continue

    quit_drivers([variant_driver_name, variant_driver])


def run_default_profile_creation_control(anticv_on=False, is_monitoring=False, **kwargs):

    thread_name = "Process-" + randomword(5)
    logger.debug("%s - Created thread name " % thread_name)

    profile_path = collect_core.PROFILE_STARTING_POINT__CONTROL
    if is_monitoring:
        profile_path = collect_core.PROFILE_STARTING_POINT_MONITORING__CONTROL
    if anticv_on:
        profile_path = collect_core.PROFILE_STARTING_POINT__ANTICV_ON__CONTROL
        if is_monitoring:
            profile_path = collect_core.PROFILE_STARTING_POINT_MONITORING__ANTICV_ON__CONTROL

    # make profile directories
    if not is_monitoring:
        collect_core.create_profile_directories()
    else:
        collect_core.create_profile_directories_monitoring()

    driver_name, driver = create_control_driver(profile_path=profile_path, **kwargs)

    collect_core._visit_domain(driver,
                               driver_name,
                               "https://google.com",
                               0,
                               None,
                               thread_name,
                               log_prefix="Main Driver")

    logger.debug("%s  - Thread will run forever" % str(thread_name))

    while (True):
        continue

    quit_drivers([driver_name, driver])


# start VirtualDisplay and go to google. 
# Then inject JS to update filter list.
# Then go to yahoo.com to give it time to update the rules
# do it for variant
def update_filter_list_for_default_profiles(
    abp_extension_absolute_path="/home/ubuntu/github/adblockpluschrome-anticv/devenv.chrome",
    anticv_on=False, 
    thread_name=None,
    **kwargs):

    virtual_display = collect_core.start_virtual_screen()

    if not thread_name:
        thread_name = "Process-" + randomword(5)
        logger.debug("%s - Created thread name ", thread_name)

    # variant
    logger.debug("%s - Updating variant profile filter list", thread_name)

    profile_path = collect_core.PROFILE_STARTING_POINT
    if anticv_on:
        profile_path = collect_core.PROFILE_STARTING_POINT__ANTICV_ON
    
    variant_driver_name, variant_driver = create_variant_driver(
        profile_path=profile_path, **kwargs)

    selen_exception = None
    try:
        update_filter_list_adblock_plus_through_options(variant_driver, abp_extension_absolute_path)
    except Exception as e:
        logger.warning("Could not update filter list")
        selen_exception = e
    finally:
        time.sleep(5)
        quit_drivers([(variant_driver_name, variant_driver)])
        # stop the virtual display
        if virtual_display is not None:
            collect_core.stop_virtual_screen(virtual_display)
        
        # clean up first before throwing exception
        if selen_exception:
            raise selen_exception

        chrome_singletons = glob.glob(profile_path + os.sep + "*" + "Singleton" +"*")
        for f in chrome_singletons:
            os.remove(f)

        time.sleep(5)
