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

import argparse
import glob
import logging
import os
import random
import shutil
import string
import sys

from Naked.toolshed.shell import execute_js

from cvinspector.common.script_utils import process_group_trails, transfer_prep, diff_groups, create_time_series_csvs
from cvinspector.common.utils import WEBREQUESTS_DATA_FILE_SUFFIX_CONTROL, WEBREQUESTS_DATA_FILE_SUFFIX_VARIANT, \
    DOMMUTATION_DATA_FILE_SUFFIX_CONTROL, DOMMUTATION_DATA_FILE_SUFFIX_VARIANT
from cvinspector.data_collect.collect import get_downloads_directory
from cvinspector.data_collect.collect_seq import run_data_collection, update_filter_list_for_default_profiles
from cvinspector.data_migrate.utils import MONGO_CLIENT_HOST, MONGO_CLIENT_PORT
from cvinspector.ml.feature_constants import BOOLEAN_FEATURES
from cvinspector.ml.labeling import label_dataset_from_saved_clf
from cvinspector.ml.output_features_to_csv import _clean_scale_data_for_labeling, get_test_features_from_file
from cvinspector.ml.output_features_to_csv import write_feature_csv
from cvinspector.ml.output_features_to_csv import write_urls_txt, RAW_UNLABEL_FILE_KEY


def move_data_collected_to_output(downloads_directory, crawler_group_name,
                                  crawl_data_output__webrequests_control,
                                  crawl_data_output__webrequests_variant,
                                  crawl_data_output__dom_control,
                                  crawl_data_output__dom_variant,
                                  logger):
    # get all files from downloads directory to the output directory

    ## WEB REQUESTS

    # move all control webrequest files from downloads_directory to corresponding output directory
    for file_path in glob.glob(downloads_directory + os.sep + "*" +
                               WEBREQUESTS_DATA_FILE_SUFFIX_CONTROL):
        try:
            shutil.move(file_path, crawl_data_output__webrequests_control)
        except shutil.Error as e:
            logger.info(e)

    # move all variant webrequest files from downloads_directory to corresponding output directory
    for file_path in glob.glob(downloads_directory + os.sep + "*" +
                               WEBREQUESTS_DATA_FILE_SUFFIX_VARIANT):
        try:
            shutil.move(file_path, crawl_data_output__webrequests_variant)
        except shutil.Error as e:
            logger.info(e)

    ## DOM MUTATION

    # move all control dommutation files from downloads_directory to corresponding output directory
    for file_path in glob.glob(downloads_directory + os.sep + "*" +
                               DOMMUTATION_DATA_FILE_SUFFIX_CONTROL):
        try:
            shutil.move(file_path, crawl_data_output__dom_control)
        except shutil.Error as e:
            logger.info(e)

    # move all variant webrequest files from downloads_directory to corresponding output directory
    for file_path in glob.glob(downloads_directory + os.sep + "*" +
                               DOMMUTATION_DATA_FILE_SUFFIX_VARIANT):
        try:
            shutil.move(file_path, crawl_data_output__dom_variant)
        except shutil.Error as e:
            logger.info(e)


def collect_data(sites_csv,
                 output_directory,
                 crawler_group_name,
                 **kwargs):
    run_data_collection(sites_csv,
                        output_directory,
                        crawler_group_name,
                        **kwargs)


def main():
    parser = argparse.ArgumentParser(
        description=
        'Given a list of domains to label: we do all necessary stuff to apply classifer on the sites and output a csv with the results for monitoring.'
    )

    ## REQUIRED
    parser.add_argument(
        '--sites_csv',
        required=True,
        help=
        'CSV of urls to label whether circumvention or not. [Rank, URL]. Line delimited.'
    )
    parser.add_argument(
        '--anticv_on',
        default="false",
        type=str,
        help=
        'Whether adblocker uses anticv list or not during data collection. Default=False'
    )
    parser.add_argument('--start_index',
                        required=True,
                        type=int,
                        help='Start processing at which line')
    parser.add_argument('--end_index',
                        required=True,
                        type=int,
                        help='End processing at which line')
    parser.add_argument('--output_suffix',
                        required=True,
                        help='Suffix used in output to make files unique')
    parser.add_argument('--output_directory',
                        required=True,
                        help='Output directory')
    parser.add_argument('--output_directory_ts',
                        required=True,
                        help='Output directory for time series')
    parser.add_argument('--filter_list_paths',
                        required=True,
                        help='Path to filter lists used to find tracking')
    parser.add_argument('--chrome_driver_path',
                        required=True,
                        help='Path to chrome driver file')
    parser.add_argument('--chrome_adblockplus_ext_abs_path',
                        required=True,
                        help='Absolute path to chrome extension')

    # Classiifer
    parser.add_argument('--classifier_path',
                        required=True,
                        help='Classifier to load')
    parser.add_argument(
        '--classifier_features_file_path',
        required=True,
        help=
        'File with features that we care about. Line delimited. Used to reduce features from unlabel data'
    )
    parser.add_argument('--threshold',
                        default="0.50",
                        help='threshold for predict probability')

    # OPTIONAL
    parser.add_argument(
        '--use_dynamic_profile',
        default="true",
        type=str,
        help=
        'Whether profile loaded is dynamic (copy from a base profile) or using only one profile. Default=True'
    )
    parser.add_argument('--sites_csv_delimiter',
                        default=',',
                        help='Delimiter to parse sites csv.')
    parser.add_argument(
        '--crawler_group_name',
        help=
        'Name to group all data collected within mongoDB. A random one will be generated if not passed in.'
    )
    parser.add_argument('--log_level', default="INFO", help='Log level')
    parser.add_argument(
        '--trials',
        type=int,
        default=4,
        help='Number of trials to do per website per control/variant')
    parser.add_argument('--beyond_landing_pages',
                        default="true",
                        help='Whether we crawl beyond the landing page')
    parser.add_argument('--beyond_landing_pages_only',
                        default="false",
                        help='Whether we crawl beyond landing pages only')
    parser.add_argument(
        '--by_rank',
        default="true",
        help='crawl by the rank column. If false, it goes by file order')
    parser.add_argument(
        '--skip_data_collection',
        default="false",
        type=str,
        help=
        'Skip data collection, assuming the data collected is already there in the correct directories'
    )
    parser.add_argument('--ground_truth_file',
                        help='Ground truth file to mark rows as labeled')
    parser.add_argument(
        '--output_external_logs',
        default="true",
        help=
        'Whether we want more logs from diff analysis and feature extraction')
    # Mongo
    parser.add_argument('--mongodb_client',
                        default=MONGO_CLIENT_HOST,
                        help='Client of mongoDB')
    parser.add_argument('--mongodb_port',
                        default=MONGO_CLIENT_PORT,
                        help='Port of mongoDB')
    parser.add_argument('--mongodb_username', help='username of mongoDB')
    parser.add_argument('--mongodb_password', help='password of mongoDB')

    args = parser.parse_args()
    print(args)

    # set up logger
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.log_level)
    logging.basicConfig(format='%(asctime)s %(message)s', level=numeric_level)

    logger = logging.getLogger(__name__)

    anticv_on = args.anticv_on.lower() == "true"
    use_dynamic_profile = args.use_dynamic_profile.lower() == "true"
    beyond_landing_pages = args.beyond_landing_pages.lower() == "true"
    beyond_landing_pages_only = args.beyond_landing_pages_only.lower() == "true"
    by_rank = args.by_rank.lower() == "true"
    skip_data_collection = args.skip_data_collection.lower() == "true"

    logger.info("NOTE: Using use_dynamic_profile: %s", str(use_dynamic_profile))
    logger.info("NOTE: Using beyond_landing_pages: %s", str(beyond_landing_pages))
    logger.info("NOTE: Using beyond_landing_pages_only: %s",
                str(beyond_landing_pages_only))
    logger.debug("NOTE: Using by_rank: %s", str(by_rank))
    logger.debug("NOTE: Using skip_data_collection: %s", str(skip_data_collection))

    crawler_group_name = args.crawler_group_name
    if crawler_group_name is None:
        crawler_group_name = ''.join([
            random.choice(string.ascii_letters + string.digits)
            for n in range(10)
        ])
        logger.debug("NOTE: No crawler group name was given, using generated one: %s",
                     crawler_group_name)

    logger.info("NOTE: Using crawler group name: %s", crawler_group_name)

    state_anticv_label_on = "OFF"
    if anticv_on:
        state_anticv_label_on = "ON"
    logger.debug("NOTE: Data collection with ANTICV LIST %s", state_anticv_label_on)

    # use crawler_group_name to change the output directory
    main_output_directory = args.output_directory + os.sep + crawler_group_name
    main_output_directory_ts = args.output_directory_ts + os.sep + crawler_group_name
    logger.info("Main output directory %s", main_output_directory)
    logger.info("Main timeseries output directory %s", main_output_directory_ts)

    if not os.path.isdir(main_output_directory):
        try:
            os.makedirs(main_output_directory)
            os.makedirs(main_output_directory_ts)
        except Exception as e:
            logger.error(e)
            logger.error("Could not create MAIN output directory " + main_output_directory)
            sys.exit(1)

    # Collect data for every site given
    if not skip_data_collection:
        # update the filter list first of the default chrome profiles
        update_filter_list_for_default_profiles(
            anticv_on=anticv_on,
            abp_extension_absolute_path=args.chrome_adblockplus_ext_abs_path,
            chrome_driver_path=args.chrome_driver_path,
            chrome_ext_path=args.chrome_adblockplus_ext_abs_path)

        # then collect the data
        collect_data(args.sites_csv,
                     main_output_directory,
                     crawler_group_name,
                     start_index=args.start_index,
                     end_index=args.end_index,
                     anticv_on=anticv_on,
                     csv_delimiter=args.sites_csv_delimiter,
                     use_dynamic_profile=use_dynamic_profile,
                     trials=args.trials,
                     beyond_landing_pages=beyond_landing_pages,
                     beyond_landing_pages_only=beyond_landing_pages_only,
                     by_rank=by_rank,
                     chrome_driver_path=args.chrome_driver_path,
                     chrome_ext_path=args.chrome_adblockplus_ext_abs_path)
    else:
        logger.warning("Note: Skipping data collection")

    # Make crawl data folder using crawler_group_name
    crawl_data_output = main_output_directory + os.sep + "crawl_data_" + crawler_group_name + os.sep
    crawl_data_output__webrequests_control = crawl_data_output + "control_webrequests" + os.sep
    crawl_data_output__webrequests_variant = crawl_data_output + "variant_webrequests" + os.sep
    crawl_data_output__dom_control = crawl_data_output + "control_dommutation" + os.sep
    crawl_data_output__dom_variant = crawl_data_output + "variant_dommutation" + os.sep

    if not os.path.isdir(crawl_data_output):
        try:
            os.makedirs(crawl_data_output)
            os.makedirs(crawl_data_output__webrequests_control)
            os.makedirs(crawl_data_output__webrequests_variant)
            os.makedirs(crawl_data_output__dom_control)
            os.makedirs(crawl_data_output__dom_variant)
        except Exception as e:
            logger.warning(e)
            logger.warning("Could not create necessary output directories %s",
                           crawl_data_output)
            sys.exit(1)

    downloads_dir = get_downloads_directory(main_output_directory,
                                            crawler_group_name)

    # Move the data to the right output directory
    move_data_collected_to_output(downloads_dir, crawler_group_name,
                                  crawl_data_output__webrequests_control,
                                  crawl_data_output__webrequests_variant,
                                  crawl_data_output__dom_control,
                                  crawl_data_output__dom_variant,
                                  logger)

    # Create group trials csv
    groups_file_name = "groups_" + crawler_group_name + ".csv"
    groups_file_name = groups_file_name.replace(" ", "_")
    groups_file_path = process_group_trails(main_output_directory,
                                            groups_file_name,
                                            crawler_group_name,
                                            logger,
                                            trials=args.trials)
    logger.debug("Created group files %s", groups_file_path)

    # Transfer data to DB
    transfer_prep(main_output_directory, crawler_group_name, logger)

    # Create Diff Groups
    diff_groups(args.mongodb_client,
                args.mongodb_port,
                crawler_group_name,
                groups_file_path,
                logger,
                mongodb_username=args.mongodb_username,
                mongodb_password=args.mongodb_password)

    # Create time series CSVs
    ts_output_directory = main_output_directory_ts + os.sep + "ts_" + crawler_group_name + os.sep
    if not os.path.isdir(ts_output_directory):
        try:
            os.makedirs(ts_output_directory)
        except Exception as e:
            logger.error(e)
            logger.error(
                "Could not create necessary output directories for time series %s",
                ts_output_directory)
            sys.exit(1)

    # CSV output of timeseries file
    ts_file_mapping_file_path = ts_output_directory + "filename_mapping.csv"
    create_time_series_csvs(groups_file_path, ts_output_directory, None, trials=args.trials)
    logger.info("Created timeseries " + ts_file_mapping_file_path)

    # Get variant urls (this grabs all outgoing URLs that will happen in variant side only)
    variant_urls_output_file_name = crawler_group_name + "_variant_urls"
    write_urls_txt(crawler_group_name,
                   args.mongodb_client,
                   args.mongodb_port,
                   ground_truth_file_path=args.ground_truth_file,
                   csv_file_name=variant_urls_output_file_name,
                   output_directory=main_output_directory,
                   ground_truth_only=False,
                   trial_count=args.trials)
    logger.debug("Got variant URLS %s", variant_urls_output_file_name)

    # Call nodejs adblock parser to identify the tracking urls
    variant_urls_file_path = main_output_directory + os.sep + variant_urls_output_file_name + ".txt"
    # File of tracking urls
    variant_tracking_file_path = main_output_directory + os.sep + variant_urls_output_file_name + "_tracking.txt"
    adblock_parser_arguments = args.filter_list_paths + " " + variant_urls_file_path + " " + variant_tracking_file_path
    execute_js("./external_scripts/adblock_parser_tracking.js", adblock_parser_arguments)
    logger.debug("Got tracking URLS %s", variant_tracking_file_path)

    # Feature extraction (this is for unlabeled data for monitoring)
    features_file_name = crawler_group_name + "_features"
    # Features File Path
    features_file_path = main_output_directory + os.sep + features_file_name + ".csv"
    write_feature_csv(crawler_group_name,
                      args.mongodb_client,
                      args.mongodb_port,
                      variant_tracking_file_path,
                      ground_truth_file_path=args.ground_truth_file,
                      csv_file_name=features_file_name,
                      output_directory=main_output_directory,
                      ground_truth_only=False,
                      time_series_mapping=ts_file_mapping_file_path,
                      include_control=False,
                      output_external_logs=args.output_external_logs,
                      trials=args.trials)
    logger.debug("Got features %s", features_file_path)

    # Clean features for unlabeled data
    test_features_only = get_test_features_from_file(
        args.classifier_features_file_path)
    result_files = _clean_scale_data_for_labeling(
        features_file_path,
        args.output_suffix,
        main_output_directory,
        BOOLEAN_FEATURES,
        test_features_only=test_features_only,
        ignore_labels=True)
    logger.debug("Cleaned features")

    unlabel_clean_file_path = result_files[RAW_UNLABEL_FILE_KEY]

    # Label the file using classifier
    threshold = float(args.threshold)
    labeled_file_name = crawler_group_name + "_labeled.csv"
    labeled_file_path = main_output_directory + os.sep + labeled_file_name
    label_dataset_from_saved_clf(unlabel_clean_file_path,
                                 args.classifier_path,
                                 labeled_file_name,
                                 main_output_directory,
                                 threshold=threshold,
                                 test_features_only=test_features_only)

    # DONE
    logger.info("DONE - Labeled file for %s located at %s",
                crawler_group_name, labeled_file_path)


if __name__ == "__main__":
    main()
