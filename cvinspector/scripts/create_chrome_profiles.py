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
import sys
import time
import os
import glob
import logging

import cvinspector.data_collect.collect as collect_core
from cvinspector.data_collect.chrome import create_variant_driver, \
    create_control_driver, setup_adblock_plus_through_options, quit_drivers


def create_default_profile_no_adblocker(
        logger,
        anticv_on=False,
        **kwargs):

    virtual_display = collect_core.start_virtual_screen()

    profile_path = collect_core.PROFILE_STARTING_POINT__CONTROL
    if anticv_on:
        profile_path = collect_core.PROFILE_STARTING_POINT__ANTICV_ON__CONTROL

    driver_name, driver = create_control_driver(
        profile_path=profile_path, **kwargs)

    selen_exception = None
    try:
        driver.get("https://www.google.com/")
    except Exception as e:
        selen_exception = e
    finally:
        time.sleep(5)
        quit_drivers([(driver_name, driver)])
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


def create_default_profile_with_adblocker(
        abp_extension_absolute_path,
        logger,
        anticv_on=False,
        **kwargs):

    virtual_display = collect_core.start_virtual_screen()

    profile_path = collect_core.PROFILE_STARTING_POINT
    if anticv_on:
        profile_path = collect_core.PROFILE_STARTING_POINT__ANTICV_ON

    variant_driver_name, variant_driver = create_variant_driver(
        profile_path=profile_path, **kwargs)

    selen_exception = None
    try:
        setup_adblock_plus_through_options(variant_driver,
                                           abp_extension_absolute_path,
                                           turn_on_anticv_list=anticv_on)
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


def main():
    parser = argparse.ArgumentParser(
        description='Create default chrome profiles')

    # REQUIRED
    parser.add_argument('--chrome_driver_path',
                        required=True,
                        help='Path to chrome driver file')
    parser.add_argument('--chrome_adblockplus_ext_abs_path',
                        required=True,
                        help='Absolute path to chrome extension')
    parser.add_argument('--log_level', default="INFO", help='Log level')

    args = parser.parse_args()
    print(args)

    # set up logger
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.log_level)
    logging.basicConfig(format='%(asctime)s %(message)s', level=numeric_level)
    logger = logging.getLogger(__name__)

    collect_core.create_profile_directories()

    create_default_profile_no_adblocker(logger,
                                        anticv_on=False,
                                        chrome_driver_path=args.chrome_driver_path)
    logger.info("Create Chrome profile : no adblocker: anticv_on=False")

    create_default_profile_no_adblocker(logger,
                                        anticv_on=True,
                                        chrome_driver_path=args.chrome_driver_path)
    logger.info("Create Chrome profile : no adblocker: anticv_on=True")

    create_default_profile_with_adblocker(args.chrome_adblockplus_ext_abs_path,
                                          logger,
                                          anticv_on=False,
                                          chrome_driver_path=args.chrome_driver_path,
                                          chrome_ext_path=args.chrome_adblockplus_ext_abs_path)
    logger.info("Create Chrome profile : with adblocker: anticv_on=False")

    create_default_profile_with_adblocker(args.chrome_adblockplus_ext_abs_path,
                                          logger,
                                          anticv_on=True,
                                          chrome_driver_path=args.chrome_driver_path,
                                          chrome_ext_path=args.chrome_adblockplus_ext_abs_path)
    logger.info("Create Chrome profile : with adblocker: anticv_on=True")


if __name__ == "__main__":
    main()
