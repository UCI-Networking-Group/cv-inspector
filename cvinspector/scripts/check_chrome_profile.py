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

from cvinspector.data_collect.collect_seq import run_default_profile_creation, run_default_profile_creation_control


def main():
    parser = argparse.ArgumentParser(
        description='Check default chrome profile')

    # REQUIRED
    parser.add_argument('--anticv_on', default="false", type=str,
                        help='Whether adblocker uses anticv list or not during data collection. Default=False')
    parser.add_argument('--is_control', default="false", type=str,
                        help='Whether we are creating for control. Default=False')
    parser.add_argument('--is_monitoring', default="false", type=str,
                        help='Whether we are creating for monitoring. Default=False')
    parser.add_argument('--chrome_driver_path',
                        required=True,
                        help='Path to chrome driver file')
    parser.add_argument('--chrome_adblockplus_ext_abs_path',
                        required=True,
                        help='Absolute path to chrome extension')

    args = parser.parse_args()
    print(args)

    anticv_on = args.anticv_on.lower() == "true"
    is_control = args.is_control.lower() == "true"
    is_monitoring = args.is_monitoring.lower() == "true"

    if not is_control:
        run_default_profile_creation(
            anticv_on=anticv_on,
            is_monitoring=is_monitoring,
            chrome_driver_path=args.chrome_driver_path,
            chrome_ext_path=args.chrome_adblockplus_ext_abs_path)
    else:
        run_default_profile_creation_control(
            anticv_on=anticv_on,
            is_monitoring=is_monitoring,
            chrome_driver_path=args.chrome_driver_path)

    # exit program early
    print("DONE")
    sys.exit(0)


if __name__ == "__main__":
    main()
