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
import os
import subprocess


def build_extension(path):
    print("Attempting to build custom chrome extension: " + path)
    # cwd gives us the current working directory
    subprocess.call(["npm", "install"], cwd=path)
    subprocess.call(["npm", "audit", "fix"], cwd=path)
    subprocess.call(["npm", "run", "build", "INLINE_RUNTIME_CHUNK=false"],
                    cwd=path)


def main():
    parser = argparse.ArgumentParser(
        description='Given a directory of chrome extensions, build them.')

    ## REQUIRED
    parser.add_argument('--extension_path',
                        required=True,
                        help='Path to extensions')

    args = parser.parse_args()
    print(args)

    extension_path = args.extension_path

    for root, dirs, files in os.walk(extension_path):
        print(root)
        print(dirs)
        for subdir in dirs:
            chrome_ext_path = root + os.sep + subdir
            build_extension(chrome_ext_path)
        break


if __name__ == "__main__":
    main()
