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

import requests
from flask import Flask, request, Response

app = Flask(__name__)
SITE_NAME = 'https://easylist-downloads.adblockplus.org/'
excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
FILTER_LIST_DIR = ""


# To get the frozen filter lists, use :
# https://easylist-downloads.adblockplus.org/easylist.txt?addonName=adblockpluschrome&addonVersion=3.7&application=chrome&applicationVersion=78&platform=chromium&platformVersion=78&lastVersion=0&downloadCount=0

@app.route('/')
def index():
    return 'Flask is running!'


@app.route('/<path:path>', methods=['GET'])
def proxy(path):
    global SITE_NAME
    if request.method == 'GET':
        print("proxing filter rules")
        url = f'{SITE_NAME}{path}'
        print(url)
        resp = requests.get(url)

        # we force the content from the file
        content = None
        if "easylist" in path:
            print("reading content from local easylist.txt")
            content = open(FILTER_LIST_DIR + os.sep + 'easylist.txt', 'r').read()
        else:
            print("reading content from local abp-filters-anti-cv.txt")
            content = open(FILTER_LIST_DIR + os.sep + 'abp-filters-anti-cv.txt', 'r').read()
        # print(content)

        headers = [(name, value) for (name, value) in resp.raw.headers.items() if name.lower() not in excluded_headers]
        response = Response(content, resp.status_code, headers)
        return response


def main():
    parser = argparse.ArgumentParser(
        description=
        'Given a list of domains to label: we do all necessary stuff to apply classifer on the sites and output a csv with the results for monitoring.'
    )
    parser.add_argument('--filter_list_directory',
                        required=True,
                        help='path to find the filter list easylist and anti-cv list')

    args = parser.parse_args()
    print(args)

    global FILTER_LIST_DIR
    FILTER_LIST_DIR = args.filter_list_directory

    app.run(debug=True, port=5000)


if __name__ == '__main__':
    main()
