/*
 * Copyright (c) 2021 Hieu Le and the UCI Networking Group
 * <https://athinagroup.eng.uci.edu>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var fs = require('fs');
var path = require('path');
const {AdBlockClient, FilterOptions} = require('ad-block')

const args = process.argv;
console.log(args);

if (args.length != 5) {
    console.log("Needs to pass in 4 things, abs path to control file, abs path to variant file, output path, output filename");
    return;
}
const filter_lists_file_path_index = 2;
const variant_urls_file_index = 3;
const output_path_index = 4;

// filter list sep by comma
var filter_lists_path = args[filter_lists_file_path_index];
var filter_lists_files = filter_lists_path.split(",")

var variant_urls_file = args[variant_urls_file_index];
var output_file_path = args[output_path_index];

const client = new AdBlockClient()

const urls_to_check = fs.readFileSync(variant_urls_file, "utf-8").split('\n');

// load the rules
for (var index in  filter_lists_files) {
    var filter_list_file_path = filter_lists_files[index]
    if (filter_list_file_path.length > 0) {
        var filter_list = fs.readFileSync(filter_list_file_path, "utf-8");
        console.log("Loading filter list " + filter_list_file_path)
        client.parse(filter_list)
    }
}

// open output file:
const result_writer = fs.createWriteStream(output_file_path, {
    flags: 'a' // 'a' means appending (old data will be preserved)
})
  

for (const url of urls_to_check) {
    let url_split = url.split(";;")
    if (url_split.length == 4) {
        let crawl_url = url_split[0]
        let main_domain = url_split[1]
        let target_url = url_split[2]
        let resource_type = url_split[3]
        let filter_option = FilterOptions.noFilterOption
        try {
            if (resource_type == "None") {
                filter_option = FilterOptions.noFilterOption
            } else if (resource_type == "sub_frame") {
                filter_option = FilterOptions.subdocument
            } else {
                filter_option = FilterOptions[resource_type]
            }
        } catch (ex) {
            //console.log("Could not find filter option for " + resource_type)
            filter_option = FilterOptions.noFilterOption
        }
        //console.log('url: ', target_url, ", main domain :", main_domain, ", resource : ", resource_type, " , fileoption: ", filter_option)

        var b1 = client.matches(target_url, filter_option, main_domain)
        //console.log('Match result: ', b1, " url :", target_url)
        if (b1) {
            //console.log('Match result: ', b1, " url :", target_url)
            result_writer.write(url+"\n")
        }
    }
}

result_writer.end()