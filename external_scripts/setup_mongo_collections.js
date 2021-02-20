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

var collection_names = [
    "adb_dommutation",
    "adb_webrequests",
    "control_dommutation",
    "control_only",
    "crawl_instance",
    "cv_detection",
    "cvwebrequests_diff_group",
    "dommutation_diff_group",
    "pgsource_diff",
    "vanilla_dommutation",
    "vanilla_webrequests",
    "variant_dommutation",
    "variant_only"
];

// create collections
collection_names.forEach(function(coll_name) {
    db.createCollection(coll_name);
});

// create the indices
db["crawl_instance"].createIndex({"crawl_group_name":1,"file_name":1});
db["vanilla_dommutation"].createIndex({"crawl_group_name":1, "crawl_instance_id":1});
db["vanilla_webrequests"].createIndex({"crawl_group_name":1, "crawl_instance_id":1});
db["adb_dommutation"].createIndex({"crawl_group_name":1, "crawl_instance_id":1});
db["adb_webrequests"].createIndex({"crawl_group_name":1, "crawl_instance_id":1});