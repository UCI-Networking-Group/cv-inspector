TARGET_COLUMN_NAME = "cv_detect"
CRAWL_URL_COLUMN_NAME = "crawl_url"
RANK_COLUMN_NAME = "Rank"
CHUNK_COLUMN_NAME = "Chunk"

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

# features that should not be used for scaling
BOOLEAN_FEATURES = [CRAWL_URL_COLUMN_NAME] + [
    "var_1_dom_as_path", "var_1_spec_char_path", "var_3rd_dom_as_path",
    "var_3rd_spec_char_path", "pagesource_var_adblock_keyword",
    "variant_subdomain_length_more5",
    "peak_sync_total_changes_offset_ispositive"
] + [TARGET_COLUMN_NAME]
