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

import logging
import os
import pickle

import pandas as pd

from cvinspector.ml.feature_constants import TARGET_COLUMN_NAME, CRAWL_URL_COLUMN_NAME, CHUNK_COLUMN_NAME, \
    BOOLEAN_FEATURES

logger = logging.getLogger(__name__)


def label_dataset(pd_data, clf, threshold=0.5):
    # predict
    y_pred = clf.predict(pd_data)

    # get probability if possible
    y_pred_prob = None
    predict_prob_func = getattr(clf, "predict_proba", "None")
    if callable(predict_prob_func):
        y_pred_prob = predict_prob_func(pd_data)[:, 1]
        y_pred = [1 if x > threshold else 0 for x in y_pred_prob]

    return y_pred, y_pred_prob


def label_dataset_from_saved_clf(unlabel_file_name,
                                 clf_path,
                                 output_filename,
                                 output_directory,
                                 target_column=TARGET_COLUMN_NAME,
                                 threshold=0.5,
                                 scaler_file_path=None,
                                 test_features_only=None):
    # read in clf
    clf = pickle.load(open(clf_path, 'rb'))
    logger.debug("Read in Classifier: %s" % clf)

    # read in input file
    csv_file_name = unlabel_file_name
    # csv_suffix = output_suffix

    header = ""
    with open(csv_file_name, 'r') as csv_file:
        for line in csv_file:
            header = line.replace("\n", "")
            break
    header_names = header.split(",")

    # ignore the index column
    pd_data = pd.read_csv(csv_file_name, index_col=0)
    logger.debug("Unlabel data shape: %s" % str(pd_data.shape))
    # scale the data
    if scaler_file_path and test_features_only:
        scale_features = [
            x for x in test_features_only if x not in BOOLEAN_FEATURES
        ]
        # read in clf
        scaler = pickle.load(open(scaler_file_path, 'rb'))
        logger.info("Read in Scaler: %s" % scaler)
        logger.debug("Scaling features %d : %s" %
                     (len(scale_features), str(scale_features)))
        # transform only
        pd_data[scale_features] = scaler.transform(pd_data[scale_features])
        logger.debug("Done scaling feature")
        logger.debug(pd_data.head)

    # see if we need to drop target column
    original_target_column = None
    if target_column in header_names:
        logger.debug("Dropping target column " + target_column +
                     " from data before labeling")
        original_target_column = pd_data[[target_column]]

        pd_data = pd_data.drop([target_column], axis=1)
        logger.debug(pd_data.head())

    # deal with crawl_url column, required to map back to url
    pd_data_crawl_column = None
    if CRAWL_URL_COLUMN_NAME in pd_data.columns.tolist():
        pd_data_crawl_column = pd_data[[CRAWL_URL_COLUMN_NAME]]

        logger.debug("Dropping crawl url column " + CRAWL_URL_COLUMN_NAME +
                     " from data before labeling")
        pd_data = pd_data.drop([CRAWL_URL_COLUMN_NAME], axis=1)

    logger.debug(pd_data)

    # deal with chunk column, required to map back which chunk the url belongs to
    pd_data_chunk_column = None
    if CHUNK_COLUMN_NAME in pd_data.columns.tolist():
        pd_data_chunk_column = pd_data[[CHUNK_COLUMN_NAME]]

        logger.debug("Dropping chunk column " + CHUNK_COLUMN_NAME +
                     " from data before labeling")
        pd_data = pd_data.drop([CHUNK_COLUMN_NAME], axis=1)

    # label and get predictions
    y_pred, y_pred_prob = label_dataset(pd_data, clf, threshold=threshold)

    # prepare data for output
    kwargs = {TARGET_COLUMN_NAME: y_pred}

    pd_data_with_pred = pd_data.assign(**kwargs)

    # pd_data_with_pred = pd_data.insert(-1, TARGET_COLUMN_NAME, y_pred)
    if y_pred_prob is not None:
        col_name = TARGET_COLUMN_NAME + "_prob"
        kwargs = {col_name: y_pred_prob}
        pd_data_with_pred = pd_data_with_pred.assign(**kwargs)

    # add chunk column back
    if pd_data_chunk_column is not None:
        pd_data_with_pred.insert(0, CHUNK_COLUMN_NAME, pd_data_chunk_column)

    # add crawl_column back
    if pd_data_crawl_column is not None:
        pd_data_with_pred.insert(0, CRAWL_URL_COLUMN_NAME,
                                 pd_data_crawl_column)

    if original_target_column is not None:
        col_name = TARGET_COLUMN_NAME + "_orig"
        kwargs = {col_name: original_target_column}
        pd_data_with_pred = pd_data_with_pred.assign(**kwargs)

    # output to csv
    pd_data_with_pred.to_csv(output_directory + os.sep + output_filename,
                             index=True)
