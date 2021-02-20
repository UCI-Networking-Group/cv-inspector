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

from cvinspector.common.utils import ABP_BLOCKED_SNIPPET, get_css_dict

logger = logging.getLogger(__name__)
# logger.setLevel("DEBUG")

ID_TAG = "id"
TEXT_NODE_TYPE = 3
NODES_ADDED = "nodes added"
NODES_REMOVED = "nodes removed"
ATTRIBUTE_CHANGED = "attribute changed"
TEXT_CHANGED = "text changed"
DOM_CONTENT_LOADED = "DOMContentLoaded"


# returns the node name of the added node (div, iframe, etc)
def get_nodes_added__node_name(event, event_index):
    if event.get("nodeInfo") and len(event.get("nodeInfo")) > event_index:
        # node info is embedded in a list of list
        node_info_obj = event.get("nodeInfo")[event_index][0]
        if node_info_obj.get("nodeName"):
            return node_info_obj.get("nodeName")
    return ""


# given a string like "DIV.hello.there"
# returns: "55" (ignoring the main DIV)
# Since css_string_split can be very large, we truncate it to only max_css_classes
def _get_class_count(css_string_split, max_css_classes=20):
    ignore_first_index = 1
    if len(css_string_split) > 0:
        class_count_str = ""
        for css_class in css_string_split[ignore_first_index:max_css_classes]:
            class_count_str += str(len(css_class))
        return class_count_str

    return "0"


# given a string like "DIV.hello.there"
# returns: "DIV55"
def _get_first_in_css(css_string, include_class_name_len=True):
    if "." in css_string:
        css_string_split = css_string.split(".")
        if len(css_string_split) > 0:
            if include_class_name_len:
                return css_string_split[0] + _get_class_count(css_string_split)
            else:
                return css_string_split[0]

    return css_string


def split_immediate_selectors(selector, delimiter=" > "):
    selector_split = selector.split(delimiter)
    return selector_split


# simplify selector to just tags:
# HTML.a-ws.a-js. > span.something > HEAD to HTML > span > HEAD
def simplify_target_selector(target_selector):
    delimiter = " > "

    target_selector = target_selector.strip()
    split_items = []

    if delimiter in target_selector:
        node_selector_split = target_selector.split(delimiter)
        for split_it in node_selector_split:
            node_simple = _get_first_in_css(split_it)
            # turn a real id into just the name id
            if "#" in node_simple:
                node_simple = ID_TAG + str(len(node_simple))
            split_items.append(node_simple)
    else:
        node_simple = _get_first_in_css(target_selector)
        if "#" in node_simple:
            node_simple = ID_TAG + str(len(node_simple))
        split_items.append(node_simple)

    simple_target_selector = delimiter.join(split_items)
    return split_items, simple_target_selector


# nodeInfo usually has just element tag for parentNode
# otherwise, parent selector is more of css selector: HTML.a-ws.a-js. > span.something > HEAD
def get_parent_node(parent_node,
                    parent_selector_backup,
                    simplify_selectors=False):
    parent_selector = parent_selector_backup

    if parent_node is not None:
        parent_selector = parent_node

    if parent_selector:
        node_selector_parent_orig = parent_selector
        if simplify_selectors:
            simplify_targets, _ = simplify_target_selector(
                node_selector_parent_orig)
        else:
            simplify_targets = split_immediate_selectors(
                node_selector_parent_orig)
        # here we only get the last in the list (parent)
        node_selector_parent = simplify_targets[-1]

        return node_selector_parent.strip().lower()

    # default to emptystring
    return ""


def get_node_info_str(
        node_info_obj,
        use_attribute_values=False,
        NODE_ATTR_VAL_THRESHOLD=50,
        NODE_STYLE_KEY_THRESHOLD=10,
        ignore_attrs=["transform", "d", "x", "x1", "x2", "y", "y1", "y2",
                      "r"]):
    node_info = ""
    # add in node info object information
    if node_info_obj.get("nodeName"):
        node_info += "_NodeName" + node_info_obj.get("nodeName") + "_"
    if node_info_obj.get("NoChildNodes"):
        node_info += "_ChildCount" + str(
            node_info_obj.get("NoChildNodes")) + "_"
    if node_info_obj.get("NodeValue"):
        node_info += "_NodeValueLen" + str(len(
            node_info_obj.get("NodeValue"))) + "_"
    if node_info_obj.get("NodeType"):
        node_info += "_NodeType" + str(node_info_obj.get("NodeType")) + "_"

    # add node info attributes
    if node_info_obj.get("NodesAttributes") and len(
            node_info_obj.get("NodesAttributes")) > 0:
        for node_attr in node_info_obj.get("NodesAttributes"):
            if len(node_attr) == 2:
                node_attr_key = node_attr[0]
                node_attr_val = node_attr[1]
                if not use_attribute_values:
                    if len(node_attr_key) <= 2:
                        continue
                    node_info += node_attr_key
                    if node_attr_key == "style":
                        # for style, make the string into the style key only like:
                        # style[height][width]
                        for index, style_key in enumerate(
                                get_css_dict(node_attr_val), start=0):
                            if index > NODE_STYLE_KEY_THRESHOLD:
                                break
                            node_info += "[" + style_key + "]"

                    elif node_attr_key not in ignore_attrs:
                        node_info += str(len(node_attr_val))

                else:
                    if "doctype" in node_attr_val:
                        continue
                    node_info += node_attr_key + node_attr_val[:
                                                               NODE_ATTR_VAL_THRESHOLD]

    if len(node_info) == 0:
        node_info = "ninfonull"

    return node_info


def get_node_single_added_key(event_target,
                              event_node_selector,
                              event_node_info,
                              use_attribute_values=False,
                              use_target_selector=True,
                              NODE_ATTR_VAL_THRESHOLD=50,
                              delimiter="__",
                              simplify_selectors=False):
    is_text_node = False
    defining_text = ""
    key = ""
    node_selector = ""
    if simplify_selectors:
        _, target_selector = simplify_target_selector(
            event_target.get("selector"))
    else:
        target_selector = event_target.get("selector")

    has_snippet_blocked = ABP_BLOCKED_SNIPPET in event_node_selector.get(
        "selector")
    if simplify_selectors:
        _, simple_node_selector_str = simplify_target_selector(
            event_node_selector.get("selector"))
    else:
        simple_node_selector_str = event_node_selector.get("selector")

    node_selector += simple_node_selector_str

    node_info = ""
    node_info_obj = None
    parent_node_obj = None
    # node info is an object in encapsulated in a list
    if event_node_info and len(event_node_info) > 0:
        # node info is embedded in a list of list
        node_info_obj = event_node_info[0]
        if node_info_obj.get("NodeType") == TEXT_NODE_TYPE:
            is_text_node = True

        parent_node_obj = node_info_obj.get("parentNode")

        node_info = get_node_info_str(
            node_info_obj,
            use_attribute_values=use_attribute_values,
            NODE_ATTR_VAL_THRESHOLD=NODE_ATTR_VAL_THRESHOLD)

    parent_node_str = ""
    if parent_node_obj is not None:
        # here we can use target_selector because it is the parent selector for add/removed nodes
        parent_node_str = get_parent_node(target_selector, parent_node_obj)

    # set to be nsnull
    if len(node_selector) == 0:
        node_selector = "nsnull"

    if use_target_selector:
        key = target_selector + delimiter + node_selector + delimiter + parent_node_str + delimiter + node_info
    else:
        key = node_selector + delimiter + parent_node_str + delimiter + node_info

    return key.lower(), defining_text, is_text_node, has_snippet_blocked


# - Important: For "nodes added and nodes removed" the target node is the one that will have CHILDREN added to or removed from.
#              It is NOT the elements being added or removed
# - target_selector IS the parent node selector
# - Event is a mongo event object
# - Using target selector would make the key more specific since it is a selector on the parent
# - use_attribute_values = True would make things more specific since we will use values as well
"""
{"type":"event","event":{"type":"nodes added",
"target":{"selector":"HTML.a-ws.a-js. > HEAD","nodeId":2},
"nodes":[{"selector":"SCRIPT","nodeId":2477}], --> IMPORTANT: this can have multiple nodes (that maps to nodeInfo)
"nodeInfo":[[{"id":"","NoChildNodes":0,"NodeType":1,"NodeValue":null,
    "nodeName":"SCRIPT","localName":"script","namespaceURI":"http://www.w3.org/1999/xhtml","parentNode":"head","
    NodesAttributes":[["async",""],["crossorigin","anonymous"],
        ["src","https://m.media-amazon.com/images/I/A1nE6gseivL.js"],
        ["type","text/javascript"]]}]]},"time":1586327346839},
"""


def get_nodes_added_key(event,
                        use_attribute_values=False,
                        use_target_selector=True,
                        NODE_ATTR_VAL_THRESHOLD=50):
    event_target = event.get("target")
    result_keys = []
    if event.get("nodes") and len(event.get("nodes")) > 0:
        # for selector we should account for multiple changes
        for index, event_node in enumerate(event.get("nodes"), start=0):
            if event.get("nodeInfo") is not None and len(
                    event.get("nodeInfo")) > index:
                result_key = get_node_single_added_key(
                    event_target,
                    event_node,
                    event.get("nodeInfo")[index],
                    use_attribute_values=use_attribute_values,
                    use_target_selector=use_target_selector,
                    NODE_ATTR_VAL_THRESHOLD=NODE_ATTR_VAL_THRESHOLD)

                result_keys.append(result_key)

    return result_keys


"""
{"type":"event","event":{"type":"nodes removed",
"target":{"selector":"BODY.a-m-us.a-aui_72554-c.","nodeId":93},
"nodes":[{"selector":"A","nodeId":2478}],
"nodeInfo":[[{"id":"","NoChildNodes":0,"NodeType":1,"NodeValue":null,"nodeName":"A","localName":
    "a","namespaceURI":"http://www.w3.org/1999/xhtml","parentNode":"","NodesAttributes":[]}]]},"time":1586327346839}
"""


def get_nodes_removed_key(event,
                          use_attribute_values=False,
                          use_target_selector=True):
    return get_nodes_added_key(event,
                               use_attribute_values=use_attribute_values,
                               use_target_selector=use_target_selector)


def is_float(some_str):
    result = False
    try:
        split_str = some_str.split(",")
        for temp_str in split_str:
            float(temp_str)
        result = True
    except:
        pass
    return result


def get_attribute_changed_info(event):
    attribute = event.get("attribute")
    old_val = ""
    if event.get("oldValue") is not None:
        old_val = event.get("oldValue")
    new_val = ""
    if event.get("newValue") is not None:
        new_val = event.get("newValue")

    return attribute, old_val, new_val


"""
{"type":"event","event":{"type":"attribute changed","target":{"selector":"#desktop-btf-grid-8","nodeId":1446},"targetType":"DIV",
 "parentNode":"#main-content","attribute":"data-csa-c-id","oldValue":null,"newValue":"vmoq6g-95h50q-27hh9r-kat2cl",
 "recd":[{"id":"desktop-btf-grid-8","NoChildNodes":3,"NodeType":1,"NodeValue":null,"nodeName":"DIV","localName":"div",
     "namespaceURI":"http://www.w3.org/1999/xhtml","parentNode":"div",
     "NodesAttributes":[["class","gw-col celwidget csm-placement-id-4854fd64-5da9-4368-8d0e-c874d4654e30 c6a0889d-1ed6-4ba5-94f4-50506d05a9d5"],["data-csa-c-id","vmoq6g-95h50q-27hh9r-kat2cl"],
         ["data-display-at","ws"],["data-gwi","{\"visible\":\"desktop-btf-grid-8-visible\",\"active\":\"desktop-btf-grid-8-active\"}"],["data-order-ws","9"],
         ["data-pf_rd_p","4854fd64-5da9-4368-8d0e-c874d4654e30"],["id","desktop-btf-grid-8"]]}]},"time":1586327346799}
"""


def get_attribute_changed_key(event,
                              truncate_length=15,
                              delimiter="__",
                              use_attribute_values=False,
                              use_target_selector=True,
                              use_compare_values=False,
                              NODE_ATTR_VAL_THRESHOLD=50,
                              simplify_selectors=False):
    # returns a key of the event
    defining_text = ""
    key = ""
    if simplify_selectors:
        _, target_selector = simplify_target_selector(
            event.get("target").get("selector"))
    else:
        target_selector = event.get("target").get("selector")

    attribute = event.get("attribute")

    # change this to generic data attribute
    if "data" in attribute:
        attribute = "data"

    target_type = event.get("targetType")

    defining_text = attribute

    # truncate value to 15 characters max
    # get value info
    old_value = "null"
    if event.get("oldValue") is not None:
        old_value = event.get("oldValue")[:truncate_length]

    new_value = "null"
    if event.get("newValue") is not None:
        new_value = event.get("newValue")[:truncate_length]
        defining_text += delimiter + event.get("newValue")
    else:
        defining_text += delimiter + "null"

    # get target info (the information about the element that is changing)
    node_info_str = ""
    parent_node = None
    node_info_obj = None
    if event.get("recd") and len(event.get("recd")) > 0:
        if len(event.get("recd")) > 1:
            logger.warning("WARNING: there are recd larger than 1: %s" %
                           str(event.get("recd")))

        # recd is diff from node_info from node added function. It is just a list with one element
        node_info_obj = event.get("recd")[0]

        # get parent info
        parent_node = node_info_obj.get("parentNode")

        # get child count
        node_info_str = get_node_info_str(
            node_info_obj,
            use_attribute_values=use_attribute_values,
            NODE_ATTR_VAL_THRESHOLD=NODE_ATTR_VAL_THRESHOLD)

    parent_node_str = ""
    if parent_node is None or len(parent_node) == 0:
        # fall back on parentNode of main event
        parent_node_str = get_parent_node(parent_node, None)

    if use_target_selector:
        key = attribute + delimiter + target_selector + delimiter + target_type + delimiter + parent_node_str + delimiter + node_info_str
    else:
        key = attribute + delimiter + target_type + delimiter + parent_node_str + delimiter + node_info_str

    if use_compare_values:
        # ignore floats
        ignore_attrs = ["transform", "translate"]
        if is_float(
                old_value) or attribute in ignore_attrs or len(attribute) <= 2:
            old_value = ""
        if is_float(
                new_value) or attribute in ignore_attrs or len(attribute) <= 2:
            new_value = ""
        key += delimiter + "oldvalue" + old_value + delimiter + "newvalue" + new_value

    return key.lower(), defining_text


"""
{"type":"event","event":{"type":"text changed",
"target":{"selector":"DIV.title > (text)","nodeId":2762},"newValue"
:"Get The Facts ","oldValue":"{{window['cXTmplMgckiyde82k8rtu6yf10']}}"},"time":1586380244930},
"""


# has no parent node except for selector
def get_text_changed_key(event,
                         truncate_length=15,
                         delimiter="__",
                         simplify_selectors=False):
    # returns a key of the event
    key = ""

    if simplify_selectors:
        _, target_selector = simplify_target_selector(
            event.get("target").get("selector"))
    else:
        target_selector = event.get("target").get("selector")

    # truncate value to 15 characters max
    old_value_key = "null"
    old_value = ""
    if event.get("oldValue") is not None:
        old_value_key = event.get("oldValue")[:truncate_length]
        old_value = event.get("oldValue")

    new_value_key = "null"
    new_value = ""
    if event.get("newValue") is not None:
        new_value_key = event.get("newValue")[:truncate_length]
        new_value = event.get("newValue")

    text_diff = ""
    # split string using spaces and make it into a set of words
    old_value_split = set(old_value.split())
    new_value_split = set(new_value.split())

    # if old is zero, then return the entire new_value string
    # if new is old, then return the entire old_value
    # else get the difference
    if len(old_value_split) == 0 and len(new_value_split) > 0:
        text_diff = new_value
    elif len(new_value_split) == 0 and len(old_value_split) > 0:
        text_diff = old_value
    else:
        # get diff and then join them again
        new_value_only = new_value_split.difference(old_value_split)
        text_diff = " ".join(new_value_only)

    key = target_selector + delimiter + "oldvalue" + old_value_key + delimiter + "newvalue" + new_value_key
    return key.lower(), text_diff
