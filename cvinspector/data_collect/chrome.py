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

import errno
import logging
import os
import shutil
import time

from selenium import webdriver

from cvinspector.common.utils import randomword


logger = logging.getLogger(__name__)
#logger.setLevel("DEBUG")


def _get_common_driver_options(
    chrome_default_download_directory=None,
    allow_running_insecure_content=True,
):
    # build options
    chrome_opt = webdriver.ChromeOptions()
    chrome_opt.add_argument("--window-size=1920,1080")
    chrome_opt.add_argument('--disable-application-cache')
    chrome_opt.add_argument('--disable-infobars')

    # trying no sandbox
    chrome_opt.add_argument('--no-sandbox')

    # Allow loading of unsafe script content
    if allow_running_insecure_content:
        chrome_opt.add_argument("--allow-running-insecure-content")
        chrome_opt.add_argument("--ignore-certificate-errors")

    if chrome_default_download_directory:
        chrome_opt.add_experimental_option(
            'prefs', {
                'download.default_directory':
                chrome_default_download_directory,
                'download.prompt_for_download': False,
            })

    return chrome_opt


def create_variant_driver(
        chrome_driver_path=None,
        chrome_ext_path=None,
        chrome_default_download_directory=None,
        profile_path=None,
        allow_running_insecure_content=True,
        include_custom_extensions=True,
        **kwargs):

    chrome_opt = _get_common_driver_options(
        chrome_default_download_directory=chrome_default_download_directory,
        allow_running_insecure_content=allow_running_insecure_content)

    # load custom extension ABP, Web requests, DOM Mutations
    if include_custom_extensions:
        custom_exts = chrome_ext_path + ",chromeext/circum_webrequests/build,chromeext/circum_dom_mutation/build"
        chrome_opt.add_argument("--load-extension=" + custom_exts)

    # Load profile
    if profile_path:
        logger.debug("Loading driver with profile: " + profile_path)
        chrome_opt.add_argument("--user-data-dir=" + profile_path)
    else:
        logger.debug("Loading driver with NO profile")

    variant_driver = webdriver.Chrome(executable_path=chrome_driver_path,
                                      options=chrome_opt)
    return 'variant', variant_driver


# control has no abp extension
def create_control_driver(chrome_driver_path=None,
                          chrome_default_download_directory=None,
                          allow_running_insecure_content=True,
                          profile_path=None,
                          include_custom_extensions=True,
                          **kwargs):

    chrome_opt = _get_common_driver_options(
        chrome_default_download_directory=chrome_default_download_directory,
        allow_running_insecure_content=allow_running_insecure_content)

    # load custom extension for webrequests collection
    if include_custom_extensions:
        chrome_opt.add_argument(
            "--load-extension=chromeext/vanilla_webrequests/build,chromeext/vanilla_dom_mutation/build"
        )

    # Load profile
    if profile_path:
        logger.debug("Loading driver with profile: " + profile_path)
        chrome_opt.add_argument("--user-data-dir=" + profile_path)
    else:
        logger.debug("Loading driver with NO profile")

    control_driver = webdriver.Chrome(executable_path=chrome_driver_path,
                                      options=chrome_opt)
    return 'control', control_driver


def quit_drivers(drivers):
    logger.debug("stopping drivers")
    if drivers:
        for _, driver in drivers:
            driver.close()
            time.sleep(1)
            driver.quit()


def create_new_profile(starting_profile_path,
                       profile_directory,
                       new_profile_prefix=None):
    dest_profile_name = randomword(10)
    if new_profile_prefix:
        dest_profile_name = new_profile_prefix + dest_profile_name

    dest_profile_path = profile_directory + dest_profile_name + os.sep
    logger.debug("Creating new profile " + dest_profile_name +
                 " FROM original profile " + starting_profile_path)

    try:
        shutil.copytree(starting_profile_path, dest_profile_path)
        return dest_profile_path
    except OSError as e:
        # If the error was caused because the source wasn't a directory
        if e.errno == errno.ENOTDIR:
            shutil.copy(starting_profile_path, dest_profile_path)
        else:
            logger.warning('Directory not copied. Error: %s' % e)


def get_scroll_width_and_height(driver):
    required_width = driver.execute_script(
        'return document.body.parentNode.scrollWidth')
    required_height = driver.execute_script(
        'return document.body.parentNode.scrollHeight')

    required_width_body = driver.execute_script(
        'return document.body.scrollWidth')
    required_height_body = driver.execute_script(
        'return document.body.scrollHeight')

    if required_width_body > required_width:
        required_width = required_width_body
    if required_height_body > required_height:
        required_height = required_height_body

    if required_height == 0:
        logger.debug(
            "required height is zero, trying to find height from body childNodes"
        )
        child_count = driver.execute_script(
            'return document.body.childNodes.length')
        logger.debug("body childNodes count %s" % str(child_count))
        if child_count and child_count > 0:
            required_height = driver.execute_script(
                'return document.body.childNodes[0].scrollHeight')

    return required_width, required_height


def save_screenshot_headless(driver,
                             screenshot_path,
                             thread_name=None,
                             max_height=3000,
                             max_width=3000):
    original_size = driver.get_window_size()
    required_width, required_height = get_scroll_width_and_height(driver)

    if required_height == 0:
        required_height = max_height
    if required_height > max_height:
        required_height = max_height

    if required_width == 0:
        required_width = max_width
    if required_width > max_width:
        required_width = max_width

    logger.debug(
        "%s - Screenshot: Setting browser window to be width %d, height %d" %
        (str(thread_name), required_width, required_height))
    driver.set_window_size(required_width, required_height)

    screenshot_success = False
    try:
        # avoids scrollbar
        driver.find_element_by_tag_name('body').screenshot(
            screenshot_path) 
        screenshot_success = True
    except Exception as e:
        logger.debug(e)
        logger.debug("%s - Could not take screenshot from body" %
                     str(thread_name))
        try:
            child_count = driver.execute_script(
                'return document.body.childNodes.length')
            if child_count and child_count > 0:
                child_node = driver.execute_script(
                    'return document.body.childNodes[0]')
                if child_node:
                    logger.debug(
                        "%s - Screenshot: take screenshot of child node" %
                        str(thread_name))
                    # avoids scrollbar
                    child_node.screenshot(screenshot_path)  
                    screenshot_success = True
        except Exception as e:
            logger.debug(e)

    if not screenshot_success:
        logger.warning(
            "%s - Screenshot: Could not take screenshot overall for %s" %
            (str(thread_name), driver.current_url))

    driver.set_window_size(original_size['width'], original_size['height'])


def get_id_of_unpacked_chrome_extension(ext_abs_path):
    import hashlib

    m = hashlib.sha256()
    m.update(bytes(ext_abs_path.encode('utf-8')))
    return ''.join([chr(int(i, base=16) + ord('a')) for i in m.hexdigest()][:32])


def update_filter_list_adblock_plus_through_options(drv, ext_abs_path):
    # unique ID of extension taken from the chrome extension store
    # https://chrome.google.com/webstore/detail/adblock-plus-free-ad-bloc/cfhdojbkjhnklbpkdaibdccddilifddb

    # for unpacked extensions, extid are generated based on the absolute path of the extension
    ext_id = get_id_of_unpacked_chrome_extension(ext_abs_path)
    drv.get("chrome-extension://" + ext_id + "/options.html")
    logger.debug("extension is unpacked %s" % ext_id)

    while drv.title == "":
        time.sleep(1)

    if "Adblock Plus" not in drv.title:
        logger.debug("driver title: %s" % str(drv.title))
        raise Exception("Failed to install AdBlock Plus!")

    drv.switch_to.frame(0)

    # go to advanced tab
    drv.find_element_by_link_text("Advanced").click()

    for elem in drv.find_elements_by_css_selector("#all-filter-lists-table > li"):
        filter_list_name = elem.get_attribute("aria-label")
        toggle_button = elem.find_element_by_css_selector("io-toggle > button")
        if toggle_button.get_attribute("aria-checked") == "true":
            menu_button = elem.find_element_by_css_selector(".wrapper.icon")
            # we have to rely on JS here because selenium has problems clicking on this element
            drv.execute_script("arguments[0].querySelector('button.update-subscription').click();", menu_button)
            time.sleep(1)
            logger.debug("updating filter list %s" % filter_list_name)

    drv.switch_to.parent_frame()


def setup_adblock_plus_through_options(drv, ext_abs_path,
                                       turn_on_easylist=True,
                                       turn_on_anticv_list=False):

    # unique ID of extension taken from the chrome extension store
    # https://chrome.google.com/webstore/detail/adblock-plus-free-ad-bloc/cfhdojbkjhnklbpkdaibdccddilifddb

    # for unpacked extensions, extid are generated based on the absolute path of the extension
    ext_id = get_id_of_unpacked_chrome_extension(ext_abs_path)
    drv.get("chrome-extension://" + ext_id + "/options.html")
    logger.debug("extension is unpacked %s" % ext_id)

    while drv.title == "":
        time.sleep(1)

    if "Adblock Plus" not in drv.title:
        logger.debug("driver title: %s" % str(drv.title))
        raise Exception("Failed to install AdBlock Plus!")

    drv.switch_to.frame(0)

    # turn off acceptable ads and go to advanced tabs
    drv.find_element_by_id("acceptable-ads-allow").click()
    drv.find_element_by_link_text("Advanced").click()

    for elem in drv.find_elements_by_css_selector("#all-filter-lists-table > li"):
        filter_list_name = elem.get_attribute("aria-label")
        toggle_button = elem.find_element_by_css_selector("io-toggle > button")
        is_toggled_on = toggle_button.get_attribute("aria-checked") == "true"

        if filter_list_name == "EasyList":
            if turn_on_easylist and not is_toggled_on:
                toggle_button.click()
                logger.debug("toggled filter list %s ON" % filter_list_name)
            elif not turn_on_easylist and is_toggled_on:
                toggle_button.click()
                logger.debug("toggled filter list %s OFF" % filter_list_name)
        elif filter_list_name == "ABP filters":
            if turn_on_anticv_list and not is_toggled_on:
                toggle_button.click()
                logger.debug("toggled filter list %s ON" % filter_list_name)
            elif not turn_on_anticv_list and is_toggled_on:
                toggle_button.click()
                logger.debug("toggled filter list %s OFF" % filter_list_name)
        elif is_toggled_on:
            # turn off everything else
            toggle_button.click()
            logger.debug("toggled filter list %s OFF" % filter_list_name)

    drv.switch_to.parent_frame()


def set_all_hidden_imgs_iframes(driver, thread_name=None):

    # we treat pixels as hidden as well.
    inject_js = """
        window.anticv_visible = function(element) {
            var not_visible = !element.offsetParent && element.offsetWidth === 0 && element.offsetHeight === 0;
            if (!not_visible) {
                if (element.offsetWidth <= 2 && element.offsetHeight <= 2) {
                    not_visible = true;
                }
            }  
            if (!not_visible) {
                var styles = window.getComputedStyle(element);
                if (styles.opacity <= 0.1 || styles.display == 'none') {
                    not_visible = true;
                }
            }
            return !not_visible;
        };

        window.anticv_label_onclicks = function(element) {
            var levels = 5;
            var current_parent = element.parentElement;
            while (levels > 0 && current_parent != null) {
                if (current_parent.onclick != null) {
                    current_parent.setAttribute('anticv-onclick', "true"); 
                }
                levels = levels - 1;
                current_parent = current_parent.parentElement;
            }
        };

        var hidden_element_count = 0;
        var all_imgs = document.images;
        for (i = 0; i < all_imgs.length; i++) {
            var img = all_imgs[i];
            var not_hidden = window.anticv_visible(img);
            if (!not_hidden) {
                img.setAttribute('anticv-hidden', true);
                hidden_element_count = hidden_element_count + 1;
            } else {
                if (img.offsetWidth !== undefined) {
                    img.setAttribute('anticv-offsetwidth', img.offsetWidth);
                }
                if (img.offsetHeight !== undefined) {
                    img.setAttribute('anticv-offsetheight', img.offsetHeight);
                }
                window.anticv_label_onclicks(img);
            }

        }
        var all_iframes = document.querySelectorAll('iframe');
        for (i = 0; i < all_iframes.length; i++) {
            var frame = all_iframes[i];
            var not_hidden = window.anticv_visible(frame);
            if (!not_hidden) {
                frame.setAttribute('anticv-hidden', true);
                hidden_element_count = hidden_element_count + 1;
            } else {
                if (frame.offsetWidth !== undefined) {
                    frame.setAttribute('anticv-offsetwidth', frame.offsetWidth);
                }
                if (frame.offsetHeight !== undefined) {
                    frame.setAttribute('anticv-offsetheight', frame.offsetHeight);
                }
            }
        }

        var all_links = document.links;
        for (i = 0; i < all_links.length; i++) {
            var link = all_links[i];
            var not_hidden = window.anticv_visible(link);
            if (!not_hidden) {
                link.setAttribute('anticv-hidden', true);
                hidden_element_count = hidden_element_count + 1;
            } else {
                if (link.offsetWidth !== null && link.offsetHeight !== null &&
                    link.offsetWidth !== undefined && link.offsetHeight !== undefined && 
                    (link.getAttribute("target") != null || link.getAttribute("rel") != null) && 
                    link.offsetWidth>50 && link.offsetHeight>50) {
                    link.setAttribute('anticv-offsetwidth', link.offsetWidth);
                    link.setAttribute('anticv-offsetheight', link.offsetHeight);
                    let linkStyles = window.getComputedStyle(link);
                    if (linkStyles.background != null &&  linkStyles.background.indexOf("url") != -1) {
                        link.setAttribute('anticv-background', linkStyles.background);
                    }
                    window.anticv_label_onclicks(link);
                }
            }
        }

        return hidden_element_count;
        """
    logger.debug("%s - Injecting JS to find hidden elements", str(thread_name))
    hidden_elements_found = driver.execute_script(inject_js)
    logger.debug("%s - Hidden elements found %s", str(thread_name), str(hidden_elements_found))
