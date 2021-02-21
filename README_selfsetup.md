# CV-Inspector Self Setup
This provides full instructions on how to [set up CV-Inspector](#setup-overview).  

CV-Inspector was developed and used in the paper: **CV-Inspector: Towards Automating Detection of Adblock Circumvention**.

Visit our [CV-Inspector Project page](https://athinagroup.eng.uci.edu/projects/cv-inspector/) for more information, including datasets that we utilized in the paper.

# Citation
If you create a publication (including web pages, papers published by a third party, and publicly available presentations) using CV-Inspector, please cite the corresponding paper as follows:
```
@inproceedings{le2021cvinspector,
  title={{CV-Inspector: Towards Automating Detection of Adblock Circumvention}},
  author={Le, Hieu and Markopoulou, Athina and Shafiq, Zubair},
  booktitle={The Network and Distributed System Security Symposium (NDSS)},
  url = {https://dx.doi.org/10.14722/ndss.2021.24055},
  doi = {10.14722/ndss.2021.24055},
  year={2021}
}
```

## Contact
We also encourage you to provide us ([athinagroupreleases@gmail.com](mailto:athinagroupreleases@gmail.com)) with a link to your publication. We use this information in reports to our funding agencies.

# Setup Overview

In order to properly run CV-Inspector, your environment must be completely setup. 
This setup was tested using Amazon Machine Image: `Ubuntu Server 18.04 LTS (HVM), SSD Volume Type`

1. Install all basic dependencies 
   1. npm, mongodb, xvfb, Python 3.6+, python-pip, virtualenv
   1. Create a virtual environment for python3: we will call this `cvinspector`
      1. `virtualenv --python=python3 [path_to_your_envs]/cvinspector`
   1. Next see [Install Google-Chrome 78](#install-google-chrome-78)
   1. Next see [Install Chrome Driver 78](#install-chrome-driver-78)
1. [Set up CV-Inspector Adblock Plus Chrome Extension](https://github.com/levanhieu-git/cv-inspector-adblockpluschrome)
   1. Follow instructions from that repo to build the extension
   1. Keep track of where the `devenv.chrome` directory is within that project
1. git clone CV-Inspector
   1. Install CV-Inspector python module: See [Installing CV-Inspector](#installing-cv-inspector)
   1. Build chrome extensions: See [Build CV-Inspector Chrome Extensions](#build-cv-inspector-chrome-extensions)
1. Install the necessary MongoDB collections: See [Setup MongoDB](#setup-mongodb)
1. Setup the chrome profiles you will need. CV-Inspector relies on two main cases: (A) No Adblocker, (B) With Adblocker. Along with those, we also can decide whether to consider the anti-cv list or only the easylist.
   1. See [Setup Chrome Profiles](#setup-chrome-profiles)
1. Install npm packages for ad-block postprocessing.
   1. See [Setup Ad-block Postprocessing](#setup-ad-block-postprocessing)
   
Your environment is now ready. 
Now proceed to [Starting CV-Inspector](#starting-cv-inspector)

## Installing CV-Inspector
1. cd to the root project
1. Activate virtualenv if necessary
   1. `source [path_to_your_envs]/cvinspector/bin/activate`
1. As a development package: `pip install -e .`

### Install Google-Chrome 78
CV-Inspector uses google-chrome 78. To make sure it is compatible, do the following:
1. `wget https://www.slimjet.com/chrome/download-chrome.php?file=files%2F78.0.3904.97%2Fgoogle-chrome-stable_current_amd64.deb -O google-chrome-stable_current_amd64.deb`
   1. Taken from https://www.slimjet.com/chrome/google-chrome-old-version.php
1. `sudo dpkg -i google-chrome-stable_current_amd64.deb`
1. `sudo apt-get -f install`

### Install Chrome Driver 78
1. `wget https://chromedriver.storage.googleapis.com/78.0.3904.105/chromedriver_linux64.zip`
1. `unzip chromedriver_linux64.zip`
1. `mv chromedriver [CV-Inspector]/chromedriver/chromedriver78`

### Build CV-Inspector Chrome Extensions
1. cd to the root project
1. `cvinspector_buildextensions --extension_path chromeext`
1. if this fails, you may need to update your nodejs and npm versions
   1. To update nodejs:
   ```
      sudo npm cache clean -f
      sudo npm install -g n
      sudo n stable
   ```
   1. Then update npm: `sudo npm install -g npm`
   1. See if both are updated by looking at their versions:
   ```
      npm -v
      node -v
   ```

## Setup MongoDB
1. Install MongoDB
1. Create the necessary db and collections:
   1. cd to the `external_scripts/` directory of this project
   1. Open a new terminal and start the client side of mongodb: `mongo`
   1. Create a new db: `use anticircumvention`
   1. Add all necessary collections using script: `load("setup_mongo_collections.js")`
   1. Verify collections are there: `show collections`
   1. You are done: `quit()`
   
## Adblock Plus Proxy
The proxy serves the version of EasyList and Anti-CV list that we want.

1. Start a screen: `screen -S abp_proxy`
1. Activate the `cvinspector` virtual env: `source [path_to_your_envs]/cvinspector/bin/activate`
1. Go to the root of CV-Inspector: `cvinspector_abp_proxy --filter_list_directory filter_lists`

## Setup Chrome Profiles

Automatically create four default profiles. For now, the chrome profiles are hardcoded to be in `chromeprofiles` within CV-Inspector root directory.
First scenario: We need No Adblocker case and With Adblocker case (with EasyList only)
Second scenario: We need No Adblocker case and With Adblocker case (with EasyList + Anti-CV list)

1. Run the Adblock Plus proxy first: See [Adblock Plus Proxy](#adblock-plus-proxy)
1. Then within a **different screen**, activate the `cvinspector` virtualenv
   1. `source [path_to_your_envs]/cvinspector/bin/activate`
1. Run the command:

The below is an example of how you can call `cvinspector_create_chrome_profiles`.
```
   cvinspector_create_chrome_profiles --chrome_driver_path chromedriver/chromedriver78 --chrome_adblockplus_ext_abs_path /home/ubuntu/github/adblockpluschrome/devenv.chrome
```

## Setup Ad-block Postprocessing
Unfortunately, we cannot use nodeJS v14 to build the ad-block package.

To build this AND run CV-Inspector, **you must have nodeJS v10** (assuming our environment is what we described in [Setup Overview](#setup-overview)).

1. Purge what we have before: `sudo apt-get purge nodejs npm`
1. `curl -sL https://deb.nodesource.com/setup_10.x | sudo bash -`
1. `sudo apt-get install -y nodejs`
1. Check that your `npm -v` should be 6 and `node -v` should be 10.
1. Go to the root directory of CV-Inspector
1. `npm install --save fs path ad-block`

# Starting CV-Inspector

Your environment must be setup already. See [Setup Overview](#setup-overview)

**Note 1**: If you already have the screen necessary, then re-use them.
**Note 2**: You must have the node version as described in [Setup Ad-block Postprocessing](#setup-ad-block-postprocessing)

A. We need to start the local proxy that serves the static list of easylist and anti-cv list.

1. Start a new screen : `screen -S abp_proxy`
1. Activate the virtualenv: `source [path_to_your_envs]/cvinspector/bin/activate`
1. Go to root directory of CV-Inspector
1. start the proxy: `cvinspector_abp_proxy --filter_list_directory filter_lists`
1. Detach from the screen: `CTRL+a then press d`

B. Now we can run the script for CV-Inspector

1. Start a new screen : `screen -S cvinspector`
1. Activate the virtualenv: `source [path_to_your_envs]/cvinspector/bin/activate`
1. Go to root directory of CV-Inspector
1. The main script is **cvinspector_monitor**. It has many parameters to pass in, so use `cvinspector_monitor --help` if need be.
1. While the script is running, you can detach from screen if necessary using `CTRL+a then press d`.

An example here: (this example can be used as is to run the example file [misc_data/example_label_input.csv](https://github.com/UCI-Networking-Group/cv-inspector/blob/main/misc_data/example_label_input.csv))

```
cvinspector_monitor --anticv_on False --trials 4 --beyond_landing_pages true --filter_list_paths filter_lists/easyprivacy.txt,filter_lists/disconnectme_abp.txt,filter_lists/getadmiral-domains.txt,filter_lists/antiadblockfilters.txt --classifier_path model/rf_model.sav --classifier_features_file_path model/features.txt --start_index 0 --end_index 2 --sites_csv misc_data/example_label_input.csv --output_directory /home/ubuntu/temp_output/detection_output/example_monitor/ --output_directory_ts /home/ubuntu/temp_output/detection_output/example_monitor_ts/ --output_suffix test_label --chrome_driver_path chromedriver/chromedriver78 --chrome_adblockplus_ext_abs_path /home/ubuntu/github/adblockpluschrome/devenv.chrome --by_rank false --log_level INFO
```

When the script finishes, it will print out where it outputs the last CSV with the label results. Use the `cv_detect` column from the CSV to know whether it predicted `0 = No Circumvention` or `1 = Has Circumvention`

**Important parameters to notice:**
* `--anticv_on`: whether you want CV-Inspector to load the anti-cv list. If false, it will only rely on EasyList
* `--filter_list_paths`: path to filterlists that you want to use to filter out traffic that you DO NOT care about
* `--sites_csv`: the file that you want CV-Inspector to run on. An example is in [misc_data/example_label_input.csv](https://github.com/UCI-Networking-Group/cv-inspector/blob/main/misc_data/example_label_input.csv). Formatting must match that file
* `--start_index` and `--end_index`: How many sites of the given file from `--sites_csv` do you want to crawl? For example, if the csv file has 100 sites and you only want to first test the first 10, then use `--start_index 0 --end_index 10`.
* `--output_directory`: where the output will be
* `--beyond_landing_pages`: if you want it to find a subpage to crawl as well.
* `--beyond_landing_pages_only`: Given a URL, crawl an existing subpage only, while skipping the given URL.
* `--chrome_driver_path`: Path to your chrome driver, this should be in `chromedriver/chromedriver78`
* `--chrome_adblockplus_ext_abs_path`: Path to the CV-Inspector custom adblock plus. See [Setup Overview](#setup-overview)