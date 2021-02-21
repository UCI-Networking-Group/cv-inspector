# CV-Inspector
This AMI contains the setup for CV-Inspector using `Ubuntu Server 18.04 LTS (HVM), SSD Volume Type`.

Proceed to [Accessing the AMI](#accessing-the-ami) to get details on how to request access to the our CV-Inspector AMI.

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

# Accessing the AMI
* [Request access to the AMI by filling out the form](https://athinagroup.eng.uci.edu/projects/cv-inspector/ami/)
  * You must have an existing AWS account. We will share the AMI directly with your account.
* Use the AMI to launch an EC2 instance. Choose the configuration that is appropriate for your use case. In our paper, we use the `m5.2xlarge` instance.
* Make sure to create your [EC2 key pairs](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair)
* Then [ssh into the launched instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html).
* The default user name should be **ubuntu**.

# Getting Started
* Once you have ssh into the EC2 instance, you can start using CV-Inspector
* First thing is to update the filter lists if you want CV-Inspector to use the latest ones. See [Filter Lists](#filter-lists)
* Prepare a list of sites you want to crawl. The format needs to match [misc_data/example_label_input.csv](https://github.com/UCI-Networking-Group/cv-inspector/blob/main/misc_data/example_label_input.csv)
* Then go to [Starting CV-Inspector](#starting-cv-inspector)

# Filter lists
Currently, the filter list are using the ones in March 2020. (This matches with what we used in the paper). To update the filter list, use `scp` and update the files in `~/github/cv-inspector/filter_lists/` directory. 

When calling `cvinspector_monitor` (described later), it will use either EasyList (if `anticv_on False`) or with the abp-filters-anti-cv.txt (if `anticv_on True`). All other filter lists are passed into `cvinspector_monitor` to filter OUT web requests.

# Starting CV-Inspector
A. We need to start the local proxy that serves the static list of easylist and anti-cv list.

1. Start a new screen : `screen -S abp_proxy`
1. Activate the virtualenv: `source ~/envs/cvinspector/bin/activate`
1. `cd ~/github/cv-inspector/`
1. start the proxy: `cvinspector_abp_proxy --filter_list_directory filter_lists`
1. Detach from the screen: `CTRL+a then press d`

B. Now we can run the script for CV-Inspector

1. Start a new screen : `screen -S cvinspector`
1. Activate the virtualenv: `source ~/envs/cvinspector/bin/activate`
1. `cd ~/github/cv-inspector/`
1. The main script is **cvinspector_monitor**. It has many parameters to pass in, so use `cvinspector_monitor --help` if need be.
1. While the script is running, you can detach from screen if necessary using `CTRL+a then press d`.

An example here: (this example can be used as is to run the example file [misc_data/example_label_input.csv](https://github.com/UCI-Networking-Group/cv-inspector/blob/main/misc_data/example_label_input.csv))

```
cvinspector_monitor --anticv_on False --trials 4 --beyond_landing_pages true --filter_list_paths filter_lists/easyprivacy.txt,filter_lists/disconnectme_abp.txt,filter_lists/getadmiral-domains.txt,filter_lists/antiadblockfilters.txt --classifier_path model/rf_model.sav --classifier_features_file_path model/features.txt --start_index 0 --end_index 2 --sites_csv misc_data/example_label_input.csv --output_directory /home/ubuntu/temp_output/detection_output/example_monitor/ --output_directory_ts /home/ubuntu/temp_output/detection_output/example_monitor_ts/ --output_suffix test_label --chrome_driver_path chromedriver/chromedriver78 --chrome_adblockplus_ext_abs_path /home/ubuntu/github/adblockpluschrome/devenv.chrome --by_rank false --log_level INFO
```

**When the script finishes**, it will print out where it outputs the last CSV with the label results. Use the `cv_detect` column from the CSV to know whether it predicted `0 = No Circumvention` or `1 = Has Circumvention`

**Important parameters to notice:**
* `--anticv_on`: whether you want CV-Inspector to load the anti-cv list. If false, it will only rely on EasyList
* `--filter_list_paths`: path to filterlists that you want to use to filter out traffic that you DO NOT care about
* `--sites_csv`: the file that you want CV-Inspector to run on. An example is in [misc_data/example_label_input.csv](https://github.com/UCI-Networking-Group/cv-inspector/blob/main/misc_data/example_label_input.csv). Formatting must match that file
* `--start_index` and `--end_index`: How many sites of the given file from `--sites_csv` do you want to crawl? For example, if the csv file has 100 sites and you only want to first test the first 10, then use `--start_index 0 --end_index 10`.
* `--output_directory`: where the output will be
* `--beyond_landing_pages`: if you want it to find a subpage to crawl as well.
* `--beyond_landing_pages_only`: Given a URL, crawl an existing subpage only, while skipping the given URL.
* `--chrome_driver_path`: Path to your chrome driver, this should be in `chromedriver/chromedriver78`
* `--chrome_adblockplus_ext_abs_path`: Path to the CV-Inspector custom adblock plus.

# Structure
* `~/github/cv-inspector`: Holds the main code base for CV-Inspector
* `~/github/adblockpluschrome`: Holds the instrumented code of Adblock Plus 3.7
* `~/github/cv-inspector-adblockpluschrome`: Holds the cv-inspector patch to Adblock Plus 3.7
* `~/envs`: Holds the virtualenvs that we created to run CV-Inspector

## MongoDB
Intermediate data is saved in mongodb to be audited, if necessary.
To inspect the mongoDB, go to a terminal within AWS EC2:
* `mongo`
* `use anticircumvention`
* `show collections`

# Misc Problems for AMI
1. When first starting an EC2 instance of AMI, running the `cvinspector_monitor` script may cause this problem:
```
pyvirtualdisplay.abstractdisplay.XStartTimeoutError: No reply from program Xvfb. command:['Xvfb', '-br', '-nolisten', 'tcp', '-screen', '0', '1920x3000x24', '-displayfd', '4']
```

Not sure why this happens, but just run the script a second time and it should work.