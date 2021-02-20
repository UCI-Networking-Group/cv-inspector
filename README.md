# CV-Inspector
Given a set of sites, CV-Inspector will automate the crawling, data collection, differential analysis, and labeling of the sites. 

* **Label 1** = the site was able to circumvent the adblocker 
* **Label 0** = the site was not successful at circumventing the adblocker or it did not attempt at circumvention

CV-Inspector was developed and used in the paper: **CV-Inspector: Towards Automating Detection of Adblock Circumvention**.

We refer to the paper for more details.

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

# Amazon Machine Image (AMI)
For **quick use**, you can use our AMI that has CV-Inspector set up already using Ubuntu 18.04.3 LTS.

* [Request access to the AMI by filling out the form](https://https://athinagroup.eng.uci.edu/projects/cv-inspector/ami/)
    * You must have an existing AWS account. We will share the AMI directly with your account.
* See the `README_AMI.md` For the AMI.

# Setting up CV-Inspector Yourself

If you want to set up your own environment, see the `README_selfsetup.md`. 

# Dependencies
- [CV-Inspector Adblock Plus Chrome Extension](https://github.com/levanhieu-git/cv-inspector-adblockpluschrome): A custom version of Adblock Plus Chrome extension to annotate the page source
- `npm`: To build chrome extensions
- `mongodb`: To save intermediate data collected
- `chromedriver78`: The ChromeDriver for Selenium (version 78)
- `Python 3.6+`: CV-Inspector is built on top on Python 3.6
- `setup.py`: List of Python packages

# License
CV-Inspector is licensed under [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgements
- The original DOM Chrome Extension was provided by the authors of [Detecting Anti-Adblockers in the Wild](https://content.sciendo.com/view/journals/popets/2017/3/article-p130.xml). We modify the extension for CV-Inspector.
