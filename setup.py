from setuptools import setup
setup(name='cv-inspector',
    version=1.0,
    description='CV-Inspector',
    author='Hieu Le',
    author_email='hieul@uci.edu',
    packages=['cvinspector'],
    python_requires='>=3.6',
    install_requires=[
          'idna==2.9',
          'trio',
          'selenium',
          'pymongo',
          'tldextract',
          'pyvirtualdisplay',
          'pandas',
          'textdistance',
          'bs4',
          'scikit-learn>=0.23.1, <0.24',
          'Naked',
          'flask'
      ],
    entry_points={'console_scripts': [
        'cvinspector_monitor = cvinspector.scripts.cvinspector_monitor:main',
        'cvinspector_buildextensions = cvinspector.scripts.build_chrome_extensions:main',
        'cvinspector_abp_proxy = cvinspector.scripts.subscription_proxy:main',
        'cvinspector_check_chrome_profile = cvinspector.scripts.check_chrome_profile:main',
        'cvinspector_create_chrome_profiles = cvinspector.scripts.create_chrome_profiles:main'

    ]}
)
