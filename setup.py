# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
from setuptools import setup, find_packages

import sys
sys.path.insert(0, 'dataworkspaces')
from dataworkspaces import __version__

with open("README.rst", "r") as f:
    long_description = f.read()
setup(
    name='dataworkspaces',
    version=__version__,
    author="Max Plack Institute for Software Systems, Data-ken Research",
    author_email='jeff@data-ken.org',
    description="Easy management of source data, intermediate data, and results for data science projects",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/data-workspaces/data-workspaces-core",
    packages=find_packages(),
    #py_modules=['dataworkspaces'],
    install_requires=[
        'click',
        'requests'
    ],
    entry_points="""
        [console_scripts]
        dws=dataworkspaces.__main__:main
        git-fat=dataworkspaces.third_party.git_fat:main
    """,
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: Linux",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows :: Windows 10",
        "Topic :: Software Development :: Version Control",
        "Topic :: Scientific/Engineering :: Information Analysis"
    ]
)
