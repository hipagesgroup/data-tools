.. hip-data-tools documentation master file, created by
   sphinx-quickstart on Mon May 18 07:55:25 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to hip-data-tools's documentation!
==========================================

© Hipages Group Pty Ltd 2019

`PyPI version <https://pypi.org/project/hip-data-tools/#history>`__
`CircleCI <https://circleci.com/gh/hipagesgroup/data-tools/tree/master>`__

Common Python tools and utilities for data engineering, ETL,
Exploration, etc. The package is uploaded to PyPi for easy drop and use
in various environments, such as (but not limited to):

1. Production workloads
2. Python notebooks
3. Local dev and exploratory analysis

Installation
------------

Install from PyPi repo:

.. code:: bash

   pip3 install hip-data-tools

Install from source


The hip-data-tools package is structured to be used in three different modalities:
1. Use base level Utility classes to interact with services (Modules with the names of their service vendors, like aws, google)
2. Use pre compiled scripts and modules that can be executed from command line (the hipages module)
3. Higher level etl classes, that allow you to build complex data transformations (the etl module)


.. toctree::
   :maxdepth: 2
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
