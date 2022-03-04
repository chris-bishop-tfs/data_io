"""
Basic setup file for connections library
"""

from setuptools import setup

setup(
    name='data_io',
    # Hard code for now
    version='0.1.2',
    description='Standardize data IO at Thermo Fisher',
    author='Christopher Bishop',
    author_email='chris.bishop@thermofisher.com',
    # Note that this is a string of words separated by whitespace, not a list.
    keywords='jupyter mapreduce nteract pipeline notebook',
    url='https://github.com/chris-bishop-tfs/data_io',
    packages=['data_io']
    # XXX Add requirements
)
