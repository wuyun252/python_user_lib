# coding: utf-8
from setuptools import setup, find_packages


setup(
    name='pyslogger',

    version='2.5.0',

    description='pyslogger',

    packages=find_packages(),

    install_requires=[
        'tornado>=4',
        'pytof>=2.0.2',
        'pycdnutil>=0.3.0',
        'pycl5>=1.3.0',
        'psutil',
    ]
)
