"""Setup script for mango_ingest"""
from setuptools import setup, find_packages

setup(
    name="mango_ingest",
    version="0.9.2",
    packages=find_packages(),
    install_requires=[
        'Click',
        'rich',
        'python_irodsclient',
        'cachetools',
        'PyYAML',
        'watchdog',
    ],
    entry_points={
        'console_scripts': [
            'mango_ingest=mango_ingest.cli:entry_point',
        ],
    },
)
