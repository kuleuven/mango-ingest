from setuptools import setup

setup(
    name='mango_ingest',
    version='0.9.2',
    py_modules=['mango_ingest'],
    install_requires=[
        'Click',
        'rich',
        'python_irodsclient',
        'cachetools',
        'pip',
        'PyYAML',
        'cachetools',
        'watchdog',
    ],
    entry_points={
        'console_scripts': [
            'mango_ingest = mango_ingest:entry_point',
        ],
    },
)