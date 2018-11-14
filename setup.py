import os
from setuptools import setup, find_packages

root_dir = os.path.dirname(os.path.abspath(__file__))
req_file = os.path.join(root_dir, 'requirements.txt')
with open(req_file) as f:
    requirements = f.read().splitlines()

setup(
    name='kf-lib-data-ingest',
    version='0.1',
    description='Kids First Data Ingest CLI',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    entry_points={
        'console_scripts': [
            'kidsfirst=cli:cli',
        ],
    },
    include_package_data=True,
    install_requires=requirements
)
