from setuptools import setup, find_packages

print(find_packages(where='src'))

setup(
    name='kf-lib-data-ingest',
    version='0.1',
    description='Kids First Data Ingest Libraries',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True
)
