from setuptools import setup, find_packages

setup(
    name='kf-lib-data-ingest',
    version='0.1',
    description='Kids First Data Ingest CLI',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    entry_points='''
        [console_scripts]
        kidsfirst=cli:cli
    ''',
    include_package_data=True,
    install_requires=[
        'Click',
    ]
)
