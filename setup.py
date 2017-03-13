from setuptools import setup

setup(
    name='sfdc-bulk',
    packages=['sfdc_bulk'],
    version='0.1',
    description='Python client library for SFDC bulk API',
    url='https://github.com/donaldrauscher/sfdc-bulk',
    download_url='https://github.com/donaldrauscher/sfdc-bulk/archive/0.1.tar.gz',
    author='Donald Rauscher',
    author_email='donald.rauscher@gmail.com',
    license='MIT',
    install_requires=[
        'requests',
        'simple_salesforce',
        'pandas',
        'pyyaml'
    ]
)
