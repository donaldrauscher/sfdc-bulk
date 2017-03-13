from setuptools import setup

setup(
    name='sfdc-bulk',
    packages=['sfdc_bulk'],
    version='0.2',
    description='Python client library for SFDC bulk API',
    url='https://github.com/donaldrauscher/sfdc-bulk',
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
