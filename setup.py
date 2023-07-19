from setuptools import setup

setup(
   name='Rackroom Connector',
   version='1.0',
   description='Facilitate data transfer between Shopify and Rackroom',
   license="MIT",
   long_description="",
   author='Eleanor Cassady, Ryan Young c.o. CQL',
   author_email='eleanor.cassady@cqlcorp.com',
   url="http://www.cqlcorp.com/",
   packages=['rackroom'],  #same as name
   install_requires=[
        'pysftp',
        'ShopifyAPI',
        'requests',
        'python-csv',
        'paramiko',
        'python-dateutil',
        'xmlformatter'
    ], 
)
