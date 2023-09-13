# README #

### What is this repository for? ###

* This repo should contain all of the necessary code/files to facilitate transforming/exchanging product and order data between Rack Room and Rookie USA.

### How do I get set up? ###

1. **Dependencies**  
	
    Dependencies have been kept to a minimum with as much drawing from the Python3 STDLIB as possible.  
	
	| Library Name          | Description                                            |
	| ----------------------|--------------------------------------------------------|
	| paramiko    	        | SFTP Library and Client                                |
	| ShopifyAPI            | Python Implementation of the Shopify REST admin API    |
	| python-datutil        | Date Manipulation                                      |
    | xmlformatter          | Validation and HF-formatting of output XML             |
    | python_graphql_client | GraphQL Client                                         |  
	
<br/>  

2. **Installation**
    1. Download  
	
        The code resides on Atlassian BitBucket. Please contact the approriate party for access.  
	
        `git clone git@bitbucket.org:cqlcorp/shopify_middleware.git`  
		<br/>


	2. Install dependencies  
	
        `pip install -r requirements.txt`  
		
<br/>  

3. **Configuration**  

    Configuration is contained in <install_dir>/config/config.json and in environment variables, with environment variables being preferable for sensitive information such as usernames and API keys but this is left to the discretion of the client.  
	
	| TMPDIR         | local temporary directory                                |
	| ---------------|----------------------------------------------------------|
	| SFTP_HOST      |  SFTP server hostname                                    |
	| SFTP_USER      |  SFTP Username                                           |
	| SFTP_KEY       |  Path to SFTP user private key                           |
	| SFTP_PATH      |  SFTP Server base directory                              |
	| SHOPIFY_KEY    |  Shopify Custom App API Key                              |
	| SHOPIFY_SECRET |  Shopify Custom App API Secret                           |
	| SHOPIFY_SITE   |  Shopify site hostname (rookieusa-dev eg)                |
	| EXPORT_TAG     |  Tag name attached to an order indicating exportability  |  
	
<br/>  

4. **Execution**  

    Each task is releagated to its own wrapper script to be executed by AppWorx  
	
	| Script Name                     | Script Intent                                                                |
	| --------------------------------|------------------------------------------------------------------------------|
    | pull_orders_from_shopify.py     | Pull tagged orders from Shopify and format into XML as per RackRoom          |
	| update_shopify_inventory.py     | Consume input files and format a Matrixify update file for product inventory |
	| update_shopify_products.py      | Consume input files and format a Matrixify update file for produucts         |  
	| update_shopify_fulfillment.py   | Consume input files and update order fulfillment details                     |  
	

### Who do I talk to? ###

* Rich Kuchera (repo admin, engineer) - [rich.kuchera@cqlcorp.com](mailto:rich.kuchera@cqlcorp.com)
* Eleanor Cassady (engineer) - [eleanor.cassady@cqlcorp.com](mailto:eleanor.cassady@cqlcorp.com)
* Ryan Young (engineer) - [ryan.young@cqlcorp.com](mailto:ryan.young@cqlcorp.com)