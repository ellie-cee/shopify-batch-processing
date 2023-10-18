import argparse
from functools import reduce
import shopify
import os
import sys
import json
import traceback
import time
import xmlformatter
import csv
from datetime import datetime
import rackroom
import requests
from dateutil.parser import parse as parsedate
from python_graphql_client import GraphqlClient

class OrderUpdateConnector(rackroom.base.ConnectorBase):
    def __init__(self):
        super().__init__()

        ap = argparse.ArgumentParser()
        ap.add_argument("-p","--path",help="Input Files Path")
        self.opts = vars(ap.parse_args())

        session = shopify.Session(f"{self.config('SHOPIFY_SITE')}.myshopify.com/admin","2023-04",self.config("SHOPIFY_SECRET"))
        shopify.ShopifyResource.activate_session(session)
        
        self.filename = self.tmpfile("Orders-Update","csv")
        self.files = []
    def statefile(self):
        return "orderupdates"
    def fields(self):
        return ["ID","Command","Tags Command","Tags","Metafield: custom.order_number [single_line_text_field]"]
    def extract(self):
        self.orders=[]
        
        try:
            for file in os.scandir(self.opts["path"]):
                if file.is_file() and file.name.startswith("order_updates"):
                    self.files.append(file)
                    reader = csv.DictReader(open(file.path),quotechar='"',delimiter=',')
                    for row in reader:
                        self.orders.append(row)
            if len(self.files)<1:
                self.exit("No files to process.")
        except Exception as e:
            self.fatal(f"Unable to fetch orders: {str(e)} ")
         
        self.info(f"Running {self.statefile()} extracted {len(self.orders)} order updates")
        return self
    def transform(self):
        super().transform()
        outfile = open(self.filename,"w")
        writer = csv.DictWriter(
            outfile,
            delimiter=',',
            quotechar='"',
            fieldnames=self.fields()
        )
        writer.writeheader()
        
        for order in self.orders:
            order_rec = shopify.Order.find(order['OrderCode'])
            tags = []
            if len(order_rec.tags):
                tags = order_rec.tags.split(",")
            tags.append(f"RROrderId:{order['OrderNumber']}")

            writer.writerow({
                "ID":order["OrderCode"],
                "Command":"UPDATE",
                "Tags Command":"MERGE",
                "Tags":",".join(tags),
                "Metafield: custom.order_number [single_line_text_field]":order["OrderNumber"]
            })
        outfile.close();
        return self
    
    def load(self):
        self.sftp_put(self.filename,f"to_Shopify/Orders-Update.csv")
        return self
    def cleanup(self):
        if self.config("purge")=="yes":
            for file in self.files:
                os.remove(file.path)
            os.remove(self.filename)