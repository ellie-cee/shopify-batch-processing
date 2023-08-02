import csv
import os
import sys
import json
import time
import shopify
from python_graphql_client import GraphqlClient

from datetime import datetime
from dateutil.parser import parse as parsedate
import rackroom

class FulfillmentConnector(rackroom.base.ConnectorBase):
    def __init__(self):
        super().__init__()
        shopify.ShopifyResource.set_site(
            "https://%s:%s@%s.myshopify.com/admin" % 
            (
                self.config("SHOPIFY_KEY"),
                self.config("SHOPIFY_SECRET"),
                self.config("SHOPIFY_SITE")
            )    
        )
        self.graphql = GraphqlClient(f'https://{self.config("SHOPIFY_KEY")}:{self.config("SHOPIFY_SECRET")}@{self.config("SHOPIFY_SITE")}.myshopify.com/admin/api/2023-07/graphql.json')

    def statefile(self):
        return "fulfillment"
    
    def extract(self):
        self.orders = {}
        try:
            reader = csv.DictReader(open(sys.argv[1]),delimiter=",",quotechar='"')
            for row in reader:
                if row['ChannelOrderID'] in self.orders:
                    self.orders[row['ChannelOrderID']].append(row)
                else:
                    self.orders[row['ChannelOrderID']] = [row]
        except Exception as e:
            self.fatal(f"extract: {str(e)}")

    def transform(self):
        for order_id in self.orders.keys():
            fulfillments = self.orders[order_id]
            order = shopify.Order.find(order_id)