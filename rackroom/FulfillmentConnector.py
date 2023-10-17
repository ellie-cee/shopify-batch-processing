import argparse
import csv
import os
import sys
import json
import time
import traceback
import shopify
import re
import hashlib
from python_graphql_client import GraphqlClient

from datetime import datetime
from dateutil.parser import parse as parsedate
import rackroom

class FulfillmentConnector(rackroom.base.ConnectorBase):
    def __init__(self):
        super().__init__()
        ap = argparse.ArgumentParser()
        ap.add_argument("-p","--path",help="Input Files Path")
        self.opts = vars(ap.parse_args())
        shopify.ShopifyResource.set_site(
            "https://%s:%s@%s.myshopify.com/admin" % 
            (
                self.config("SHOPIFY_KEY"),
                self.config("SHOPIFY_SECRET"),
                self.config("SHOPIFY_SITE")
            )    
        )
        self.filename = self.tmpfile("Order-Shipping","csv")

    def statefile(self):
        return "fulfillment"
    
    def fields(self):
        return [
            "ID","Name","Command","Line: Type","Line: ID","Line: Quantity",
            "Fulfillment: ID","Fulfillment: Status","Fulfillment: Created At",
            "Fulfillment: Updated At","Fulfillment: Tracking Company","Fulfillment: Location",
            "Fulfillment: Shipment Status","Fulfillment: Tracking Number",
            "Fulfillment: Tracking URL","Fulfillment: Send Receipt",
            "Refund: ID","Refund: Note","Refund: Restock","Refund: Restock Type",
        	"Refund: Send Receipt","Refund: Generate Transaction"
        ]
    
    def extract(self):
        self.orders = {}
        self.files = []
        processed = {}
        try:
            for file in os.scandir(self.opts["path"]):
                if file.is_file() and re.search(self.config("fulfillment_file_name"),file.name):
                    
                    self.files.append(file)
                    reader = csv.DictReader(open(file.path),quotechar='"',delimiter=',')

                    for row in reader:
                        digest = hashlib.md5(json.dumps(row,sort_keys=True).encode('utf-8')).hexdigest()

                        if row['OrderCode'] in self.orders:
                            if digest not in processed:                     
                                self.orders[row['OrderCode']].append(row)
                                processed[digest] = True
                        else:
                            self.orders[row['OrderCode']] = [row]
                            processed[digest] = True

            
            
            
            if len(self.files)>0:
                self.upc_map = self.get_matrixify_products()
                
            else:
                self.exit("Nothing to process!")
        except Exception as e:
            self.fatal(f"extract: {str(e)}")
        return self
    
    def transform(self):
        outfile = open(self.filename,"w")
        writer = csv.DictWriter(outfile,delimiter=',',quotechar='"',fieldnames=self.fields())
        writer.writeheader()
        fulfillment_id = 1
        for order_id in self.orders.keys():
            fulfillments = self.orders[order_id]
            order = shopify.Order.find(order_id)
            
            for code in fulfillments:
                try:
                    line_item = list(
                        filter(lambda x:x.sku==self.upc_map[code['productCode']]['Variant SKU'],order.line_items)
                    )[0]
                    
                    row = None
                    if code['Fulfilled']=="1":
                        row ={
                            "ID":order.id,
                            "Name":order.name,
                            "Command":"UPDATE",
                            "Line: Type":"Fulfillment Line",
                            "Line: ID":line_item.id,
                            "Line: Quantity":code['Quantity'],
                            "Fulfillment: ID":str(fulfillment_id),
                            "Fulfillment: Status":"success",
                            "Fulfillment: Shipment Status":"label_printed",
                            "Fulfillment: Tracking Company":code['Carrier'],
                            "Fulfillment: Tracking Number":code['ShippingTrackingNumber'],
                            "Fulfillment: Send Receipt":"true"
                        }
                    else:
                        row ={
                            "Refund: ID":str(fulfillment_id),
                            "ID":order.id,
                            "Name":order.name,
                            "Command":"UPDATE",
                            "Line: Type":"Refund Line",
                            "Line: ID":line_item.id,
                            "Line: Quantity":f"-{code['Quantity']}",
                            "Fulfillment: Status":"cancelled",
                            "Refund: Note":"cancelled or unfulfillable",
                            "Refund: Restock":"TRUE",
                            "Refund: Restock Type":"cancel",
        	                "Refund: Send Receipt":"TRUE",
                            "Refund: Generate Transaction":"false"
                        }
                    writer.writerow(row)
                    fulfillment_id=fulfillment_id+1
                except:
                    traceback.print_exc()
                    self.info("skipping")
        outfile.close()
        return self
    def load(self):
        self.sftp_put(
            self.filename,
            "to_Shopify/Orders-Shipping.csv"
        )
        return self
    def cleanup(self):
        for file in self.files:
            if self.config("purge")=="yes":
                os.remove(file)
            x = 1
        #os.remove(self.filename)
        return self
