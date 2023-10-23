import csv
import argparse
import os
import re
import sys
import json
import time
from datetime import datetime
from dateutil.parser import parse as parsedate
import rackroom

class InventoryConnector(rackroom.base.ConnectorBase):
    def __init__(self):
        super().__init__()
        ap = argparse.ArgumentParser()
        ap.add_argument("-p","--path",help="Input Files Path")
        
        self.opts = vars(ap.parse_args())
        if not "path" in self.opts:
            self.fatal("Input path not supplied!")
        
        self.filename = self.tmpfile("Products-Inventory","csv")
    
    def extract(self):
        self.lines = []
        self.files = []
        self.upc_map = {}
        for file in os.scandir(self.opts["path"]):
            if file.is_file() and "storestock" in file.name and not "done" in file.name:
                self.files.append(file)
                reader = csv.reader(open(file.path),quotechar='"',delimiter=',')
                next(reader)
                for row in reader:
                    self.lines.append(row)
        if len(self.files)<1:
            
            self.exit("No files to process.")
        else:
          self.upc_map = self.get_matrixify_products(self.config("PRODUCTS_FILE"))

        return self

    def statefile(self):
        return "inventory"
    
    def fields(self):
        return ["ID","Variant ID","Command","Variant Command","Variant Barcode","Inventory Available: ROK1","Inventory Available: ZAP2"]
    def map_to_location(self,store_number):
        for pattern in self.config_dict['inventory']['storemap'].keys():
            if re.search(pattern,store_number):
                return self.config_dict['inventory']['storemap'][pattern]
        return "ROK1"
    
    def transform(self):
        outfile = open(self.filename,"w")
        writer = csv.DictWriter(outfile,fieldnames=self.fields(),delimiter=',',quotechar='"')
        writer.writeheader()
        for line in self.lines:
            if line[1] in self.upc_map:
                row = {
                    "ID":self.upc_map[line[1]]['ID'],
                    "Variant ID":self.upc_map[line[1]]['Variant ID'],
                    "Command":"Merge",
                    "Variant Command":"Merge",
                    "Variant Barcode":line[1],
                    "Inventory Available: ROK1":"",
                    "Inventory Available: ZAP2":""
                }
                row[f"Inventory Available: {self.map_to_location(line[0])}"] = line[2]
                writer.writerow(row)
            else:
                print(f"UPC {line[1]} not in products masterfile")
        outfile.close()
        return self
    def load(self):
        self.sftp_put(self.filename,f"to_Shopify/Product-Inventory.csv")
        return self
    def cleanup(self):
        for file in self.files:
            if self.config("purge")=="yes":
                os.remove(file.path)
        #os.remove(self.filename)
    