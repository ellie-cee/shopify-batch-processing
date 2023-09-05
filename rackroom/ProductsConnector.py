
import argparse
import csv
import os
import sys
import json
import time
import traceback
import rackroom
from datetime import datetime
from slugify import slugify
from dateutil.parser import parse as parsedate

class ProductsConnector(rackroom.ConnectorBase):
    def __init__(self):
        super().__init__()
        ap = argparse.ArgumentParser()
        ap.add_argument("-p","--path",help="Input Files Path")
        self.opts = vars(ap.parse_args())
        
        self.filename = self.tmpfile("Products","csv")
    def statefile(self):
        return "products";
    def fieldnames(self):
        return [
            "Handle","Command","Title","Body HTML","Vendor","Type",
            "Tags Command","Tags","Published","Option1 Name","Option1 Value",
            "Option2 Name","Option2 Value","Variant SKU","Variant Grams","Variant Inventory Tracker",
            "Variant Inventory Policy","Variant Price","Variant Requires Shipping",
            "Variant Taxable","Variant Barcode [ID]","Image Src",
            "Status",
            "Metafield: custom.product_type [single_line_text_field]",
            "Metafield: custom.product_category [single_line_text_field]",
            "Metafield: custom.product_subcategory [single_line_text_field]",
            "Metafield: custom.gender [list.single_line_text_field]"
        ]
    def read_file(self,file,pkey):
        result = {}
        infile = open(file.path)
        reader = csv.DictReader(infile,quotechar='"',delimiter=',')
        for row in reader:
            if pkey in row:
                if row[pkey] in result:
                    if isinstance(result[row[pkey]],list):
                        result[row[pkey]].append(row)
                    else:
                        result[row[pkey]] = [result[row[pkey]],row]
                else:
                    result[row[pkey]] = row
            else:
                print(f"missing primary key {pkey} in {file.path}")
        infile.close()
        return result
    def extract(self):
        
        self.files = []
        try:
            for file in os.scandir(self.opts["path"]):
                if file.is_file():
                    match file.name.split(".")[0]:
                        case "12":
                            self.files.append(file)
                            self.products = self.read_file(file,"baseProduct")
                        case "04":
                            self.brands = self.read_file(file,"id")
                        case "11":
                            self.base_products = self.read_file(file,"baseProduct")
                        case "03":
                            self.colors = self.read_file(file,"id")
                        case "13":
                            self.sizes = self.read_file(file,"sku")
                        case "14":
                            self.prices = self.read_file(file,"sku")
                        case "02":
                            self.color_families = self.read_file(file,"id")

                    
                    
            if len(self.files)>0:
               # self.upc_map = self.get_matrixify_products()
               x=0
            else:
                self.exit("Nothing to process!")
        except Exception as e:
            self.fatal(f"extract: {str(e)}")
        return self
    def transform(self):
        outfile = open(self.filename,"w")
        writer = csv.DictWriter(outfile,delimiter=',',quotechar='"',fieldnames=self.fieldnames())
        writer.writeheader()
        for products in self.products.values():
            if not isinstance(products,list):
                products = [products]

            for product in products:
                try:
                    
                    product['base'] = self.base_products[product['baseProduct']]
                    product['brand'] = self.brands[product['base']['brandId']]
                    product['size'] = self.sizes[product['sku']]
                    product['colors'] = list(map(lambda x: self.colors[x],product['colors'].split("|")))
                    
                    product['price'] = self.prices[product['sku']]
                    for size in product['size']:
                        row = {
                            "Handle":self.map_handle(product),
                            "Command":"MERGE",
                            "Title":product['base']['name'].title(),
                            "Body HTML":"",
                            "Vendor":product['brand']['name'].title(),
                            "Type":product['base']['type'],
                            "Tags Command":"MERGE",
                            "Tags":",".join([]),
                            "Published":"true",
                            "Option1 Name":"Size",
                            "Option1 Value":size['size'],
                            "Option2 Name":"Color",
                            "Option2 Value":"/".join(list(map(lambda x: x['name'].title(),product['colors']))),
                            "Variant SKU":product['sku'],
                            "Variant Grams":"",
                            "Variant Inventory Tracker":"shopify",
                            "Variant Inventory Policy":"deny",
                            "Variant Price":product['price']['price'],
                            "Variant Requires Shipping":"true",
                            "Variant Taxable":"true",
                            "Variant Barcode [ID]":size['upc'],
                            "Image Src":self.map_images(product),
                            "Status":"active",
                            "Metafield: custom.product_type [single_line_text_field]":"",
                            "Metafield: custom.product_category [single_line_text_field]":"",
                            "Metafield: custom.product_subcategory [single_line_text_field]":"",
                            "Metafield: custom.gender [list.single_line_text_field]":product['gender'].capitalize()
                        }
                        writer.writerow(row)

                except Exception as e:
                    traceback.print_exc()
        outfile.close()
        return self
    def load(self):
      #  self.sftp_put(self.filename,"to_Shopify/Products-ImpUp.csv")
        return self
    def cleanup(self):
        return self
    def map_images(self,product):
        return "; ".join(
            list(
                map(
                    lambda x:"https://deichmann.scene7.com/asset/deichmann/US_04_%s_%02d" % (product['sku'],x),
                    list(range(0,int(product['imageCount'])))
                )
            )
        )
    def map_handle(self,product):
        return slugify(
            "-".join([
                product['base']['name'],
                "-".join(list(map(lambda x:x['name'],product['colors']))),
                product['sku']
            ])
        )
        
    