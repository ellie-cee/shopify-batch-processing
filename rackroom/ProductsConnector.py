
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
            "Tags Command","Tags","Published","Published At","Option1 Name","Option1 Value",
            "Option2 Name","Option2 Value","Variant SKU","Variant Grams","Variant Inventory Tracker",
            "Variant Inventory Policy","Variant Price","Variant Requires Shipping",
            "Variant Taxable","Variant Barcode [ID]","Variant Inventory Qty","Image Src",
            "Status",
            "Metafield: custom.product_type [single_line_text_field]",
            "Metafield: custom.product_category [single_line_text_field]",
            "Metafield: custom.product_subcategory [single_line_text_field]",
            "Metafield: custom.gender [list.single_line_text_field]"
        ]
    def read_file(self,file,pkey,force_array=False):
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
                    if force_array:
                        result[row[pkey]] = [row]
                    else:
                        result[row[pkey]] = row
            else:
                print(f"missing primary key {pkey} in {file.path}")
        infile.close()
        return result
    
    def read_inventory(self,file):
        infile = open(file.path)
        reader = csv.DictReader(infile,quotechar='"',delimiter=',')
        for row in reader:
            self.inventory[row['upc']] = row['qty']
        infile.close()

    def extract(self):
        
        self.files = []
        self.inventory = {}
        try:
            for file in os.scandir(self.opts["path"]):
                if file.is_file() and not "done" in file.name:
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
                            self.sizes = self.read_file(file,"sku",True)
                        case "14":
                            self.prices = self.read_file(file,"sku")
                        case "02":
                            self.color_families = self.read_file(file,"id")
                        case "01":
                            self.categories = self.read_file(file,"categoryId")
                        case "24":
                           self.read_inventory(file)
                        case "25":
                            self.read_inventory(file)
                    if file.name.startswith("product_desc"):
                        self.product_desc = {x['product_sku']:x['product_description'] for x in json.load(open(file.path))}

                    
                    
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

                    if not product['sku'] in self.sizes:
                        sku= product['sku']
                        print(f'"{sku}"')
                        continue
                    
                    product['base'] = self.base_products[product['baseProduct']]
                    product['brand'] = self.brands[product['base']['brandId']]
                    product['size'] = self.sizes[product['sku']]
                    product['colors'] = list(map(lambda x: self.colors[x],product['colors'].split("|")))
                    product['inventory'] = ""
                    if product['sku'] in self.product_desc:
                        product['description'] = self.product_desc[product['sku']]
                    else:
                        product['description'] = ""
                    
                        
                    product["category"] = product["categories"].split("|")
                    product['webcats'] = self.parse_categories(product["categories"])
                    
                    product['price'] = self.prices[product['sku']]
                    for size in product['size']:
                        if size["upc"] in self.inventory:
                            product["inventory"] = self.inventory[size['upc']]
                        else:
                            product["inventory"] = ""
                            
                        row = {
                            "Handle":self.map_handle(product),
                            "Command":"MERGE",
                            "Title":product['base']['name'].title(),
                            "Body HTML":product['description'],
                            "Vendor":product['brand']['name'].title(),
                            "Type":product['base']['type'],
                            "Tags Command":"MERGE",
                            "Tags":",".join([
                                f"Gender: {product['gender'].capitalize()}",
                                f"ProductId: {product['sku']}",
                                f"ProductCategory: {self.categories[product['category'][0]]['name'] if len(product['category'][0])>0 else ''}",
                                f"GenderProductCategory: {product['gender'].capitalize()} {self.categories[product['category'][0]]['name'] if len(product['category'][0])>0 else ''}",
                                f"cv:{self.map_handle(product)}",
                                f"WebSkuGroup:{product['base']['name'].title()}"
                            ]),
                            "Published":"true",
                            "Option2 Name":"Size",
                            "Option2 Value":size['size'],
                            "Option1 Name":"Color",
                            "Option1 Value":"/".join(list(map(lambda x: x['name'].title(),product['colors']))),
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
                            "Variant Inventory Qty":product['inventory'],
                            "Metafield: custom.product_type [single_line_text_field]":", ".join(product['webcats']['cat']),
                            "Metafield: custom.product_category [single_line_text_field]":", ".join(product['webcats']['styles']),
                            "Metafield: custom.product_subcategory [single_line_text_field]":", ".join(product['webcats']['features']),
                            "Published At":datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "Metafield: custom.gender [list.single_line_text_field]":product['gender'].capitalize()
                        }
                        writer.writerow(row)

                except Exception as e:
                    traceback.print_exc()
        outfile.close()
        return self
    def load(self):
        self.sftp_put(self.filename,"to_Shopify/Products-ImpUp.csv")
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
    def parse_categories(self,cats):
        ret = {
            'cat':[],
            'styles':[],
            'features':[]
        }
        for cat in cats.split("|"):
            if cat in self.categories:
                catname = self.categories[cat]['name']
                if cat.endswith("style"):
                    ret['styles'].append(catname)
                elif cat.endswith("feature"): 
                    ret['features'].append(catname)
                else:
                    ret['cat'].append(catname)
        return ret    
    