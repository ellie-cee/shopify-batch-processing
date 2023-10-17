import argparse
from functools import reduce
import shopify
import os
import sys
import json
import traceback
import time
import xmlformatter
from datetime import datetime
import rackroom
import requests
from dateutil.parser import parse as parsedate
from python_graphql_client import GraphqlClient

class OrderConnector(rackroom.base.ConnectorBase):
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
        self.graphql = GraphqlClient(f'https://{self.config("SHOPIFY_KEY")}:{self.config("SHOPIFY_SECRET")}@{self.config("SHOPIFY_SITE")}.myshopify.com/admin/api/2023-07/graphql.json')
        self.product_cache = {};
        self.files = []
    def statefile(self):
        return "orders"
    def extract(self):
        self.orders=[]
        try:
            result = self.graphql.execute(query=self.graphql_bulk_orders())
            unfinished = True
            while unfinished:
                result = self.graphql.execute(query=self.graphql_query_orders())
                if result["data"]["currentBulkOperation"]["status"]=="COMPLETED":
                    url = result["data"]["currentBulkOperation"]["url"]
                    if url is not None:
                        ret = requests.get(url).content.decode()
                        for line in ret.splitlines():
                            
                            self.orders.append(json.loads(line)["id"].split("/")[-1])
                        unfinished = False
                    else:
                        self.exit("No orders. Exiting")
                else:
                    time.sleep(1)
        except Exception as e:
            self.fatal(f"Unable to fetch orders: {str(e)} ")
         
        self.info(f"Running {self.statefile()} extracted {len(self.orders)} orders")
        return self
    def extract_api(self):
        super().extract()
        self.orders = []
        proceed = True

        dt = parsedate(self.state.get("lastrun"))
        orders = shopify.Order.find(created_at_min=self.state.get("lastrun"))
        while proceed:
            for order in orders:
                odt = parsedate(order.created_at)
                if (odt>=dt):
                    self.orders.append(order)

            if orders.has_next_page():
                orders = orders.next_page()
            else:
                proceed = False
        if len(self.orders)<1:
            self.exit("No orders. Exiting")
        self.info(f"Running {self.statefile()} extracted {len(self.orders)} orders")
        return self
    def transform(self):
        super().transform()
        
        
        
        products = {}
        proceed = True
        for order_id in self.orders:
                xml = """<?xml version='1.0' encoding='UTF-8'?>
<orders xmlns="http://www.rackroomshoes.com/xml/hybris/6.0/impex">
        """
                order = shopify.Order.find(order_id) 

                xml+=f"""
    <order>
        <code>{order.id}</code>
        <creation-date>{order.created_at}</creation-date>
        <meta-status>
            <value>created</value>
            <date>{order.created_at}</date>
        </meta-status>
        <currency>{order.currency}</currency>
        <subtotal>{order.subtotal_price}</subtotal>
        <total-price>{order.total_price}</total-price>
        <total-tax>{order.total_tax}</total-tax>
        <tax-provider>Shopify</tax-provider>
        <tax-transaction-id></tax-transaction-id>
        <payment>
            <cost>{order.total_price}</cost>
            <paymentinfo>
                {self.render_transactions(order)}
            </paymentinfo>
        </payment>
        <delivery>
            <code>{order.shipping_lines[0].code}</code>
            <cost>{order.shipping_lines[0].price}</cost>
            {self.render_shipping_tax_lines(order)}
        </delivery>
        <user>
            <code>{order.customer.id}</code>
            <lang>en</lang>
            <shipping-address>
                <phone>{order.shipping_address.phone}</phone>
                <checked>true</checked>
                <first-name>{order.shipping_address.first_name}</first-name>
                <last-name>{order.shipping_address.last_name}</last-name>
                <street1>{order.shipping_address.address1}</street1>
                <street2>{order.shipping_address.address2}</street2>
                <city>{order.shipping_address.city}</city>
                <postal-code>{order.shipping_address.zip}</postal-code>
                <postal-code-plus4></postal-code-plus4>
                <state>{order.shipping_address.province_code}</state>
                <country-code>{order.shipping_address.country_code}</country-code>
            </shipping-address>
            <smartbutton-member-id></smartbutton-member-id>
            <last-name>{order.customer.last_name}</last-name>
            <first-name>{order.customer.first_name}</first-name>
            <country-code>{order.customer.default_address.country_code}</country-code>
            <phone-number>{order.customer.phone}</phone-number>
            <email-address>{order.customer.email}</email-address>
            <store-no>9404</store-no>
        </user>
        <order-entries>
            {self.render_order_entries(order)}
        </order-entries>
        <voucher-entries/>
    </order>
            """
                order.attributes["tags"] = ",".join(list(filter(lambda x: x!=self.config("EXPORT_TAG"),order.tags.split(","))))
                order.save()
                xml+="""
</orders>"""
                filename = f'{self.opts["path"]}/Order.{order.id}.xml'
                output = open(filename,"w")
                self.files.append(filename)
                #formatter = xmlformatter.Formatter(indent="1", indent_char="\t", encoding_output="ISO-8859-1", preserve=["literal"])
                print(xml,file=output)
                output.close()
        
        return self
    
    def render_order_entries(self,order):
        xml=""
        order_entry = 0
        for line_item in order.line_items:
            for itn in range(line_item.quantity):
                order_entry = order_entry+1
                product = self.fetch_product(line_item.product_id)
                try:
                    variant = self.get_variant(product,line_item.variant_id)
                except Exception as e:
                    self.error(f"Line Item {line_item.id} on order {order.id} is no longer available")
                    continue

                xml+=f"""
                <order-entry>
                    <entrynumber>{order_entry}</entrynumber>
                    <product-name><![CDATA[{line_item.name}]]></product-name>
                    <product-code>{variant.barcode}</product-code>
                        <base-price>{variant.price}</base-price>
                        <total-price>{float(line_item.pre_tax_price)/line_item.quantity}</total-price>
                        <discounts>
                            {self.render_discounts(line_item,order)}
                        </discounts>
                        <article-number>{variant.sku}</article-number>
                        <variant-size>{self.find_option("Size",product,variant)}</variant-size>
                        <variant-width>{self.find_option("Width",product,variant)}</variant-width>
                        <variant-color>{self.find_option("Color",product,variant)}</variant-color>
                        <orderentry-pk></orderentry-pk>
                        <tax>
                            <country-code>{order.shipping_address.country_code}</country-code>
                            <state-or-province>{order.shipping_address.province_code}</state-or-province>
                            <total-tax-applied>{"%0.2f" % (reduce(lambda a,b:a+float(b.price),line_item.tax_lines,0.0)/line_item.quantity)}</total-tax-applied>
                            <tax-details>
                                {self.render_tax_lines(line_item)}
                            </tax-details>
                        </tax>
                        <stock-type>SHOPIFY</stock-type>
                        <delivery-code>{order.shipping_lines[0].code}</delivery-code>
                    </order-entry>
                """
        return xml
    def render_shipping_tax_lines(self,order):
        xml=""
        for tax_line in order.shipping_lines[0].tax_lines:

            xml+=f"""
              <tax-details>
                    <detail>
                        <authorityName></authorityName>
                        <authorityType>12043</authorityType>
                        <taxName>{tax_line.title}</taxName>
                        <taxApplied>{float(tax_line.price)}</taxApplied>
                        <feeApplied>0.0</feeApplied>
                        <taxableQuantity>1</taxableQuantity>
                        <taxableAmount>{float(order.shipping_lines[0].price)}</taxableAmount>
                        <exemptQty>0.0</exemptQty>
                        <exemptAmt>0.0</exemptAmt>
                        <taxRate>{tax_line.rate}</taxRate>
                        <baseType/>
                        <passFlag/>
                        <passType/>
                    </detail>
                </tax-details>
            """
        
        return f"""<tax>
                    <country-code>{order.shipping_address.country_code}</country-code>
                    <state-or-province>{order.shipping_address.province_code}</state-or-province>
                    <total-tax-applied>{"%0.2f" % (reduce(lambda a,b:a+float(b.price),order.shipping_lines[0].tax_lines,0.0))}</total-tax-applied>
                    {xml}
                </tax>"""
    def render_tax_lines(self,line_item):
        xml=""
        for tax_line in line_item.tax_lines:
            xml+=f"""
                                
                            <detail>
                                <authorityName></authorityName>
                                <authorityType>12043</authorityType>
                                <taxName>{tax_line.title}</taxName>
                                <taxApplied>{float(tax_line.price)/line_item.quantity}</taxApplied>
                                <feeApplied>0.0</feeApplied>
                                <taxableQuantity>1</taxableQuantity>
                                <taxableAmount>{float(line_item.price)/line_item.quantity}</taxableAmount>
                                <exemptQty>0.0</exemptQty>
                                <exemptAmt>0.0</exemptAmt>
                                <taxRate>{tax_line.rate}</taxRate>
                                <baseType/>
                                <passFlag/>
                                <passType/>
                            </detail>
                """
        return xml
    def render_discounts(self,line_item,order):
        xml=""
        for discount_line in line_item.discount_allocations:
            discount = self.get_discount_allocation(discount_line,order)
            discount_label = ""
            if "title" in discount:
                discount_label = discount.get("title")
            else:
                discount_label = discount.get("code")
            xml+=f"""
                        <discount>
                            <id></id>
                            <name>{discount_label}</name>
                            <value>{float(discount_line.amount)/line_item.quantity}</value>
                            <groupid></groupid>
                            <discountType>PROMOTION</discountType>
                        </discount>
            """
            return xml
    def render_transactions(self,order):
        return self.render_transactions_cc(order)

    def render_transactions_cc(self,order):
        xml=""
        transactions = self.fetch_transactions(order.id)
        if transactions:
            transaction = transactions[-1]
            xml+=f"""
                        <shopify>
                            <merchant-id>rackroom</merchant-id>
                            <request-id>{transaction.receipt.charges.data[0].id}</request-id>
                            <requestToken>{transaction.receipt.charges.data[0].payment_intent}</requestToken>
                            <subscription-id>{transaction.receipt.charges.data[0].metadata.order_id}</subscription-id>
                            <transaction-type>{transaction.kind}</transaction-type>
                            <decision>{transaction.status}</decision>
                            <reason-code>100</reason-code>
                            <authorization-code>08095D</authorization-code>
                            <authorized-date-time>{transaction.processed_at}</authorized-date-time>
                            <avs-matching-code>{transaction.payment_details.avs_result_code}</avs-matching-code>
                            <order-amount>{transaction.receipt.charges.data[0].amount/100}</order-amount>
                            <request-amount>{transaction.receipt.charges.data[0].amount/100}</request-amount>
                            <authorized-amount>{self.authorized_amount(transaction)}</authorized-amount>
                            <currency>
                                <currency-code>{transaction.currency}</currency-code>
                            </currency>
                            <mask-cc-number>XXXX XXXX XXXX {transaction.payment_details.credit_card_number.split(" ")[-1]}</mask-cc-number>
                """
            try:
                            xml+=f"""
                            <card-expiration-month>{transaction.receipt.charges.data[0].payment_method_details.card.exp_month}</card-expiration-month>
                            <card-expiration-year>{transaction.receipt.charges.data[0].payment_method_details.card.exp_year}</card-expiration-year>
                            """
            except:
                            xml+="""
                            <card-expiration-month></card-expiration-month>
                            <card-expiration-year></card-expiration-year>
                            """        
            
            xml+=f"""        <card-type>{transaction.payment_details.credit_card_company}</card-type>
                            <billing-address>
                                <email>{order.customer.email}</email>
                                <phone/>
                                <checked>true</checked>
                                <first-name>{order.billing_address.first_name}</first-name>
                                <last-name>{order.billing_address.last_name}</last-name>
                                <street1>{order.billing_address.address1}</street1>
                                <street2>{order.billing_address.address2}</street2>
                                <city>{order.billing_address.city}</city>
                                <postal-code>{self.zip(order.billing_address)}</postal-code>
                                <postal-code-plus-4>{self.zip4(order.billing_address)}</postal-code-plus-4>
                                <state>{order.billing_address.province_code}</state>
                                <country-code>{order.billing_address.country_code}</country-code>
                            </billing-address>
                        </shopify>
                """
        return xml
    def zip(self,address):
        if address.country_code.startswith("US"):
            return address.zip.split("-")[0]
        
        return address.zip
    def zip4(self,address):
        if address.country_code.startswith("US"):
            zp =  address.zip.split("-")
            if (len(zp)>1):
                return zp[-1]
        return ""
    def authorized_amount(self,transaction):
        try:
            return transaction.receipt.charges.data[0].payment_method_details.card.amount_authorized
        except:
            return transaction.receipt.charges.data[0].amount/100

    def fetch_transactions(self,id):
        retries = 10
        success = False
        while retries>0 and not success:
            retries = retries -1
            try:
                transactions = shopify.Transaction.find(order_id=id)
                time.sleep(0.25)
                success = True
                return transactions
            except:
                self.error(f"Unable to fetch product data for product #{id} ")
                time.sleep(1)
            if not success:
                self.fatal(f"Unable to fetch transaction data for order #{id} after 10 retries")
        
    def fetch_product(self,id):
        
        if id in self.product_cache:
            return self.product_cache[id]
        else:
            retries = 10
            success = False
            while retries>0 and not success:
                retries = retries -1
                try:
                    product = shopify.Product.find(id)
                    time.sleep(0.25)
                    self.product_cache[id] = product
                    return product
                    
                except:
                    self.error(f"Unable to fetch product data for product #{id} ")
                    time.sleep(1)
            if not success:
                self.fatal(f"Unable to fetch product data for product #{id} after 10 retries")
    def get_variant(self,product,id):
        for variant in product.variants:
            if variant.id==id:
                return variant
        return None
    def find_option(self,option,product,variant):
        names=["option1","option2","option3"]
        option = list(filter(lambda x: x.name==option,product.options))
        if (len(option)):
            return variant.to_dict()[names[option[0].position-1]]
        return ""
    def get_discount_allocation(self,discount_line,order):
        try:
            return order.discount_applications[discount_line.discount_application_index].to_dict()
        except:
            
            return {}
   # def load(self):
      #  super().load()
        #for file in self.files:
         #   self.sftp_put(
           #     file,
            #    f"from_Shopify/{file.split('/')[-1]}"
          #  )
        #return self
    
    def graphql_bulk_orders(self):
        return '''
mutation {
  bulkOperationRunQuery(
   query: """
    {
      orders(query:"tag:EXPORT_ERP") {
        edges {
          node {
            id
          }
        }
      }
    }
    """
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
'''
    def graphql_query_orders(self):
        return """
query {
  currentBulkOperation {
    id
    status
    errorCode
    createdAt
    completedAt
    objectCount
    fileSize
    url
    partialDataUrl
  }
}
"""