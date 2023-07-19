
import csv
import os
import sys
import json
import time
import rackroom
from datetime import datetime
from dateutil.parser import parse as parsedate

class ProductsConnector(rackroom.ConnectorBase):
    def statefile(self):
        return "products";