import csv
import os
import sys
import json
import time
from datetime import datetime
from dateutil.parser import parse as parsedate
import rackroom

class InventoryConnector(rackroom.base.ConnectorBase):
    def __init__(self):
        super().__init()

        self.data = []
        self.filename = self.tmpfile()

    def statefile(self):
        return "inventory"
    
    def transform(self):
