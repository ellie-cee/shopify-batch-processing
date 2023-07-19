import csv
import os
import sys
import json
import time
from datetime import datetime
from dateutil.parser import parse as parsedate

class InventoryConnector(ConnectorBase):
    def statefile(self):
        return "inventory"
    def load(self):
        filename=""
        self.sftp_put(
            "%s/%s" % (self.config("TMPDIR"),filename),
            "%s/Matrixify/%s" % (self.config("SFTP_REMOTE"),filename)
        )
