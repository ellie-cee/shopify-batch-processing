
import requests
import shopify
import csv
import os
import sys
import json
import paramiko
import traceback
import time
import xmlformatter
from datetime import datetime
from dateutil.parser import parse as parsedate

class ConnectorBase:
    def __init__(self):
        try:
            self.config_dict = json.load(open("config/config.json"))
            self.state = self.loadstate()
        except Exception as e:
            traceback.print_exc()
            self.error(str(e))
        if (self.state['state']=="running"):
            self.warning("%s already running" % (sys.argv[0]))
            sys.exit(0)
        self.setstate("running")
    def config(self,key):
        if os.getenv(key):
            return os.getenv(key)
        elif key in self.config_dict:
            return self.config_dict[key]
        else:
            return None
    def setstate(self,state,ts=None):
        self.state['state'] = state
        if (ts!=None):
            self.state['lastrun'] = ts.strftime("%Y-%m-%dT%H:%M:%S-4:00")
        sfo = open("config/%s.state" % (self.statefile()),"w")
        print(json.dumps(self.state),file=sfo)

    def loadstate(self):
        try:
            return json.load(open("config/%s.state" % (self.statefile())))
        except:
            return {
                'state':'init',
                'lastrun':datetime.now().strftime("%Y-%m-%dT%H:%M:%S-4:00")
            }
    def statefile(self):
        return "base"
    def extract(self):
        self.info(f"Running {self.statefile()}: Extract Data")
        return self
    def transform(self):
        self.info(f"Running {self.statefile()}: Transform Data")
        return self
    def load(self):
        self.info(f"Running {self.statefile()}: Load Data")
        return self
    def cleanup(self,purge=True):
        self.info(f"Running {self.statefile()}: Cleanup")
        try:
            for file in os.scandir(os.getenv("TMPDIR")):
                if file.is_file():
                    if purge:
                        os.remove(file.path)
                lastrun = datetime.now()
            self.setstate("success",ts=datetime.now())
        except Exception as e:
            self.error(str(e))
    def exit(self,message):
        self.info(message)
        self.setstate("success",ts=datetime.now())
        sys.exit(0)
    def fatal(self,message):
        self.error(message)
        sys.exit(-1)
    def error(self,message):
        self.log("error",message)
        self.setstate("error")
    def warning(self,message):
        self.log("warning",message)
    def info(self,message):
        self.log("info",message)
        
    def log(self,level,message):
        now = datetime.now()
        print(
            "[%s] %s; %s" % (now.strftime("%Y-%m-%d %H:%M:%S"),level,message),
            file=sys.stderr
        )
    def sftpclient(self):
            try:
                transport = paramiko.Transport(self.config("SFTP_HOST"),22)
                transport.connect(
                    username=self.config("SFTP_USER"),
                    password=None,
                    pkey=paramiko.ECDSAKey(filename=self.config("SFTP_KEY"))
                )
                return paramiko.SFTPClient.from_transport(transport)
            except Exception as e:
                self.fatal(f"Unable to connect: {str(e)}")
    def sftp_put(self,local,remote):
        try:
          client = self.sftpclient()
          client.put(
              local,
              remote
          )
          self.info(f"Uploaded {remote} to SFTP")
        except Exception as e:
            self.fatal(f"Unable to put {local} to remote: {str(e)}")
    def tmpfile(self,base,type):
        return f"{self.config('TMPDIR')}/{base}-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.{type}"


