#!/usr/bin/env python3

import rackroom

connector = rackroom.OrderConnector()
try:
    connector.extract().transform().load().cleanup()
except Exception as e:
    connector.fatal(f"fatal errror: {str(e)}")


