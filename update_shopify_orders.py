#!/usr/bin/env python3
import rackroom

connector = rackroom.OrderUpdateConnector()
try:
    connector.extract().transform().load().cleanup()
except Exception as e:
    connector.fatal(f"fatal errror: {str(e)}")
