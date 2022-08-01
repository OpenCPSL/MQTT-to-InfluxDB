# MQTT-to-InfluxDB
Subscribe to MQTT topic and write new messages to InfluxDB

# Overview

The purpose of this programme is to subscribe to an MQTT broker and await new messages on configured topics.  When a new message is received, it will be written to an InfluxDB database.

The '__main__' function sets up the MQTT Client, which operates via callback functions.  The InfluxDB functions operate as a separate thread in the 'influxDB_worker' function.

New data from the MQTT client callback is added to a queue, 'q'.  The 'influxDB_worker' monitors 'q' and writes new messages to the InfluxDB database.

Several functions have been found on or were inspired by:
http://www.steves-internet-guide.com/into-mqtt-python-client/
