# MQTT-to-InfluxDB
Subscribe to MQTT topic and write new messages to InfluxDB

# Overview

The purpose of this programme is to subscribe to an MQTT broker and await new messages on configured topics.  When a new message is received, it will be written to an InfluxDB database.

The '__main__' function sets up the MQTT Client, which operates via callback functions.  The InfluxDB functions operate as a separate thread in the 'influxDB_worker' function.

New data from the MQTT client callback is added to a queue, 'q'.  The 'influxDB_worker' monitors 'q' and writes new messages to the InfluxDB database.

Several functions have been found on or were inspired by:
http://www.steves-internet-guide.com/into-mqtt-python-client/

## Configuration

The configuration is stored in `config.json`.  An overview of the settings is below.
| Setting | Description |
| --- | --- |
|"MQTT_broker": "127.0.0.1",|             IP Address of MQTT Broker. |
|"MQTT_port": 1883,|                      Port of MQTT Broker. |
|"MQTT_topics": [["zigbee2mqtt/#",0]],|   MQTT topic(s) to which this programme will subscribe. List in form `[topic, QoS]`. |
|"MQTT_username": "",|                    MQTT Broker Username (default: none). |
|"MQTT_password": "",|                    MQTT Broker Password (default: none). |
|"MQTT_cname": "",|                       MQTT Client Name (i.e. this programme's name as far as broker is concerned). |
|"InfluxDB_host": "127.0.0.1",|           IP Address of InfluxDB. |
|"InfluxDB_port": "8086",|                Port of InfluxDB. |
|"InfluxDB_database": "MySensors",|       InfluxDB Database Name. |
|"storeChangesOnly": true|                Only write to database when values have changed. |

Sometimes it is desirable to change the names of sensors before they are written to the database.  This is useful if sensors have been replace, or have been relocated.  To do this, add lines to `stationNames.json`, in the format of:

`"originalName": "newName"`

Where `originalName` is the name incoming from the MQTT Broker, and `newName` is the name which will be written to InfluxDB (measurement name).

## Notes

Note that it is not possible to change the format of a measurement name in InfluxDB once the measurement name has been created.  For example, is a sensor which only reports temperate has been written to a measurement named `TempA`, and the sensor is replaced with one that measures temperature and humidity, InfluxDB will not accept the new values into the `TempA` measurement.  A new measurement name will need to be used.
