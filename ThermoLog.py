# -*- coding: utf-8 -*-
"""
CPSL - ThermoLog
Copyright (C) 2022  CPSL

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""


"""
The purpose of this programme is to read the values from 1-wire 
temperature sensors and publish the values to an MQTT broker.

It presently assumes use of DS18B20 temperature sensors, but may be
compatible with, or modified for use with, other similar sensors.

It has been tested using a Raspberry Pi.  Remember to enable the 1-wire
protocol using the Raspberry Pi OS configuration utility.


Several functions have been found on or were inspired by:
http://www.steves-internet-guide.com/into-mqtt-python-client/
"""


import time
import json
from datetime import datetime
import paho.mqtt.client as mqtt

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("Topic/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    # print("MQTT Published:", userdata)
    pass


# Load the config file    
def loadConfig(configFile="config.json"):
    
    with open(configFile) as jsonFile:
        return json.load(jsonFile)        

# Check the CRC checksum status for the temperature sensors
def crc(input):

    crc = input.rsplit(' ',1)
    crc = crc[1].replace('\n', '')   
    
    if crc == "YES":
        crcValue = True
    else:
        crcValue = False

    return crcValue

# Read the value of a particular DS18B20 sensor given a specific sensor ID	
def readDS18B20(sensorID):    
    
    with open('/sys/bus/w1/devices/' + sensorID + '/w1_slave', 'r') as f:

        # w1_slave has two lines, first has the CRC check, second has the temperature value.
    
        line = f.readline()                 # First line of file

        if crc(line):
            line = f.readline()             # Second line of file
            sensorTempStr  = line.rsplit('t=',1)[1].replace('\n', '')  
        else:
            sensorTempStr = "CRC_False" 

    return sensorTempStr            

# Read the temperature from a particular sensor by sensor ID (N.B.: assumes a DS18B20 sensor)
# By default, will try to read up-to 5 times.   	
def readTemp(sensorID, retries = 5):

    retryCnt = 0
    
    while retryCnt < retries:
    
        sensorTempStr = readDS18B20(sensorID)
        
		# The value exactly 85000 (85.000 C) usually indicates a sensor error, often power related.
		# If this happens, try and read the sensor again.
        if sensorTempStr == "85000":
            print("Sensor showing value of 85 deg.  Not valid.  Retrying:", retryCnt)
            retryCnt += 1
            time.sleep(1)
            sensorTempStr = "-274000"
            continue
		# If the CRC is invalid, try and read again.
        elif sensorTempStr == "CRC_False":
            print("Sensor CRC check failed.  Not valid.  Retrying:", retryCnt)
            rretryCnt += 1
            time.sleep(1)
            sensorTempStr = "-274000"
            continue
        else:
            break
    
	# If retry count is exceeded, the function returns an absurd temperature value to indicate an error to the application.
    if retryCnt > retries:
        sensorTemp = -274.0             # Less than absolute zero, impossible.
    else:
        sensorTemp = float(int(sensorTempStr) / 1000.0)
    
    return sensorTemp
	
	

# ##############################
# ------------ MAIN ------------
# ##############################
               
if __name__ == '__main__':
    
    config  = loadConfig("config.json")	 	# Load config from file
    sensors = loadConfig("sensors.json") 	# Load sensor IDs to read from file
    
	# Create an MQTT client and setup callback functions
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
        
    client.connect(config["broker"], config["port"], 60)
    # client.loop~ function required to handle automatic reconnects to the broker.
    client.loop_start()    
    
	
	# Setup Python dictionaries to store temperature values
    sensorTemp = dict()
    publishTemps = dict()
    publish = False
    
    # Loop forever
    while True:
	
		# For each sensor ID, read the temperature of that sensor.
        for key, value in sensors.items():
        
            sensorName = key
            sensorID   = value
            sensorTemp[key] = readTemp(sensorID)        
        
        # Log data to local file
        timeNow = datetime.now().astimezone().isoformat('T', 'seconds')   		# Doesn't work on Python 3.5
        # timeNow = datetime.now().replace(microsecond=0).isoformat()			# If running Python 3.5 or earlier, use this line
        sensorTempJSON = json.dumps(sensorTemp)
        logString = timeNow + ", " + sensorTempJSON
        
        # Print to console        
        print(logString)
        
		
		# Log to file, if enabled in config.
        if config["logToFile"]:
            fileTime = datetime.now().strftime("%Y-%m-%d")
            fileName = fileTime + "_Log.txt"
            
            with open(fileName, "a") as f:            
                f.write(logString + "\n")
        
                
        ### Check if data has actually changed before publishing to MQTT Broker   
		
        # Make sure 'publishTemps' dictionary is same length as 'sensorTemp' dictionary		
		if len(publishTemps) != len(sensorTemp):
            publishTemps = sensorTemp.copy()
            publish = True
        
		# Step throw sensor values, compare against previously published values.
		# If any are different by an amount larger than the configured 'deltaT', publish all recent sensor values.
        for key in sensorTemp:
        
            if key == "Time":		# Ignore the 'Time' key
                continue
        
            if abs(sensorTemp[key] - publishTemps[key]) >= config["deltaT"]:
                publish = True
                publishTemps = sensorTemp.copy()               
        
		# Print to console the status of the 'publish' flag
        print("Publish:", publish)
        
        # Publish to MQTT Broker, if true
        if publish:
            publishTempsJSON = json.dumps(publishTemps)
            client.publish(config["sensorTempTopic"], publishTempsJSON, qos=0, retain=False)
            print("MQTT Published:", publishTemps)
            publish = False
        
        # Wait a bit before looping
        time.sleep(10)
