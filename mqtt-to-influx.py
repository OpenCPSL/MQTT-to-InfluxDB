# -*- coding: utf-8 -*-
"""
CPSL - MQTT-to-InfluxDB
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
The purpose of this programme is to subscribe to an MQTT broker and
await new messages on configured topics.  When a new message is received,
it will be written to an InfluxDB database.

Several functions have been found on or were inspired by:
http://www.steves-internet-guide.com/into-mqtt-python-client/
"""

import paho.mqtt.client as mqtt
import json
import time
import random
import logging

import threading
from queue import Queue

from datetime import datetime,timezone
from influxdb import InfluxDBClient



# Callback function on connection to the MQTT broker
def on_connect(client, userdata, flags, rc):
   """
   set the bad connection flag for rc >0, Sets onnected_flag if connected ok
   also subscribes to topics
   """
   logging.debug("Connected flags"+str(flags)+"result code "\
    +str(rc)+"client1_id")
       
   if rc == 0:
      
      client.connected_flag=True #old clients use this
      client.bad_connection_flag=False
      
      if client.sub_topic != "": #single topic
         logging.debug("subscribing "+str(client.sub_topic))
         print("subscribing in on_connect")
         topic=client.sub_topic
         if client.sub_qos!=0:
            qos=client.sub_qos
         client.subscribe(topic,qos)
         
      elif client.sub_topics != "":
         #print("subscribing in on_connect multiple")
         client.subscribe(client.sub_topics)

   else:
     print("set bad connection flag")
     client.bad_connection_flag=True #
     client.bad_count +=1
     client.connected_flag=False #
     
     
# Callback function to handle new messages from MQTT broker     
def on_message(client,userdata, msg):
    topic=msg.topic
    m_decode=str(msg.payload.decode("utf-8","ignore"))
    message_handler(client,m_decode,topic)

# Message handler to process the new message    
def message_handler(client,msg,topic):
    data = dict()
    tnow = datetime.now().astimezone().isoformat('T', 'seconds')

    try:
        msg=json.loads(msg)     # Convert JSON to Dict
    except:
        pass

    data["time"]    = tnow
    data["topic"]   = topic
    data["message"] = msg
    
    if options["storeChangesOnly"]:
        if has_changed(client,topic,msg):
            client.q.put(data)  # Put messages on queue
    else:
        client.q.put(data)      # Put messages on queue

# Check if latest message has changed since previous message
def has_changed(client,topic,msg):
    topic2=topic.lower()
    if topic2.find("control")!=-1:
        return False
    if topic in client.last_message:
        if client.last_message[topic]==msg:
            return False
    client.last_message[topic]=msg
    return True






# Load the config file    
def loadConfig(configFile="config.json"):
    
    with open(configFile) as jsonFile:
        return json.load(jsonFile)


# Fix station names to preferred names for Database
def stationNameFix(stationIn, stationNameFile="stationNames.json"):

    with open(stationNameFile) as jsonFile:
        stationNames = json.load(jsonFile)
       
    try:
        stationOut = stationNames[stationIn]
    except:
        stationOut = stationIn
    
    return(stationOut)



# Make sure every numeric value is a float
def floatDict(dictIn):  
    
    dictOut = dict()
    for key in dictIn:
        try:
            dictOut[key] = float(dictIn[key])
        except:
            pass

    return(dictOut)


# InfluxDB Worker thread to pass data from message queue to InfluxDB
def influxDB_worker():
    """runs in own thread to pass data from queue to InfluxDB"""

    client = InfluxDBClient(host=options["InfluxDB_host"], port=options["InfluxDB_port"])
    print(client.get_list_database())

    while influxDB_worker_flag:
        time.sleep(0.01)
        while not q.empty():
            results = q.get()
        
            # Time must be in ISO format
            timeISO  = results["time"]
                    
            # Get the sensor name from last field of topic
            topic   = results["topic"]
            sensor = topic.split('/')[-1]
            
            # Fix the sensor name
            sensor = stationNameFix(sensor)    
            
            # 'message' is the sensor payload
            message = results["message"]
            
            # Check for blank payloads
            if len(message) > 0:
            
                influxDict = {}
                
                # Convert all values to floats, due to InfluxDB type fussiness
                message = floatDict(message)  
                
                # Create the structure for insersion into InfluxDB
                influxDict["time"] = timeISO
                influxDict["measurement"] = sensor
                influxDict["fields"] = message
                        
        
            print(influxDict)
                    
            influxList = []
            influxList.append(influxDict)
        
            if enableDB == True:
                               
                client.switch_database(options["InfluxDB_database"])
                
                if writeDB == True:
                    
                    if len(influxDict["fields"]) > 0:
                    
                        client.write_points(influxList)
                        
                        print("Write to InfluxDB done!")
                
                #print(client.query('SELECT "temperature" FROM "Outside"'))





# ######################################
# ------------ MAIN PROGRAM ------------
# ######################################

# The '__main__' function sets up the MQTT Client, which operates 
# via callback functions. The InfluxDB functions operate as a 
# separate thread in the 'influxDB_worker' function.

# New data from the MQTT client callback is added to a queue, 'q'.
# The 'influxDB_worker' monitors 'q' and writes new messages to the
# InfluxDB database.

if __name__ == "__main__":
    print("MQTT to InfluxDB Logger")
    
    influxDB_worker_flag    = True
    
    # For debug, set to false to prevent unwanted writes
    enableDB = True
    writeDB  = True
    
    # Set up queue for incoming MQTT messages / outgoing InfluxDB writes
    q = Queue()
    
    # Load Config
    options = loadConfig()
    
    # Fix "topics" to be tuples of the form "(topic, QoS)"
    for index, topic in enumerate(options["topics"]):
        options["topics"][index] = tuple(topic)
    
    
    # Set up a client name "cname"
    if not options["cname"]:
        r=random.randrange(1,10000)
        cname="logger-"+str(r)
    else:
        cname="logger-"+str(options["cname"])
    
    logging.info("creating client"+cname)
    
    print("Initialising clients")
    logging.info("initialising clients")
    client = mqtt.Client()
    client.cname = cname
    client.on_connect        = on_connect        #attach function to callback
    client.on_message        = on_message        #attach function to callback
    #client.on_disconnect    = on_disconnect
    #client.on_subscribe     = on_subscribe
    
    if options["username"] !="":
        client.username_pw_set(options["username"], options["password"])
    
    client.sub_topic    = ""                                # Redundant
    client.sub_topics   = options["topics"]
    client.broker       = options["broker"]
    client.port         = options["port"]
    
    client.last_message = dict()
    client.q            = q                                 # make queue available as part of client
    
    
    # Set up the InfluxDB Worker thread, to process messages from the MQTT queue
    t = threading.Thread(target = influxDB_worker)            
    t.start()     
    
    
    try:
        res = client.connect(client.broker,client.port)       # connect to broker
        client.loop_start()                                 # start loop
    
    except:
        logging.debug("connection to ",client.broker," failed")
        raise SystemExit("connection failed")
    try:
        while True:
            time.sleep(1)
            pass
    
    except KeyboardInterrupt:
        print("interrrupted by keyboard")
    
    client.loop_stop()                                      # stop loop
    influxDB_worker_flag = False                            # stop logging thread
