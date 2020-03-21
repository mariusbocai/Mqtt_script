#!/usr/bin/python
# DHT Sensor Data-logging to MQTT Temperature channel

# Requies a Mosquitto Server Install On the destination.

# Copyright (c) 2014 Adafruit Industries
# Author: Tony DiCola
# MQTT Encahncements: David Cole (2016)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import json
import requests
import sys
import time
import datetime
import paho.mqtt.client as mqtt
import Adafruit_DHT

makeMeasurement = 0


def on_disconnect(client, userdata, rc):
    print("disconnecting reason  "  +str(rc))
    client.connected_flag=False
#
def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        print("connected to thingsBoard OK")
    else:
        print("Bad connection Returned code=",rc)
        client.loop_stop()  
############
def on_message(client, userdata, message):
    global makeMeasurement
    print("message received " ,str(message.payload.decode("utf-8")))
    makeMeasurement = 1
    #print("message topic=",message.topic)
    #print("message qos=",message.qos)
    #print("message retain flag=",message.retain)
########################################

#==================================================================================================================================================
#Usage
#python mqtt.channel.py <temp topic> <humidity topic> <gpio pin> <optional update frequency>
# eg python mqtt.channel.py 'cupboard/temperature1' 'cupboard/humidity1' 4
# will start an instance using 'cupboard/temperature1' as the temperature topic, and using gpio port 4 to talk to a DHT22 sensor
# it will use the default update time of 300 secons.
#usage: ./mqtt.dhtsensor.py 'cupboard/temperature1' 'cupboard/humidity1' 4 50
#==================================================================================================================================================

# Type of sensor, can be Adafruit_DHT.DHT11, Adafruit_DHT.DHT22, or Adafruit_DHT.AM2302.
DHT_TYPE = Adafruit_DHT.DHT11

# Example of sensor connected to Raspberry Pi pin 23
DHT_PIN  = sys.argv[3]
# Example of sensor connected to Beaglebone Black pin P8_11
#DHT_PIN  = 'P8_11'

if (len(sys.argv) < 2):
   raise  ValueError('Input arguments of mqtt channel temperature humidity not passed')

broker="demo.thingsboard.io"
port =1883
topic="v1/devices/me/telemetry"
#need to edit user name 
#username="Apx1r8fNNQbr9JILm3" #device house
username="CAJHmOZ5opvh7Pf4blpJ"
password=""
usernameH="5IJ3JlUdPeU294s4eOOj"

MOSQUITTO_HOST = 'localhost'
MOSQUITTO_PORT = 1883
MOSQUITTO_TEMP_MSG = str(sys.argv[1]) # Old channel name in here
MOSQUITTO_HUMI_MSG = str(sys.argv[2]) # Old channel name now passed by argument
print('Mosquitto Temp MSG {0}'.format(MOSQUITTO_TEMP_MSG))
print('Mosquitto Humidity MSG {0}'.format(MOSQUITTO_HUMI_MSG))

# How long to wait (in seconds) between measurements.
print "Args length: " + str(len(sys.argv))
FREQUENCY_SECONDS      = 300

if (len(sys.argv) > 4):
	FREQUENCY_SECONDS = float(sys.argv[4])

mqtt.Client.connected_flag=False#create flag in class
print('Logging sensor measurements to {0} every {1} seconds.'.format('MQTT', FREQUENCY_SECONDS))
print('Press Ctrl-C to quit.')
print('Connecting to MQTT on {0}'.format(MOSQUITTO_HOST))
mqttc = mqtt.Client("python_pub")
client = mqtt.Client("python1")             #create new instance 
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.username_pw_set(username, password)

time.sleep(3)
mqttc.on_message=on_message #attach function to callback
mqttc.connect(MOSQUITTO_HOST,MOSQUITTO_PORT);
print("Subscribing to topic","status/dht11")
mqttc.loop_start()
mqttc.subscribe("status/dht11")
data=dict()
try:

    while True:
        # Attempt to get sensor reading.
        humidity, temp = Adafruit_DHT.read(DHT_TYPE, DHT_PIN)

        # Skip to the next reading if a valid measurement couldn't be taken.
        # This might happen if the CPU is under a lot of load and the sensor
        # can't be reliably read (timing is critical to read the sensor).
        if humidity is None or temp is None:
           time.sleep(2)
           continue
	if humidity > 100:
	    continue

        currentdate = time.strftime('%Y-%m-%d %H:%M:%S')
        print('Date Time:   {0}'.format(currentdate))
        print('Temperature: {0:0.2f} C'.format(temp))
        print('Humidity:    {0:0.2f} %'.format(humidity))

        # Publish to the MQTT channel
        try:
     	    #connect to MQTT once
            #mqttc.connect(MOSQUITTO_HOST,MOSQUITTO_PORT);
            print 'Updating {0}'.format(MOSQUITTO_TEMP_MSG)
            (result1,mid) = mqttc.publish(MOSQUITTO_TEMP_MSG,temp)
	    data["Temperatura"]=temp
	    data["Umiditate"]=humidity
	    data_out=json.dumps(data)
	    client.username_pw_set(username, password)
	    client.connect(broker,port)           #establish connection
	    client.loop_start()
	    while not client.connected_flag: #wait in loop
		time.sleep(1)
	    (ret,mid) = client.publish(topic,data_out)
	    if ret == mqtt.MQTT_ERR_SUCCESS:
	        print 'ThingsBoard Updated '
            elif ret == mqtt.MQTT_ERR_NO_CONN:
                print 'ERROR '
	    client.disconnect()
	    client.loop_stop()
	    if False == client.connected_flag:
		print("Disconnected")
            print 'Updating {0}'.format(MOSQUITTO_HUMI_MSG)
	    time.sleep(1)
            (result2,mid) = mqttc.publish(MOSQUITTO_HUMI_MSG,humidity)
            print 'MQTT Updated result {0} and {1}'.format(result1,result2)
            if result1 == 1 or result2 == 1:
                raise ValueError('Result for one message was not 0')
	    #do not disconnect anymore since we are expectin also subscribed values
        #mqttc.disconnect()

        except Exception,e:
            # Error appending data, most likely because credentials are stale.
            # Null out the worksheet so a login is performed at the top of the loop.
            mqttc.loop_stop()
	    mqttc.disconnect()
	    client.disconnect()
            print('Append error, logging in again: ' + str(e))
            continue

        # Wait 30 seconds before continuing
        print('Wrote a message tp MQQTT')
        #time.sleep(FREQUENCY_SECONDS)
        #wait in a loop for 300 seconds or until a read is requested
        t_end = time.time() + FREQUENCY_SECONDS
        while time.time() < t_end:
            if makeMeasurement == 1:
                makeMeasurement = 0
                break

except Exception as e:
    print('Error connecting to the moqtt server: {0}'.format(e))
