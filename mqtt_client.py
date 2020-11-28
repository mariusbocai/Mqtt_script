import json
import requests
import sys
import time
import datetime
import paho.mqtt.client as mqtt
import Adafruit_DHT
import logging

makeMeasurement = 0
S1AirTempRcvd = 0
S1AirHumiRcvd = 0
S1SoilHumiRcvd = 0
S2AirTempRcvd = 0
S2AirHumiRcvd = 0
S2SoilHumiRcvd = 0
CameraAirTempRcvd = 0
CameraAirHumiRcvd = 0


def Local_on_disconnect(client, userdata, rc):
    print("Got disconnected from local MQTT Broker; reason  "  +str(rc))
    logging.warning('Got disconnected from local MQTT Broker')
    #client.connected_flag=False
def Local_on_connect(client, userdata, flags, rc):
    if rc==0:
        #client.connected_flag=True #set flag
        print("Succesfully connected to local MQTT Broker")
	logging.info('Succesfully connected to local MQTT Broker with code %s', str(rc))
    else:
        print("Bad connection Returned code=",rc)
	logging.warning('Bad connection')
        #client.loop_stop()  
def Local_on_message(client, userdata, message):
    global makeMeasurement
    global S1AirTempRcvd
    global S1AirHumiRcvd
    global S1SoilHumiRcvd
    global S1AirTemp
    global S1AirHumi
    global S1SoilHumi
    global S2AirTempRcvd
    global S2AirHumiRcvd
    global S2SoilHumiRcvd
    global S2AirTemp
    global S2AirHumi
    global S2SoilHumi
    global CameraAirTemp
    global CameraAirHumi
    global CameraAirTempRcvd
    global CameraAirHumiRcvd
    print("Message received from local MQTT Broker topic:" ,str(message.topic))
    logging.info('Message received from local MQTT Broker topic: %s' ,str(message.topic))
    print("Message received from local MQTT Broker payload:" ,str(message.payload.decode("utf-8")))
    logging.info('Message received from local MQTT Broker payload: %s' ,str(message.payload.decode("utf-8")))
    print("Message qos=",message.qos)
    print("Message retain flag=",message.retain)
    makeMeasurement = 1 #set flag to announce a message has arrived
    if message.topic=="/Camera/Temperatura_aer":
        CameraAirTempRcvd = 1
        CameraAirTemp = message.payload
    if message.topic=="/Camera/Umiditate_aer":
        CameraAirHumiRcvd = 1
        CameraAirHumi = message.payload
    if message.topic=="/Solar1/Temperatura_aer":
        S1AirTempRcvd = 1
        S1AirTemp = message.payload
    if message.topic=="/Solar1/Umiditate_aer":
        S1AirHumiRcvd = 1
        S1AirHumi = message.payload
    if message.topic=="/Solar1/Umiditate_sol":
        S1SoilHumiRcvd = 1
        S1SoilHumi = message.payload
    if message.topic=="Solar2/AirTemp":
        S2AirTempRcvd = 1
        S2AirTemp = message.payload
    if message.topic=="Solar2/AirHumi":
        S2AirHumiRcvd = 1
        S2AirHumi = message.payload
    if message.topic=="Solar2/SoilHumi":
        S2SoilHumiRcvd = 1
        S2SoilHumi = message.payload
########################################

def on_disconnect(client, userdata, rc):
    print("Whoops... got disconnected from Thingsboard because:  "  +str(rc))
    logging.warning('Whoops... got disconnected from Thingsboard')
    client.connected_flag=False
#
def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        print("Yey! Connected to thingsBoard!")
        logging.info('Yey! Connected to thingsBoard!')
    else:
        print("Bad connection to ThingsBoard; Returned code=",rc)
	logging.warning('Bad connection to ThingsBoard')
        client.loop_stop()  

broker="demo.thingsboard.io"
port =1883
topic="v1/devices/me/telemetry"
username="CAJHmOZ5opvh7Pf4blpJ"
password=""
username_camera="LQzrm6entkKl6t1x7vfs"

MOSQUITTO_HOST = 'localhost'
MOSQUITTO_PORT = 1883

print('Welcome! Starting listening for messages on 6 topics. Will relay the info to ThingsBoard soon.')
print('Press Ctrl-C to quit.')
print('Connecting to MQTT on {0}'.format(MOSQUITTO_HOST))
logging.basicConfig(filename='/home/pi/Public/Mqtt_script/out.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.info('Script started')
mqttc = mqtt.Client("python_pub")
mqttc.on_message=Local_on_message
mqttc.on_connect=Local_on_connect
mqttc.on_disconnect=Local_on_disconnect
try:
    mqttc.connect(MOSQUITTO_HOST,MOSQUITTO_PORT);
    mqttc.loop_start()
    mqttc.subscribe("/Solar1/Temperatura_aer")
    mqttc.subscribe("/Solar1/Umiditate_aer")
    mqttc.subscribe("/Solar1/Umiditate_sol")
    mqttc.subscribe("Solar2/AirTemp")
    mqttc.subscribe("Solar2/AirHumi")
    mqttc.subscribe("Solar2/SoilHumi")
    mqttc.subscribe("/Camera/Temperatura_aer")
    mqttc.subscribe("/Camera/Umiditate_aer")
except Exception as e:
    print('Error connecting to the local mqtt broker: {0}'.format(e))
    logging.error('Error connecting to the local mqtt broker!')

print('Preparing the ThingsBoard connection:')
logging.info('Preparing the ThingsBoard connection')
mqtt.Client.connected_flag=False#create flag in class
client = mqtt.Client("python1")             #create new instance 
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.username_pw_set(username, password)

#time.sleep(3)

data=dict()
try:
    while True:
        #stay in a loop until a message has arrived
        while makeMeasurement==0:
            continue
        makeMeasurement = 0
        #got a measurement, now add it to the JSON struct
        if CameraAirTempRcvd==1:
            data["Temp_camera"] = CameraAirTemp
        if CameraAirHumiRcvd==1:
            data["Umid_camera"] = CameraAirHumi
        if S1AirTempRcvd==1:
            data["S1_AT"] = S1AirTemp
            #print('S1 Temperature: {0:0.2f} C'.format(S1AirTemp))
            print("S1AirTemp")
            #S1AirTempRcvd = 0
        if S1AirHumiRcvd==1:
            data["S1_AH"] = S1AirHumi
            #print('S1 Air Humidity:    {0:0.2f} %'.format(S1AirHumi))
            #S1AirHumiRcvd=0
        if S1SoilHumiRcvd==1:
            data["S1_SH"] = S1SoilHumi
            #print('S1 Soil Humidity:    {0:0.2f} %'.format(S1SoilHumi))
            #S1SoilHumiRcvd=0
        if S2AirTempRcvd==1:
            data["S2_AT"] = S2AirTemp
            print('S2 Temperature: {0:0.2f} C'.format(S2AirTemp))
            #S2AirTempRcvd = 0
        if S2AirHumiRcvd==1:
            data["S2_AH"] = S2AirHumi
            print('S2 Air Humidity:    {0:0.2f} %'.format(S2AirHumi))
            #S2AirHumiRcvd=0
        if S2SoilHumiRcvd==1:
            data["S2_SH"] = S2SoilHumi
            print('S1 Soil Humidity:    {0:0.2f} %'.format(S2SoilHumi))
            #S2SoilHumiRcvd=0
        currentdate = time.strftime('%Y-%m-%d %H:%M:%S')
        print('Date Time:   {0}'.format(currentdate))
        #print('Temperature: {0:0.2f} C'.format(temp))
        #print('Humidity:    {0:0.2f} %'.format(humidity))
        if S1AirTempRcvd==1 and S1AirHumiRcvd==1 and S1SoilHumiRcvd:
            print("Solar1 all 3 parameters have been succesfully received, attempting to send data to ThingsBoard")
            logging.info('Solar1 all 3 parameters have been succesfully received, attempting to send data to ThingsBoard')
            S1AirTempRcvd = 0
            S1AirHumiRcvd = 0
            S1SoilHumiRcvd = 0
            data_out=json.dumps(data)
            client.username_pw_set(username, password)
            client.connect(broker,port)           #establish connection
            time.sleep(5)
            client.loop_start()
            time.sleep(2)
            if True == client.connected_flag:
                time.sleep(1)
                (ret,mid) = client.publish(topic,data_out)
                if ret == mqtt.MQTT_ERR_SUCCESS:
                    print 'ThingsBoard Updated '
                    logging.info('ThingsBoard Updated')
                elif ret == mqtt.MQTT_ERR_NO_CONN:
                    print 'ERROR '
                    logging.error('Error updating Thingsboard')
                client.disconnect()
                client.loop_stop()

            if False == client.connected_flag:
                print("Disconnected")
                logging.error('Disconnected')
        if CameraAirTempRcvd==1 and CameraAirHumiRcvd==1:
            logging.info('Camera parameters have been succesfully received, attempting to send data to ThingsBoard')
            CameraAirTempRcvd = 0
            CameraAirHumiRcvd = 0
            data_out=json.dumps(data)
            client.username_pw_set(username_camera, password)
            client.connect(broker,port)           #establish connection
            time.sleep(5)
            client.loop_start()
            time.sleep(2)
            if True == client.connected_flag:
                time.sleep(1)
                (ret,mid) = client.publish(topic,data_out)
                if ret == mqtt.MQTT_ERR_SUCCESS:
                    logging.info('Camera ThingsBoard Updated')
                elif ret == mqtt.MQTT_ERR_NO_CONN:
                    logging.error('Error updating Thingsboard Camera')
                client.disconnect()
                client.loop_stop()

            if False == client.connected_flag:
                print("Disconnected")
                logging.error('Disconnected')

except Exception,e:
    mqttc.loop_stop()
    mqttc.disconnect()
    client.disconnect()
    print('Append error, logging in again: ' + str(e))

