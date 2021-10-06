    #! /usr/bin/python3

"""
This script subscribes the data from the MQTT broker and parses it before storing
it in the timescaledb
"""
import os
import sys
import base64
import threading
import paho.mqtt.client as mqtt
import time
import json 
import pytz
import datetime
import logging
import socket
import requests
from logging.handlers import SysLogHandler
from sqlalchemy import create_engine
from sqlalchemy.schema import Table,MetaData
from sqlalchemy.sql import select
from sqlalchemy import insert,update,delete,select,column

RDS_DB_NAME="Pragyaam_DIY"
RDS_USERNAME="admin"
RDS_PASSWORD="47N9zoo2s&4p"
RDS_HOST="grid-dev-rds.cvt6u27fv795.ap-south-1.rds.amazonaws.com"

ORGANZATION_ID = "pragyaam"
BROKER_ENDPOINT = "mqtt.workongrid.com"
BROKER_UNAME = "pragyaam"
BROKER_PWD = "Omni#2021"
PORT_NO = 1883

worksheetNames = ["Device Meter Data", "LCU Status", "RFC Status", "Device Action Response" , "Application List"]

def get_database_from_organisation(organisation_id):
    """
     Function that returns the database of the respective organisation from database `Pragyaam_DIY` 
     and table `organizations`
    """

    connection_string = "mysql://{}:{}@{}/{}".format(RDS_USERNAME,RDS_PASSWORD,RDS_HOST,RDS_DB_NAME)
    engine = create_engine(connection_string,pool_recycle=3600,convert_unicode=True,encoding='utf-8')
    connection = engine.connect()
    meta = MetaData()

    worksheet = Table("organizations",meta ,autoload=True, autoload_with=connection)
    statement = select([worksheet.c['database_name']]).where(worksheet.c['organization_id']==organisation_id)

    result = connection.execute(statement)
    database_name = result.fetchone()
    connection.close()
    return database_name[0]

def connect_with_database(organisation_id):
    """
     Function that connect with the database of the respective organisation
     sample connection string
     `mysql://pragyaam:password@grid-dev.cluster-cvt6u27fv795.ap-south-1.rds.amazonaws.com/pragyaam709184`
    """
    
    database_name = get_database_from_organisation(organisation_id)
    print("connect_with_database:",database_name)

    connection_string = "mysql://{}:{}@{}/{}?charset=utf8".format(RDS_USERNAME,RDS_PASSWORD,RDS_HOST,database_name) 
    engine = create_engine(connection_string,pool_recycle=3600,pool_timeout=600,convert_unicode=True,case_sensitive=False)

    return engine

def get_worksheet(engine,worksheet_id):
    """
     Function that returns the worksheet as a sqlalchemy object
    """
    meta = MetaData()
   
    try:
        worksheet = Table(worksheet_id,meta ,autoload=True, autoload_with=engine)
    except exc.NoSuchTableError:
        return {"status":"failed","message":"Some worksheet you are trying to access does not exist"}
    return worksheet

def data_parser(topic,data):
    payload = json.loads(data["payload"])

    base64_data = payload["data"]
    base64_bytes = base64_data.encode('ascii')
    meter_data_bytes = base64.b64decode(base64_bytes)
    meter_data = meter_data_bytes.decode('ascii')
    splitLst = meter_data.split("|")
    # logger.info(splitLst)

    if "L" in splitLst[0]:
        # logger.info("LCU DATA")
        parse_lcu_data(payload,splitLst)

    elif "R" in splitLst[0]:
        parse_rfc_data(payload,splitLst)

def parse_lcu_data(payload,data):
    zone = pytz.timezone('Asia/Kolkata')
    currentTime = datetime.datetime.now(zone).strftime("%Y-%m-%d %H:%M:%S")

    #Meter Data
    if data[0][1] == '6':
        row = {}
        row["applicationid"]=payload["applicationID"]  
        row["deveui"]=payload["devEUI"]   
        row["total_kwh"]=float(data[2])/1000
        row["frequency"]=float(data[3])/100
        row["voltage"]=float(data[4])/10
        row["current"]=float(data[5])/1000
        row["power_factor"]=float(data[6])/100
        row["active_power"]=float(data[7])/1000
        row["apprent_power"]=float(data[8])/1000
        row["reactive_power"]=float(data[9].split("*")[0])/1000
        row["response_code"]=data[0]
        row["raw_data"] = payload["data"]
        row["uploaded_time"] = currentTime

        meter_data_semaphore.acquire()    
        meter_data.append(row)
        meter_data_semaphore.release()
        logger.info("Pushed")
    
    # Set Schedule and ON/OFF Response
    elif (data[0][1] == '0') or (data[0][1] == '1') or (data[0][1] == '3') or (data[0][1] == '4') :
        row = {}
        row["applicationid"]=payload["applicationID"]
        row["fcnt"]=payload["fCnt"]
        row["deveui"]=payload["devEUI"]
        row["response_code"]=str(data[0])
        row["raw_data"] = payload["data"]
        row["uploaded_time"] = currentTime

        response_data_semaphore.acquire()    
        response_data.append(row)
        response_data_semaphore.release()
        logger.info("Pushed")

    #Status Command
    elif data[0][1] == '5':
        row = {}
        row["applicationid"]=payload["applicationID"]
        row["fcnt"]=payload["fCnt"]
        row["deveui"]=payload["devEUI"]
        row["rssi"]=payload["rxInfo"][0]["rssi"]
        row["lorasnr"]=payload["rxInfo"][0]["loRaSNR"]
        row["mode"] = data[2]
        row["raw_data"] = payload["data"]

        if row["mode"] == 'S':
            row["current_date"]=data[3]
            row["current_time"]=data[4]
            row["start_time"]=data[5]
            row["relay_status"]=data[7] 
            row["end_time"]=data[6]
            row["dimming_value"]=data[8]
            row["meter_interval"]=data[9].split("*")[0]
            row["uploaded_time"] = currentTime
        else:
            row["current_date"]=data[3]
            row["current_time"]=data[4]
            row["start_time"]=None
            row["end_time"]=None
            row["relay_status"]=data[5] 
            row["dimming_value"]=data[6]
            row["meter_interval"]=data[7].split("*")[0]
            row["uploaded_time"] = currentTime

        lcu_status_data_semaphore.acquire()
        lcu_status_data.append(row)
        lcu_status_data_semaphore.release()
        logger.info("Pushed")


def parse_rfc_data(payload,data):
    zone = pytz.timezone('Asia/Kolkata')
    currentTime = datetime.datetime.now(zone).strftime("%Y-%m-%d %H:%M:%S")

    #Meter Data
    if data[0][1] == '1':
        row = {}
        row["applicationid"]=payload["applicationID"]  
        row["deveui"]=payload["devEUI"]   
        row["total_kwh"]=float(data[2])/1000
        row["frequency"]=float(data[3])/100
        row["voltage"]=float(data[4])/10
        row["current"]=float(data[5])/1000
        row["power_factor"]=float(data[6])/100
        row["active_power"]=float(data[7])/1000
        row["apprent_power"]=float(data[8])/1000
        row["reactive_power"]=float(data[9].split("*")[0])/1000
        row["response_code"]=data[0]
        row["raw_data"] = payload["data"]
        row["uploaded_time"] = currentTime

        meter_data_semaphore.acquire()    
        meter_data.append(row)
        logger.info("Pushed")
        meter_data_semaphore.release()    


    # Set Schedule and ON/OFF Response
    elif (data[0][1] + data[0][2] == '01') or (data[0][1] + data[0][2] == '02') or (data[0][1] + data[0][2] == '00') or (data[0][1] + data[0][2] == '04'):
        row = {}
        row["applicationid"]=payload["applicationID"]
        row["fcnt"]=payload["fCnt"]
        row["deveui"]=payload["devEUI"]
        row["response_code"]=str(data[0])
        row["raw_data"] = payload["data"]
        row["uploaded_time"] = currentTime

        response_data_semaphore.acquire()    
        response_data.append(row)
        response_data_semaphore.release()
        logger.info("Pushed")    

    # RFC Status 
    elif data[0][2] == '8':
        row={}
        row["applicationid"]=payload["applicationID"]
        row["fcnt"]=payload["fCnt"]
        row["deveui"]=payload["devEUI"]
        row["rssi"]=payload["rxInfo"][0]["rssi"]
        row["lorasnr"]=payload["rxInfo"][0]["loRaSNR"]
        row["mode"]=data[2]
        row["phase"]=data[3]
        row["relay_status"]=int(data[4])
        row["door_sensor"]=int(data[5])
        row["battery"]=int(data[6])
        row["meter_interval"]=int(data[7])
        row["current_date"]=data[8]
        row["current_time"]=data[9]
        row["raw_data"] = payload["data"]
        row["uploaded_time"] = currentTime

        rfc_status_data_semaphore.acquire()    
        rfc_status_data.append(row)
        rfc_status_data_semaphore.release()
        logger.info("Pushed") 


def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.connected_flag=True #set flag
        
        logger.info("connected OK")
    
        logger.info("Subscribing to the topic - " + str(userdata) )
        client.subscribe(userdata + 'test')
        
    else:
        logger.info("Bad connection Returned code=",rc)    

def on_message(client, userdata, message):  
    data ={"payload":message.payload.decode("utf-8")}
    
    x = threading.Thread(target=data_parser, args=(message.topic,data,))
    x.start()

def on_subscribe(client, userdata, mid, granted_qos):   
    logger.info("Subscribed to the topic.")

def connectToMqtt():
    mqtt.Client.connected_flag=False
    clients = []
    for i in range(0,nClients):
        topic = 'application/' + str(application_ids[i]) + '/device/+/rx'

        client = mqtt.Client(client_id="data-parser-test-"+str(i),userdata=topic)   
        client.on_connect=on_connect
        client.loop_start()
        client.on_message = on_message
        client.on_subscribe = on_subscribe
        client.username_pw_set(username=BROKER_UNAME,password=BROKER_PWD)
        client.connect(BROKER_ENDPOINT,port=PORT_NO)

        count = 0
        while not client.connected_flag: #wait in loop
            if count<5:
                logger.info("In wait loop")
                time.sleep(1)
            else:
                continue

        clients.append(client)

    return clients

def fetchWorksheetIds():
    logger.info("Fetching Worksheet Ids")

    worksheet_ids = {}

    with engine.begin() as connect:
        table = get_worksheet(engine,"worksheets")

        for ws in worksheetNames:
            statement = select([table.c.worksheet_id]).where(table.c.worksheet_name == ws)   
            result = connect.execute(statement)
            data = result.fetchall()
            worksheet_ids[ws] = data[0][0]

    return worksheet_ids

def fetchApplicationIds():
    with engine.begin() as connect:
        table = get_worksheet(engine,worksheet_ids["Application List"])   
        statement = select([column('application_id')]).select_from(table)
        result = connect.execute(statement)
        data = result.fetchall()

    application_ids=[]
    for tup in data:
        application_ids.append(tup[0])
    
    return application_ids 

def spawnUploadThread():
    x = threading.Thread(target=upload_data, args=())
    x.start()

def upload_data():
    try:
        logger.info("Upload data Thread started.")
        while True:
            if len(meter_data)>0:
                table = get_worksheet(engine,worksheet_ids["Device Meter Data"])
                ins = insert(table)

                meter_data_semaphore.acquire()
                with engine.begin() as connect:
                    connect.execute(ins,meter_data)

                logger.info("Meter data was uploaded.")
                meter_data.clear()
                meter_data_semaphore.release()

            if len(response_data)>0:
                table = get_worksheet(engine,worksheet_ids["Device Action Response"])
                ins = insert(table)

                response_data_semaphore.acquire()
                with engine.begin() as connect:
                    connect.execute(ins,response_data)

                logger.info("Response data was uploaded.")
                response_data.clear()
                response_data_semaphore.release()

            if len(lcu_status_data)>0:
                table = get_worksheet(engine,worksheet_ids["LCU Status"])
                ins = insert(table)

                lcu_status_data_semaphore.acquire()
                with engine.begin() as connect:
                    connect.execute(ins,lcu_status_data)

                logger.info("LCU status data was uploaded.")
                lcu_status_data.clear()
                lcu_status_data_semaphore.release()

            if len(rfc_status_data)>0:
                table = get_worksheet(engine,worksheet_ids["RFC Status"])
                ins = insert(table)

                rfc_status_data_semaphore.acquire()
                with engine.begin() as connect:
                    connect.execute(ins,rfc_status_data)

                logger.info("RFC status data was uploaded.")
                rfc_status_data.clear()
                rfc_status_data_semaphore.release()
            time.sleep(5)
    
    except Exception as err:
        logger.error("Upload data thread failed due to - " + str(err))

        meter_data.clear()
        response_data.clear()
        lcu_status_data.clear()
        rfc_status_data.clear()

        meter_data_semaphore.release()
        repsonse_data_semaphore.release()
        lcu_status_data_semaphore.release()
        rfc_status_data_semaphore.release()

        logger.info("Restarting Upload data thread")
        spawnUploadThread()

def spawnHeartBeatThread():
    x = threading.Thread(target=heartbeat_data, args=())
    x.start()

def heartbeat_data():
    logger.info("Heart Beat thread started.")
    while True:
        try:
            requests.get("https://betteruptime.com/api/v1/heartbeat/hqqymYjSiqDhgVyJ4SN6pQXf")
        except Exception as err:
            logger.error("Failed to send the heartbeat data due to foloowing exception - " + str(err))
        time.sleep(30)

if __name__ == "__main__":
    #Setup logging 
    logger = logging.getLogger("mqtt-data-parser")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    try:
        #Spawn the main thread
        logger.info("Main Thread Running")

        #Get the database engine
        engine = connect_with_database(ORGANZATION_ID)
        connection = engine.connect()

        #Fetch the worksheet ids
        worksheet_ids = fetchWorksheetIds()
        logger.info(worksheet_ids)

        #Fetch the application ids
        application_ids=fetchApplicationIds()
        logger.info(application_ids)

        #Connect to the MQTT broker
        nClients=len(application_ids)
        clients = connectToMqtt()

        if len(clients)>0:
            #Buffers to hold the incoming data
            meter_data , response_data , lcu_status_data , rfc_status_data= [],[],[],[]

            meter_data_semaphore = threading.Semaphore(1)
            response_data_semaphore = threading.Semaphore(1)
            lcu_status_data_semaphore = threading.Semaphore(1)
            rfc_status_data_semaphore = threading.Semaphore(1)

            #Spawn new thread which will handle the upload of data
            spawnUploadThread()

            #Spawn new thread which will send the heartbeat data to the cloud
            spawnHeartBeatThread()

            #Wait for the data to be received
            while True:
                pass
        else:
            raise Exception("Not able to connect to the broker")
    
    except Exception as err:
        logger.error("Exception occured while running Main Thread - " + str(err))

        connection.close()
        engine.dispose()  
        for client in clients:
            client.loop_stop()

        logger.info("Exiting gracefully")
        
