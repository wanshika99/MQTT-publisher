import time
import paho.mqtt.client as paho
import numpy as np
import threading
import time
import random

broker = '5g-vue.projects.uom.lk'
port = 1883
username = 'iot_user'
password = 'iot@1234'
client_id = 'group_09w'

topic = "/powerplant_group_09"

p_val = {}
direction = ["N","NE","E","SE","S","SW","W","NW"]

def sensor_value(id="", low=0, high=0, a=0, start=""):

    if id == topic + "/Wind_Direction":
        r=random.randint(0,7)
        return direction[r]
    
    elif id == topic + "/Power_Gen" or  id == topic + "/Battery_Capacity":
        if id in p_val: previous = p_val[id]
        elif start == 0: previous = low

        x = random.randint(1,4)
        val = previous + x*previous/15
        if val > high:
            val = high
        p_val[id] = val
        val = round(val, 3)
        return val

    else: 
        n = random.randint(low,high) 
    
        if id in p_val: previous = p_val[id]
        elif start == "": previous = (low + high)/2
        else: previous = start  
            
        val=a*n+(1-a)*previous
        p_val[id] = val
        val = round(val, 3)
        val = min(max(low, val), high)
        
        return val

def on_message(client, userdata, message):
    time.sleep(1)
    print("received message =", str(message.payload.decode("utf-8")))
    
    
def mqtt_client():
    client = paho.Client(client_id)
    client.username_pw_set(username, password)
    client.on_message = on_message
    print("connecting to broker ", broker)
    client.connect(broker)
    return client

def thread_1(name, seed, cycles):
    sensor_topic = topic + "/Power_Gen"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic, low=300, high=800, a=0.7, start=0)
        print("publishing:",sensor_topic +"-",str(val)+"kW")
        client.publish(sensor_topic, str(val)+"kW") 
        time.sleep(20)

def thread_2(name, seed, cycles):
    sensor_topic = topic + "/Temp01"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic, low=18, high=40, a=0.5, start=0)
        print("publishing:",sensor_topic +"-",str(val)+"°C")
        client.publish(sensor_topic, str(val)+"°C") 
        time.sleep(21)

def thread_3(name, seed, cycles):
    sensor_topic = topic + "/Temp02"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic, low=40, high=80, a=0.7, start=0)
        print("publishing:",sensor_topic +"-",str(val)+"°C")
        client.publish(sensor_topic, str(val)+"°C") 
        time.sleep(19)

def thread_4(name, seed, cycles):
    sensor_topic = topic + "/Humidity01"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic, low=30, high=100, a=0.6, start=0)
        print("publishing:",sensor_topic +"-",str(val)+"%")
        client.publish(sensor_topic, str(val)+"%") 
        time.sleep(23)

def thread_5(name, seed, cycles):
    sensor_topic = topic + "/Humidity02"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic, low=50, high=100, a=0.5, start=0)
        print("publishing:",sensor_topic +"-",str(val)+"%")
        client.publish(sensor_topic, str(val)+"%") 
        time.sleep(12)

def thread_6(name, seed, cycles):
    sensor_topic = topic + "/Wind_Speed"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic, low=10, high=100, a=0.4, start=0)
        print("publishing:",sensor_topic +"-",str(val)+"km/h")
        client.publish(sensor_topic, str(val)+"km/h") 
        time.sleep(17)

def thread_7(name, seed, cycles):
    sensor_topic = topic + "/Wind_Direction"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic)
        print("publishing:",sensor_topic +"-",str(val))
        client.publish(sensor_topic, str(val)) 
        time.sleep(27)

def thread_8(name, seed, cycles):
    sensor_topic = topic + "/Sunlight_Intensity01"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic, low=0, high=65535, a=0.8, start=0)
        print("publishing:",sensor_topic +"-",str(val)+"lx")
        client.publish(sensor_topic, str(val)+"lx") 
        time.sleep(11)

def thread_9(name, seed, cycles):
    sensor_topic = topic + "/Sunlight_Intensity02"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic, low=100, high=3000, a=0.4, start=0)
        print("publishing:",sensor_topic +"-",str(val)+"lx")
        client.publish(sensor_topic, str(val)+"lx") 
        time.sleep(13)

def thread_10(name, seed, cycles):
    sensor_topic = topic + "/Battery_Capacity"

    for i in range(cycles):
        val = sensor_value(id=sensor_topic, low=5, high=25, a=0.7, start=0)
        print("publishing:",sensor_topic +"-",str(val)+"kWh")
        client.publish(sensor_topic, str(val)+"kWh") 
        time.sleep(29)

if True or _name_ == "_main_":

    client = mqtt_client()

    client.loop_start()  
    # client.loop_forever()
    print("subscribing ")
    client.subscribe(topic)

    t1 = threading.Thread(target=thread_1, args=(1, 1, 5))
    t2 = threading.Thread(target=thread_2, args=(2, 2, 5))
    t3 = threading.Thread(target=thread_3, args=(3, 3, 5))
    t4 = threading.Thread(target=thread_4, args=(4, 4, 5))
    t5 = threading.Thread(target=thread_5, args=(5, 5, 5))
    t6 = threading.Thread(target=thread_6, args=(6, 6, 5))
    t7 = threading.Thread(target=thread_7, args=(7, 7, 5))
    t8 = threading.Thread(target=thread_8, args=(8, 8, 5))
    t9 = threading.Thread(target=thread_9, args=(9, 9, 5))
    t10 = threading.Thread(target=thread_10, args=(10, 10, 5))
   

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t9.start()
    t10.start()

