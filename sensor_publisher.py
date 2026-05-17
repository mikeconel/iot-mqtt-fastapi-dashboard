import paho.mqtt.client as mqtt
import json, time, random

#client = mqtt.Client()
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
#client.connect("localhost", 1883, 60)
broker = "broker.hivemq.com"
client.connect(broker, 1883, 60)

while True:
    sensorData = {
        "sensor_id": "room_1",
        "temperature": round(random.uniform(11, 35), 1),
        "humidity": round(random.uniform(30, 80), 1),
        "motion": random.choice([0, 1]),
        "ts": time.time()  # MISSING: Added timestamp field
    }
    
    client.publish("sensors/bis_room_2", json.dumps(sensorData))
    time.sleep(2)