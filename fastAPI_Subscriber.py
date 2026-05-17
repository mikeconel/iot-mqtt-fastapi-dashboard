from fastapi import FastAPI, HTTPException
import sqlite3
import json
import paho.mqtt.client as mqtt
from datetime import datetime
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect("broker.hivemq.com", 1883, 60)
    client.subscribe("sensors/bis_room_2")
    client.loop_start()
    yield
    client.loop_stop()  # Clean shutdown

app = FastAPI(title="BSI IoT Dashboard", lifespan=lifespan)

conn = sqlite3.connect("sensor.db", check_same_thread=False)

conn.execute("""CREATE TABLE IF NOT EXISTS readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    device_id TEXT, 
    temp REAL, 
    humidity REAL, 
    motion INT, 
    ts REAL
)""")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
        print(data)
        conn.execute(
            "INSERT INTO readings (device_id, temp, humidity, motion, ts) VALUES (?,?,?,?,?)",
            (data["sensor_id"], data["temperature"], data["humidity"], data["motion"], data["ts"])
        )
        conn.commit()

    except Exception as e:
        print(f"[MQTT] Failed to process message: {e}")


@app.get("/readings")
def get_readings(limit: int = 100):
    cursor = conn.execute("SELECT * FROM readings ORDER BY ts DESC LIMIT ?", (limit,))
    rows = cursor.fetchall()
    
    if not rows:
        raise HTTPException(status_code=404, detail="No sensor data found")
    else:
    # Convert to list of dictionaries with formatted timestamp
        readings = []
        for row in rows:
            readings.append({
                "id": row[0],
                "device_id": row[1],
                "temp": row[2],
                "humidity": row[3],
                "motion": row[4],
                "ts": row[5],  # Keep original for calculations
                "ts_readable": datetime.fromtimestamp(row[5]).strftime('%Y-%m-%d %H:%M:%S')
            })
    
    return readings

from datetime import datetime, timedelta

@app.delete("/readings/cleanup")
def cleanup_old_readings(days: int = 15):
    cutoff = (datetime.now() - timedelta(days=days)).timestamp()
    conn.execute("DELETE FROM readings WHERE ts < ?", (cutoff,))
    conn.commit()
    return {"message": f"Deleted readings older than {days} days"}

# @app.on_event("startup")
# async def startup():
#     #client = mqtt.Client()
#     client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
#     client.on_message = on_message
#     #client.connect("localhost", 1883, 60)
#     client.connect("broker.hivemq.com", 1883, 60)
#     client.subscribe("sensors/bis_room_2")
#     client.loop_start()


# @app.get("/readings")
# def get_readings(limit: int = 100):
#     cursor = conn.execute("SELECT * FROM readings ORDER BY ts DESC LIMIT ?", (limit,))
#     rows = cursor.fetchall()
#     return rows

# @app.get("/readings")
# def get_readings(limit: int = 100):
#     cursor = conn.execute("SELECT * FROM readings ORDER BY ts DESC LIMIT ?", (limit,))
#     rows = cursor.fetchall()
#     # Return as list of dictionaries for better JSON handling
#     columns = ['id', 'device_id', 'temp', 'humidity', 'motion', 'ts']
#     return [dict(zip(columns, row)) for row in rows]

# @app.get("/readings")
# def get_readings(limit: int = 100):
#     cursor = conn.execute("SELECT * FROM readings ORDER BY ts DESC LIMIT ?", (limit,))
#     rows = cursor.fetchall()
    
    
#     # Convert to list of dictionaries with formatted timestamp
#     readings = []
#     for row in rows:
#         readings.append({
#             "id": row[0],
#             "device_id": row[1],
#             "temp": row[2],
#             "humidity": row[3],
#             "motion": row[4],
#             "ts": row[5],  # Keep original for calculations
#             "ts_readable": datetime.fromtimestamp(row[5]).strftime('%Y-%m-%d %H:%M:%S')
#         })
    
#     return readings