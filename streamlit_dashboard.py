import streamlit as st
import pandas as pd
import requests
import altair as alt
import time
import paho.mqtt.client as mqtt
import json
import threading
import uuid
from datetime import datetime

# --- Configuration ---
BROKER = "broker.hivemq.com"
PORT = 1883
TOPIC = "sensors/bis_room_2"
FASTAPI_URL = "http://localhost:8002/readings?limit=50"

# --- Shared State (Thread-Safe) ---
shared_data = {
    "connected": False,
    "last_message": None,
    "error": None,
    "message_count": 0,
    "last_message_time": None
}

# --- MQTT Client Setup ---
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        shared_data["connected"] = True
        shared_data["error"] = None
        client.subscribe(TOPIC)
        print(f"Connected to {BROKER}")
    else:
        shared_data["connected"] = False
        shared_data["error"] = f"Connection failed with code {reason_code}"
        print(f"Connection failed: {reason_code}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        data = json.loads(payload)
        shared_data["last_message"] = data
        shared_data["error"] = None
        shared_data["message_count"] += 1
        shared_data["last_message_time"] = datetime.now()
        print(f"MQTT message received: {data}")
        print(f"Message keys: {data.keys()}")  # Debug: print available keys
    except Exception as e:
        shared_data["error"] = f"Error parsing message: {e}"
        print(f"Error parsing message: {e}")

client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=str(uuid.uuid4()))
client.on_connect = on_connect
client.on_message = on_message

# Start MQTT client in a separate thread
def start_mqtt():
    try:
        client.connect(BROKER, PORT, 60)
        client.loop_start()
        print("MQTT thread started")
    except Exception as e:
        shared_data["connected"] = False
        shared_data["error"] = f"MQTT connection error: {e}"
        print(f"MQTT error: {e}")

mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
mqtt_thread.start()
time.sleep(2)  # Give time for connection

# --- Streamlit UI ---
st.set_page_config(layout="wide", page_title="Michael's Live Room Monitor")

# --- Auto-refresh Control in Sidebar ---
st.sidebar.header("Refresh Controls")
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", min_value=2, max_value=10, value=3)
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)

st.sidebar.markdown("---")
st.sidebar.info(
    "**Data Sources:**\n"
    "- 📊 **FastAPI**: Historical data from database\n"
    "- 📡 **MQTT**: Live real-time sensor data"
)

# Main title
st.title("📊 Michael's Live Room Monitor")
st.markdown("---")

# --- Function to fetch FastAPI data (NO CACHING) ---
def get_fastapi_data():
    try:
        response = requests.get(FASTAPI_URL, timeout=5)
        response.raise_for_status()
        data = response.json()
        print(f"FastAPI data received: {len(data) if data else 0} records")  # Debug
        return data
    except Exception as e:
        print(f"FastAPI error: {e}")  # Debug
        return {"error": str(e)}

# --- Function to get MQTT data ---
def get_mqtt_data():
    return {
        "connected": shared_data["connected"],
        "message": shared_data["last_message"],
        "error": shared_data["error"],
        "message_count": shared_data["message_count"],
        "last_message_time": shared_data["last_message_time"]
    }

# --- Main Content Container ---
main_container = st.container()

with main_container:
    # --- Status Bar ---
    col1, col2, col3, col4 = st.columns([1, 2, 1, 1])
    with col1:
        if shared_data["connected"]:
            st.success("✅ MQTT Connected")
        else:
            st.error("❌ MQTT Disconnected")
    
    with col2:
        if shared_data["error"]:
            st.error(f"⚠️ {shared_data['error']}")
        else:
            st.info(f"📡 MQTT Topic: {TOPIC}")
    
    with col3:
        st.metric("📨 MQTT Msgs", shared_data["message_count"])
    
    with col4:
        if auto_refresh:
            st.info(f"🔄 Refresh: {refresh_rate}s")
        else:
            st.info("⏸️ Auto-refresh Off")
    
    st.markdown("---")
    
    # --- Section 1: FastAPI Historical Data ---
    st.header("📊 Historical Sensor Data (FastAPI)")
    
    # Fetch fresh data each time (no caching)
    fastapi_data = get_fastapi_data()
    
    if isinstance(fastapi_data, dict) and "error" in fastapi_data:
        st.warning(f"⚠️ FastAPI server unavailable: {fastapi_data['error']}")
        st.info("Make sure FastAPI is running: `uvicorn fastAPI_Subscriber:app --reload`")
    else:
        try:
            if fastapi_data and len(fastapi_data) > 0:
                df = pd.DataFrame(fastapi_data)
                
                # Debug: Show raw data structure
                with st.expander("🔍 Debug: Raw FastAPI Data Structure"):
                    st.write(f"Columns: {df.columns.tolist()}")
                    st.write(f"First row: {df.iloc[0].to_dict()}")
                
                # Convert timestamp if it exists
                if 'ts' in df.columns and not df.empty:
                    df['ts_readable'] = pd.to_datetime(df['ts'], unit='s')
                elif 'timestamp' in df.columns:
                    df['ts_readable'] = pd.to_datetime(df['timestamp'], unit='s')
                
                if not df.empty:
                    # Display metrics from latest reading
                    latest = df.iloc[0]
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        if 'temp' in latest:
                            st.metric("🌡️ Temperature", f"{latest['temp']:.1f}°C")
                        elif 'temperature' in latest:
                            st.metric("🌡️ Temperature", f"{latest['temperature']:.1f}°C")
                        else:
                            st.metric("🌡️ Temperature", "No Data")
                    
                    with col2:
                        if 'humidity' in latest:
                            st.metric("💧 Humidity", f"{latest['humidity']:.1f}%")
                        else:
                            st.metric("💧 Humidity", "No Data")
                    
                    with col3:
                        if 'motion' in latest:
                            motion_status = "🔴 Motion Detected" if latest['motion'] == 1 else "🟢 No Motion"
                            st.metric("🚶 Motion", motion_status)
                        else:
                            st.metric("🚶 Motion", "No Data")
                    
                    with col4:
                        if 'ts_readable' in df.columns:
                            st.metric("⏰ Last Update", df['ts_readable'].iloc[0].strftime('%H:%M:%S'))
                    
                    # Create chart for temperature and humidity
                    if 'ts_readable' in df.columns and 'temp' in df.columns and 'humidity' in df.columns:
                        st.subheader("📈 Temperature & Humidity Trends")
                        
                        # Prepare data for Altair
                        chart_df = df[['ts_readable', 'temp', 'humidity']].copy()
                        chart_df = chart_df.sort_values('ts_readable')
                        
                        # Melt for Altair
                        melted_df = chart_df.melt(
                            id_vars=['ts_readable'],
                            value_vars=['temp', 'humidity'],
                            var_name='Parameter',
                            value_name='Value'
                        )
                        
                        # Create chart
                        chart = alt.Chart(melted_df).mark_line().encode(
                            x=alt.X('ts_readable:T', title='Time',
                                   axis=alt.Axis(labelAngle=45, format='%H:%M:%S')),
                            y=alt.Y('Value:Q', title='Value'),
                            color=alt.Color('Parameter:N',
                                          scale=alt.Scale(domain=['temp', 'humidity'],
                                                        range=['red', 'blue']),
                                          title='Parameter')
                        ).properties(
                            title='Temperature & Humidity Over Time',
                            height=400
                        ).interactive()
                        
                        st.altair_chart(chart, use_container_width=True)
                    
                    # Show recent data table
                    with st.expander("📋 View Recent FastAPI Data", expanded=False):
                        display_cols = ['id', 'device_id', 'temp', 'humidity', 'motion', 'ts_readable']
                        available_cols = [col for col in display_cols if col in df.columns]
                        display_df = df.head(20)[available_cols].copy()
                        display_df = display_df.rename(columns={
                            'device_id': 'Device',
                            'temp': 'Temp (°C)',
                            'humidity': 'Humidity (%)',
                            'motion': 'Motion',
                            'ts_readable': 'Time'
                        })
                        if 'Motion' in display_df.columns:
                            display_df['Motion'] = display_df['Motion'].apply(lambda x: "Yes" if x == 1 else "No")
                        st.dataframe(display_df, use_container_width=True)
                else:
                    st.info("No historical data available in FastAPI database")
            else:
                st.info("No data received from FastAPI. Waiting for sensor data...")
                
        except Exception as e:
            st.error(f"Error processing FastAPI data: {e}")
            import traceback
            with st.expander("Debug: Error Details"):
                st.code(traceback.format_exc())
    
    st.markdown("---")
    
    # --- Section 2: Live MQTT Data ---
    st.header("📡 Live Sensor Data (Direct MQTT)")
    
    mqtt_status = get_mqtt_data()
    
    # Display live metrics if data is available
    if mqtt_status["connected"] and mqtt_status["message"]:
        live_msg = mqtt_status["message"]
        
        # Debug: Show MQTT message structure
        with st.expander("🔍 Debug: MQTT Message Structure"):
            st.write(f"Message keys: {live_msg.keys()}")
            st.json(live_msg)
        
        # Display live metrics in columns - handle different key names
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if 'temperature' in live_msg:
                st.metric("🌡️ Live Temp", f"{live_msg['temperature']:.1f}°C", delta="Real-time")
            elif 'temp' in live_msg:
                st.metric("🌡️ Live Temp", f"{live_msg['temp']:.1f}°C", delta="Real-time")
            else:
                # Try to find any temperature-like field
                temp_keys = [k for k in live_msg.keys() if 'temp' in k.lower()]
                if temp_keys:
                    st.metric("🌡️ Live Temp", f"{live_msg[temp_keys[0]]:.1f}°C", delta="Real-time")
                else:
                    st.metric("🌡️ Live Temp", "No Data", delta="Check sensor")
        
        with col2:
            if 'humidity' in live_msg:
                st.metric("💧 Live Humidity", f"{live_msg['humidity']:.1f}%", delta="Real-time")
            else:
                # Try to find any humidity-like field
                humid_keys = [k for k in live_msg.keys() if 'humid' in k.lower()]
                if humid_keys:
                    st.metric("💧 Live Humidity", f"{live_msg[humid_keys[0]]:.1f}%", delta="Real-time")
                else:
                    st.metric("💧 Live Humidity", "No Data", delta="Check sensor")
        
        with col3:
            if 'motion' in live_msg:
                motion_text = "🔴 Detected" if live_msg['motion'] == 1 else "🟢 None"
                st.metric("🚶 Live Motion", motion_text, delta="Live")
            else:
                # Try to find any motion-like field
                motion_keys = [k for k in live_msg.keys() if 'motion' in k.lower()]
                if motion_keys:
                    motion_text = "🔴 Detected" if live_msg[motion_keys[0]] == 1 else "🟢 None"
                    st.metric("🚶 Live Motion", motion_text, delta="Live")
                else:
                    st.metric("🚶 Live Motion", "No Data", delta="Check sensor")
        
        with col4:
            st.metric("📨 Message #", mqtt_status["message_count"])
        
        # Display full live data
        with st.expander("🔍 View Raw MQTT Data", expanded=False):
            st.json(live_msg)
            
            # Display as DataFrame
            df_live = pd.DataFrame([live_msg])
            st.dataframe(df_live, use_container_width=True)
            
            # Show timestamp if available
            if 'ts' in live_msg:
                live_time = datetime.fromtimestamp(live_msg['ts']).strftime('%Y-%m-%d %H:%M:%S')
                st.success(f"📅 Message timestamp: {live_time}")
            elif mqtt_status["last_message_time"]:
                st.success(f"📅 Received at: {mqtt_status['last_message_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show success message
        st.success(f"✅ Live data received! Total messages: {mqtt_status['message_count']}")
        
    elif mqtt_status["connected"]:
        st.info("🔄 Connected to MQTT broker, waiting for sensor data...")
        st.caption("Make sure your sensor publisher is sending data to the topic")
        
        # Show debug info
        with st.expander("Debug: MQTT Status"):
            st.write(f"Connected: {mqtt_status['connected']}")
            st.write(f"Message count: {mqtt_status['message_count']}")
            st.write(f"Last message: {mqtt_status['message']}")
            st.write(f"Error: {mqtt_status['error']}")
            st.write(f"Topic: {TOPIC}")
            st.write(f"Broker: {BROKER}")
    else:
        st.error("❌ MQTT broker disconnected")
        if st.button("🔄 Attempt Reconnection"):
            try:
                client.reconnect()
                time.sleep(1)
                st.rerun()
            except Exception as e:
                st.error(f"Reconnection failed: {e}")
    
    # Footer with timestamp
    st.markdown("---")
    st.caption(f"📅 Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption(f"🔄 Auto-refresh every {refresh_rate} seconds | Press 'R' to refresh manually")

# --- Auto-refresh Logic ---
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
else:
    # Manual refresh button in sidebar
    if st.sidebar.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()






# import streamlit as st
# import pandas as pd
# import requests
# import altair as alt
# import time
# import paho.mqtt.client as mqtt
# import json
# import threading
# import uuid
# from datetime import datetime
# # Add this after your imports
# import threading


# # --- Configuration ---
# BROKER = "broker.hivemq.com"
# PORT = 1883
# TOPIC = "sensors/bis_room_2"
# FASTAPI_URL = "http://localhost:8000/readings?limit=50"

# # --- Shared State (Thread-Safe) ---
# shared_data = {
#     "connected": False,
#     "last_message": None,
#     "error": None
# }

# # --- MQTT Client Setup ---
# def on_connect(client, userdata, flags, reason_code, properties=None):
#     if reason_code == 0:
#         shared_data["connected"] = True
#         shared_data["error"] = None
#         client.subscribe(TOPIC)
#         print(f"Connected to {BROKER}")
#     else:
#         shared_data["connected"] = False
#         shared_data["error"] = f"Connection failed with code {reason_code}"
#         print(f"Connection failed: {reason_code}")

# def on_message(client, userdata, msg):
#     try:
#         payload = msg.payload.decode()
#         data = json.loads(payload)
#         shared_data["last_message"] = data
#         shared_data["error"] = None
#         print(f"MQTT message received: {data}")  # Debug
#     except Exception as e:
#         shared_data["error"] = f"Error parsing message: {e}"
#         print(f"Error parsing message: {e}")

# client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=str(uuid.uuid4()))
# client.on_connect = on_connect
# client.on_message = on_message

# # Start MQTT client in a separate thread
# def start_mqtt():
#     try:
#         client.connect(BROKER, PORT, 60)
#         client.loop_start()
#         print("MQTT thread started")
#     except Exception as e:
#         shared_data["connected"] = False
#         shared_data["error"] = f"MQTT connection error: {e}"
#         print(f"MQTT error: {e}")

# mqtt_thread = threading.Thread(target=start_mqtt, daemon=True)
# mqtt_thread.start()
# time.sleep(2)

# # --- Streamlit UI ---
# st.set_page_config(layout="wide", page_title="Michael's Live Room Monitor")
# st.title("Michael's Live Room Monitor")

# # --- Section 1: Status Bar ---
# col1, col2, col3 = st.columns([1, 4, 1])
# with col1:
#     if shared_data["connected"]:
#         st.success("● Connected to Broker")
#     else:
#         st.error("● Disconnected")

# with col2:
#     if shared_data["error"]:
#         st.error(f"Error: {shared_data['error']}")
#     else:
#         st.write("MQTT Status: Active")

# # --- Section 2: FastAPI Historical Data ---
# st.header("Historical Sensor Data via FastAPI")

# @st.cache_data(ttl=5)
# def get_fastapi_data():
#     try:
#         response = requests.get(FASTAPI_URL, timeout=5)
#         response.raise_for_status()
#         return response.json()
#     except Exception as e:
#         return {"error": str(e)}

# fastapi_data = get_fastapi_data()

# if isinstance(fastapi_data, dict) and "error" in fastapi_data:
#     st.warning("FastAPI server unavailable: " + fastapi_data["error"])
#     df = pd.DataFrame()  # Create empty DataFrame
# else:
#     try:
#         df = pd.DataFrame(fastapi_data)
        
#         # Convert timestamp if it exists
#         if 'ts' in df.columns and not df.empty:
#             df['ts_readable'] = pd.to_datetime(df['ts'], unit='s')
#         elif not df.empty:
#             # If no ts column, create a dummy index
#             df['ts_readable'] = pd.date_range(end=datetime.now(), periods=len(df), freq='S')
        
#         # Rename columns to match your API
#         if not df.empty:
#             rename_dict = {}
#             if 'device_id' in df.columns:
#                 rename_dict['device_id'] = 'device'
#             if 'temp' in df.columns:
#                 rename_dict['temp'] = 'temperature'
            
#             if rename_dict:
#                 df = df.rename(columns=rename_dict)
        
#         if not df.empty:
#             # Fixed: Use .iloc[0] to access first row correctly
#             latest = df.iloc[0]
            
#             col1, col2, col3 = st.columns(3)
#             # Fixed: Check if columns exist before accessing
#             if 'temperature' in latest:
#                 col1.metric("Temperature °C", f"{latest['temperature']:.1f}")
#             else:
#                 col1.metric("Temperature °C", "No Data")
                
#             if 'humidity' in latest:
#                 col2.metric("Humidity %", f"{latest['humidity']:.1f}")
#             else:
#                 col2.metric("Humidity %", "No Data")
                
#             if 'motion' in latest:
#                 col3.metric("Motion", "Detected" if latest['motion'] == 1 else "None")
#                 st.write(f"**Motion Status:** {'Motion Detected!' if latest['motion'] == 1 else 'No Motion'}")
#             else:
#                 col3.metric("Motion", "No Data")
            
#             # Fixed: Melt the DataFrame correctly for Altair
#             chart_df = df.melt(
#                 id_vars=['ts_readable'], 
#                 value_vars=['temperature', 'humidity'], 
#                 var_name='Parameter', 
#                 value_name='Value'
#             )
#             # Fixed: Sort by timestamp to avoid line connecting issues
#             chart_df = chart_df.sort_values(['ts_readable', 'Parameter'])
            
#             # Create chart with proper datetime handling
#             chart = alt.Chart(chart_df).mark_line().encode(
#                 x=alt.X('ts_readable:T', title='Time', 
#                        axis=alt.Axis(labelAngle=45, format='%H:%M:%S')),
#                 y=alt.Y('Value:Q', title='Value'),
#                 color=alt.Color('Parameter:N', 
#                               scale=alt.Scale(domain=['temperature', 'humidity'], 
#                                             range=['red', 'blue']))
#             ).properties(
#                 title='Temperature & Humidity Over Time', 
#                 height=500
#             ).interactive()
            
#             st.altair_chart(chart, use_container_width=True)
#         else:
#             st.info("No data available in FastAPI.")
#     except Exception as e:
#         st.error(f"Error processing FastAPI data: {e}")
#         import traceback
#         st.code(traceback.format_exc())
#         df = pd.DataFrame()

# # --- Section 3: Live MQTT Data ---
# st.header("Live Sensor Data via MQTT")

# @st.cache_data(ttl=2)
# def get_mqtt_data():
#     return {
#         "connected": shared_data["connected"],
#         "message": shared_data["last_message"],
#         "error": shared_data["error"]
#     }

# mqtt_status = get_mqtt_data()

# with st.expander("View Raw MQTT Data", expanded=False):
#     if mqtt_status["connected"] and mqtt_status["message"]:
#         st.success("● MQTT Data Received")
#         st.json(mqtt_status["message"])
        
#         # Display as DataFrame
#         if isinstance(mqtt_status["message"], dict):
#             df_mqtt = pd.DataFrame([mqtt_status["message"]])
#             st.dataframe(df_mqtt, use_container_width=True)
#     elif mqtt_status["connected"]:
#         st.info("● Connected, waiting for data...")
#     else:
#         st.error("● Disconnected from Broker")
#         st.write("Attempting to reconnect...")
#         if st.button("Reconnect MQTT"):
#             try:
#                 client.reconnect()
#                 time.sleep(1)
#                 st.rerun()
#             except Exception as e:
#                 st.error(f"Reconnection failed: {e}")

# # Add auto-refresh note
# st.caption("Auto-refreshing every 2-5 seconds. Data updates automatically.")