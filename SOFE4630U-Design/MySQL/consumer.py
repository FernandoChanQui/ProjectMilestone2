from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob  
import mysql.connector                       
import json
import os 

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="secure-sorter-449116-i5";
subscription_id = "designMySQL-sub";   # change it for your topic name if needed

# create a subscriber to the subscriber for the project using the subscription_id
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

db = mysql.connector.connect(
    host="34.95.44.103",
    user="usr",
    password="sofe4630u",
    database="Readings"
)

def callback(message):
    row_data = json.loads(message.data.decode("utf-8"))
    cursor = db.cursor()
    insert_query = ("INSERT INTO your_table "
                    "(Timestamp, Car1_Location_X, Car1_Location_Y, Car1_Location_Z, "
                    " Car2_Location_X, Car2_Location_Y, Car2_Location_Z, Occluded_Image_view, "
                    " Occluding_Car_view, Ground_Truth_View, pedestrianLocationX_TopLeft, "
                    " pedestrianLocationY_TopLeft, pedestrianLocationX_BottomRight, "
                    " pedestrianLocationY_BottomRight) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    data_tuple = (
        row_data["Timestamp"],
        row_data["Car1_Location_X"],
        row_data["Car1_Location_Y"],
        row_data["Car1_Location_Z"],
        row_data["Car2_Location_X"],
        row_data["Car2_Location_Y"],
        row_data["Car2_Location_Z"],
        row_data["Occluded_Image_view"],
        row_data["Occluding_Car_view"],
        row_data["Ground_Truth_View"],
        row_data["pedestrianLocationX_TopLeft"],
        row_data["pedestrianLocationY_TopLeft"],
        row_data["pedestrianLocationX_BottomRight"],
        row_data["pedestrianLocationY_BottomRight"]
    )
    cursor.execute(insert_query, data_tuple)
    db.commit()
    cursor.close()
    message.ack()

future = subscriber.subscribe(subscription_path, callback=callback)
print("Listening for CSV record messages...")
future.result()