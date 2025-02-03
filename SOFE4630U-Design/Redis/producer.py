from google.cloud import pubsub_v1
import os
import glob
import base64

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\fcq11\OneDrive\Escritorio\PM2\SOFE4630U-MS2\SOFE4630U-Design\Redis\secure-sorter-449116-i5-48b23201a985.json"
project_id = "secure-sorter-449116-i5"
images_topic_name = "designRedis"

publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient( publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, images_topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

images_folder = r"C:\Users\fcq11\OneDrive\Escritorio\PM2\SOFE4630U-MS2\SOFE4630U-Design\Dataset_Occluded_Pedestrian"
for image_path in glob.glob(images_folder + "/*.png"):
    with open(image_path, "rb") as img_file:
        img_data = img_file.read()
    encoded_data = base64.b64encode(img_data).decode("utf-8")
    message_key = os.path.basename(image_path)
    future = publisher.publish(
        topic_path,
        encoded_data.encode("utf-8"),
        filename=message_key
    )
    future.result()

print("All images published.")