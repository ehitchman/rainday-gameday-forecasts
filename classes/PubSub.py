from google.cloud import pubsub_v1
import base64
import functions_framework
from classes.ConfigManagerClass import ConfigManager

# This script is designed for managing Google Cloud Pub/Sub topics.
# It schedules messages to be published based on the result of another cloud function.

#Learning:  
# - We're just basically doing some schedulign here based on the result of another
#    cloud function.  Are there better GCP products to accomplish this? Why do it
#    this way?

class PubSubManager:
    def __init__(self, project_id, topic_id):
        self.publisher = pubsub_v1.PublisherClient()
        self.project_id = project_id
        self.topic_id = topic_id

    @functions_framework.http        
    def publish_topic_data(self, data_bytestr="Task completed".encode("utf-8")):
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        try:
            # Publish the message to the specified topic
            future = self.publisher.publish(self.topic_path, data_bytestr)
            print(f"Published message to {self.topic_id}: {future.result()}")
            return "Message published to Pub/Sub", 200
        except Exception as e:
            print(f"An error occurred: {e}")
            return "Error in publishing message", 500
    
if __name__ == '__main__':
    topics = {
        "test_topic_1": "complete_a",
        "test_topic_2": "complete_b"
    }
    # Initialize configuration and publish data to the topic
    config = ConfigManager(yaml_filename='config.yaml', yaml_filepath='config')
    publisher = PubSubManager(config.pubsub_project_id, topic_id=list(topics.keys())[0])
    data_bytestr = list(topics.values())[0].encode("utf-8")
    publisher.publish_topic_data(data_bytestr=data_bytestr)
