# Guido
Guido is a library that simplifies the integration with Apache Kafka and streamlines the creation of Kafka consumers. It offers easy setup, automatic offset management, and built-in error handling, helping you quickly build reliable and efficient Kafka consumer applications.

## Features
* Easy Kafka Integration: Seamlessly connect to Kafka clusters.
* Simplified Consumer Setup: Quickly create and manage Kafka consumers.
* Automatic Offset Management: Handle offsets automatically.
* Error Handling: Built-in error handling and retry mechanisms.

## Getting Started
To use Kafka Consumer Helper, follow these steps:

Install the library: Add guido to your project. For example, if you're using pip, you can install it with:

```
pip install guido-kafka
```

Configure Kafka Service: Set up your Kafka service connection with the required parameters.

Create and Start the Consumer: Use the Guido class and KafkaService to create a consumer and start processing messages.

## Example Usage
Here’s a simple example of how to use guido in your project:

```python
from guido import Guido, KafkaService

# Initialize Kafka service with connection parameters
service = KafkaService(bootstrap_servers='localhost:29092', group_id='my_favorite_group')

# Create a new Guido application with the Kafka service
app = Guido(service)

# Define a function to process messages from the 'my_topic' topic
@app.subscribe('my_topic')
def process_message(message: dict):
    print(message)
```

Save the file as `test_app.py` and run

```
guido test_app.app
```
License
This project is licensed under the MIT License. See the LICENSE file for more details.
