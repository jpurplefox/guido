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
from guido import Guido, KafkaConfig

# Create a new Guido application
app = Guido(
    config=KafkaConfig(bootstrap_servers="localhost:29092", group_id="foo")
)

# Define a function to process messages from the 'my_topic' topic
@app.subscribe('my_topic')
def process_message(message: dict):
    print(message)
```

Save the file as `test_app.py` and run

```
guido test_app.app
```

### Producing messages
You can produce messages easily through a command
```
guido test_app.app produce my_topic '{"foo": "bar"}'
```
Message to be produced must be a valid JSON

### Checking pending messages
Also you can check how many messages there are in a topic partition pending to process
```
guido test_app.app pending-messages my_topic
```

### Configuring Guido with Environment Variables
Guido can be configured using environment variables, which can be useful for managing configuration settings in different environments (e.g., development, staging, production) without hardcoding values in your source code.

Just create your app without using any configuration
``` python
app = Guido()
```
Then set `GUIDO_HOSTS` and `GUIDO_GROUP_ID`. Take into account that any configuration passed to Guido constructor takes priority over environment variables


## Error handling
Guido has built-in error handling mechanisms to ensure that messages are processed reliably. If an exception is raised while processing a message, Guido captures the error and takes the necessary steps to manage it.

1. Sends the message to a Dead Letter Topic (DLT): The problematic message is sent to a designated Dead Letter Topic (DLT). This helps in isolating messages that failed processing and allows for further investigation and troubleshooting. Guido creates by default a DLT named `<original_topic>_<group_id>_dlt`.
2. Committing the Original Message: After sending the message to the DLT, Guido commits the original message's offset. This means that the consumer will not attempt to reprocess the same message, avoiding potential processing loops and ensuring that the consumer continues processing new messages.

If you want, you can tell guido what DLT to use for a message
```
@app.subscribe('my_topic', dead_letter_topic='custom_topic')
def process_message(message: dict):
    print(message)
```

## License
This project is licensed under the MIT License. See the LICENSE file for more details.
