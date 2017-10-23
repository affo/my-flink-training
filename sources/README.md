This folder contains scripts for:

  * Producing messages to Kafka on topic `test`, partition `0`;
  * Consuming messages from Kafka on topic `test`, partition `0`;
  * Produce updates in Redis storage and transactionally publish the updates to Kafka on topic `updates`, partition `0`.

### Install

  ```
$ virtualenv venv
$ source rc
$ pip install -r requirements.txt
  ```

### Producing

  ```
$ python producer.py
# Kill with CTRL-C
  ```

### Consuming

  ```
$ python consumer.py <topic-name>
# Kill with CTRL-C
  ```

### Updates

  ```
$ python updates.py
# You'll be prompted with:
# KEY: -> insert the key you want to update <-
# VALUE: -> insert the value <-
# Kill with CTRL-C
  ```
