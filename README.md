1. Start kafka server
.\bin\windows\kafka-storage.bat format --standalone -t kafkastore -c .\config\server.properties

.\bin\windows\kafka-server-start.bat .\config\server.properties 


2. Produce messages to apache kafka topic 'input-topic'
python interactive_producer.py


3. Consume messages from topic 'input-topic', process using AI and send result to topic output-topic
set OPENROUTER_API_KEY=<<key value>>
python consumer.py


4. Read messages from topic output-topic and view the formatted result
python read_formatted_output.py
