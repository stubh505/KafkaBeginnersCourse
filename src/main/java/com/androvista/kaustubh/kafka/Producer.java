package com.androvista.kaustubh.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    static Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {

        // bootstrap server
        String bootstrapServer = "127.0.0.1:9092";

        // producer config
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "java side again");

        // send data
        producer.send(record, (recordMetadata, e) -> {
            if (e == null) {
                logger.info("Message received : { topic : " + recordMetadata.topic() + ", " +
                        "partition : " + recordMetadata.partition() + ", offset : " + recordMetadata.offset() + " }");
            } else {
                logger.error("Error occurred", e);
            }
        });

        producer.flush();
        producer.close();
    }
}
