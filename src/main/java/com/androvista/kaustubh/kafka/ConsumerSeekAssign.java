package com.androvista.kaustubh.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerSeekAssign {

    static Logger logger = LoggerFactory.getLogger(ConsumerSeekAssign.class);

    public static void main(String[] args) {

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";

        // consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // seek and assign are used to replay a specific message

        // assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offset = 15L;
        consumer.assign(Collections.singletonList(topicPartition));

        // seek
        consumer.seek(topicPartition, offset);

        int numberOfMessages = 5;
        boolean flag = true;

        // poll for new data
        while (flag) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key : "+record.key()+" Value : "+record.value());
                logger.info("Partition : "+record.partition()+ " Offset : "+record.offset());

                numberOfMessages--;

                if (numberOfMessages == 0) {
                    flag = false;
                    break;
                }
            }
        }

    }
}
