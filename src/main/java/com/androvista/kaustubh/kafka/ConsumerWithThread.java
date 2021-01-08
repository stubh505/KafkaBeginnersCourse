package com.androvista.kaustubh.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThread {

    static Logger logger = LoggerFactory.getLogger(ConsumerWithThread.class);

    public static void main(String[] args) {
        new ConsumerWithThread().run();
    }

    public ConsumerWithThread() { }

    public void run() {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "java-consumer-group-demo-thread-1";
        String topic = "first_topic";

        // creating the latch
        CountDownLatch latch = new CountDownLatch(1);

        // creating consumer runnable
        logger.info("Creating consumer thread");
        Runnable consumeRunnable = new ConsumerRunnable(bootstrapServer, groupId, topic, latch);

        // starting thread
        Thread thread = new Thread(consumeRunnable);
        thread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumeRunnable).shutdown();

            // Exits execution
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Process encountered error", e);
        } finally {
            logger.info("Process ended");
        }
    }

    public class ConsumerRunnable implements Runnable {

        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            // consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<>(properties);

            // subscribe
            consumer.subscribe(Collections.singletonList(topic));
        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key : " + record.key() + " Value : " + record.value());
                        logger.info("Partition : " + record.partition() + " Offset : " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                // notify main this is done
                latch.countDown();
            }
        }

        public void shutdown() {
            // this method interrupts consumer.poll()
            consumer.wakeup();
        }
    }
}
