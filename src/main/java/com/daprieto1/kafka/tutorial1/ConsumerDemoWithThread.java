package com.daprieto1.kafka.tutorial1;

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

public class ConsumerDemoWithThread {

    private static final String TOPIC = "first_topic";
    private static final String GROUP_ID = "my_app_group";
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final CountDownLatch latch = new CountDownLatch(1); // latch for dealing with multiple threads
    private final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
    }

    private void run() {

        // create consumer runnable
        logger.info("Creating the consuming thread!");
        Runnable myConsumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVERS, GROUP_ID, TOPIC, latch);

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caugth shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
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
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {

            this.latch = latch;

            // create consumer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            this.consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to our topic(s)
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                // poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info(String.format("Key: %s, Value: %s, Partition: %s, Offset: %s", record.key(), record.value(), record.partition(), record.offset()));
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown(); // tell main code we're done with the consumer
            }
        }

        /**
         * Interrupt consumer.poll()
         * It will throw the WakeupException
         */
        public void shutdown() {
            consumer.wakeup();
        }
    }

}
