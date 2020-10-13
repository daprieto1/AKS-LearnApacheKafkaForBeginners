package com.daprieto1.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final String TWITTER_TOPIC = "twitter_tweets";
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private static final List<String> terms = Lists.newArrayList("kafka");

    public static void main(String[] args) throws IOException {

        new TwitterProducer().run();

    }

    public void run() throws IOException {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // create twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Shutting down Twitter client...");
            client.stop();
            logger.info("Stoping Kafka producer...");
            producer.close();
            logger.info("Done!");
        }));

        // lop to send tweets to Kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>(TWITTER_TOPIC, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Error sending message to Kafka", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application!");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException {

        Properties twitterProperties = getProperties("src/main/resources/twitter.properties");

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(twitterProperties.getProperty("twitter.api.key"),
                twitterProperties.getProperty("twitter.api.key.secret"),
                twitterProperties.getProperty("twitter.access.token"),
                twitterProperties.getProperty("twitter.access.token.secret"));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer() throws IOException {

        Properties kafkaProperties = getProperties("src/main/resources/kafka.properties");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProperties);
        return producer;
    }

    public Properties getProperties(String file) throws IOException {
        Properties properties = new Properties();

        try (InputStream propFile = new FileInputStream(file)) {
            properties.load(propFile);
            return properties;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Problem with %s properties file", file), e);
            throw e;
        }
    }
}
