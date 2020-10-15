package kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final String TWITTER_TOPIC = "twitter_tweets";
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer(TWITTER_TOPIC);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets")
                        .source(record.value(), XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();
                logger.info(id);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) throws IOException {

        // create consumer properties
        Properties kafkaProperties = getProperties("kafka-consumer-elasticsearch/src/main/resources/kafka.properties");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProperties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    public static RestHighLevelClient createClient() throws IOException {

        Properties elasticSearchProperties = getProperties("kafka-consumer-elasticsearch/src/main/resources/elasticsearch.properties");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticSearchProperties.getProperty("username"), elasticSearchProperties.getProperty("password")));

        HttpHost host = new HttpHost(elasticSearchProperties.getProperty("hostname"), 443, "https");
        RestClientBuilder builder = RestClient.builder(host)
                .setHttpClientConfigCallback(new RestClientBuilder
                        .HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient
                            (HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider
                                (credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static Properties getProperties(String file) throws IOException {
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
