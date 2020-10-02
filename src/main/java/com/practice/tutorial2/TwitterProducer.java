package com.practice.tutorial2;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final Integer QUEUE_CAPACITY = 1000;
    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String TOPIC_NAME = "twitter_topic";

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    BlockingQueue<String> msgQueue;
    private Client client;

    public TwitterProducer() {
        this.msgQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.client = new TwitterClient(msgQueue).create();
    }

    public static void main(String[] args) {
        new TwitterProducer().produce();
    }

    public void produce() {
        this.client.connect();
        KafkaProducer<String, String> producer = this.createProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown(producer)));
        this.publish(producer);
    }

    private void publish(KafkaProducer<String, String> producer) {
        //loop to produce tweets to kafka
        // on a different thread, or multiple different threads....
        try {
            this.publishMessage(producer);
        } catch (InterruptedException e) {
            logger.error("Interrupted the program", e);
        }finally {
            client.stop();
        }
    }

    private void publishMessage(KafkaProducer<String, String> producer) throws InterruptedException {
        while (!client.isDone()) {
            String msg = msgQueue.poll(5, TimeUnit.SECONDS);
            logger.info("====================================================================================================");
            logger.info(msg);
            producer.send(new ProducerRecord<>(TOPIC_NAME, null, msg), this::onFailure);
            logger.info("====================================================================================================");
        }
    }

    private KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(getProducerProperties());
    }

    private Properties getProducerProperties(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //make producer safe
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // we don't need to set these explicitly, but just to remember what their values will be after the producer is idempotent
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 use 5 otherwise use 1

        // High throughput producer(at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); //created by google
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

        return properties;
    }

    private void onFailure(RecordMetadata recordMetadata, Exception e) {
        if(e != null){
            logger.error("Something bad happened!", e);
        }
    }

    private void shutdown(KafkaProducer<String, String> producer) {
        logger.info("stopping application...");
        logger.info("shutting down client from twitter....");
        client.stop();
        logger.info("closing producer...");
        producer.close();
        logger.info("done!!!");
    }
}
