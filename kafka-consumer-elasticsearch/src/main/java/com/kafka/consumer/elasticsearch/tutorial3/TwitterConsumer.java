package com.kafka.consumer.elasticsearch.tutorial3;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumer {

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String TOPIC_NAME = "twitter_topic";
    private static final String GROUP_ID = "kafka-elasticsearch-group";

    public KafkaConsumer<String, String> getKafkaConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(getConsumerProperties(GROUP_ID, "earliest"));
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        return consumer;
    }

    private Properties getConsumerProperties(String groupId, String resetParam){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetParam);
        //disable auto commit of offsets and manage them manually
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return properties;
    }
}
