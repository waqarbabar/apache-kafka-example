package com.practice.producer.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.Stream;

public class ProducerDemo {

    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String TOPIC_NAME = "waqar_practic_topic";

    public static void main(String[] args) {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getProducerProperties());

        for(int i=0;i< 100000; i++)
            producer.send(getProducerRecord("hello world" + i));
        producer.flush();
        producer.close();
    }

    private static Properties getProducerProperties(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static ProducerRecord getProducerRecord(String value) {
        return new ProducerRecord<String, String>(TOPIC_NAME, value);
    }

}
