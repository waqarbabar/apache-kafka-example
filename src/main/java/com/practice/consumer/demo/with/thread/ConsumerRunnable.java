package com.practice.consumer.demo.with.thread;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable{

    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    private KafkaConsumer<String, String> consumer;
    private CountDownLatch countDownLatch;

    public ConsumerRunnable(String bootstrapServer, String topicName, String groupId, String resetParam, CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
        Properties properties = getConsumerProperties(bootstrapServer, groupId, resetParam);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                logMessages(records);
            }
        }catch (WakeupException e){
            logger.info("Received shutdown signal!");
        }finally {
            consumer.close();
            countDownLatch.countDown();
        }
    }

    private Properties getConsumerProperties(String bootstrapServer, String groupId, String resetParam){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, resetParam);
        return properties;
    }

    private void logMessages(ConsumerRecords<String, String> records) {
        for(ConsumerRecord<String, String> record : records){
            logger.info("Topic: " +record.topic());
            logger.info("Partition: " +record.partition());
            logger.info("Key: " +record.key());
            logger.info("Value: " +record.value());
            logger.info("Offset: " +record.offset());
            logger.info("=======================================================================");
        }
    }

    public void shutdown(){
        consumer.wakeup();
    }
}
