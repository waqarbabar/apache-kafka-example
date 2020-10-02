package com.practice.tutorial1.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoWithKeys {
    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String TOPIC_NAME = "waqar_practic_topic";

    public static void main(String[] args) {
        int msgCount = 200000;
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getProducerProperties());

        for(int i=1; i<= msgCount; i++){
            String key = i <= msgCount/2 ? "id_below_" : (i%2 == 0) ? "id_even_" :"id_odd_" ;

            producer.send(getProducerRecord(key, "hello world " + i +" "+ key+"_....."), ProducerDemoWithKeys::onMessageSuccessfullySent);
        }
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

    private static ProducerRecord getProducerRecord(String key, String value) {
        return new ProducerRecord<String, String>(TOPIC_NAME, key, value);
    }


    private static void onMessageSuccessfullySent(RecordMetadata recordMetadata, Exception e) {
        if(e == null){
            logger.info("Received new metadata. \n" +
                    "Topic: " +recordMetadata.topic()+ "\n" +
                    "Partition: "+recordMetadata.partition()+"\n"+
                    "Offset: "+recordMetadata.offset()+"\n"+
                    "Timestamp: "+recordMetadata.timestamp());
        }else {
            logger.error("Error while producing: ", e);
        }
    }
}
