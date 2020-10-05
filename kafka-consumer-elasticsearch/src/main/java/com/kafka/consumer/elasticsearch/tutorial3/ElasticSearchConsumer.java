package com.kafka.consumer.elasticsearch.tutorial3;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class ElasticSearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {
        ElasticSearchClientProvider clientProvider = new ElasticSearchClientProvider();
        RestHighLevelClient client = clientProvider.provideClient();
        KafkaConsumer<String, String> consumer = new TwitterConsumer().getKafkaConsumer();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            int recordCount = records.count();
            logger.info("Received "+ recordCount +" Records!");
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                //2 Strategies for id
                //1 kafka generic id
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                //2 twitter feed id
                try {
                    String id = extractIdFromTweet(record.value());
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id // this is to make consumer idempotent
                    ).source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);

                }catch (NullPointerException e){
                    logger.warn("skipping bad data: " + record.value());
                }
            }
            if(recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitAsync();
                logger.info("committed!!");
                pause(1000);
            }
        }
        //client.close();
    }

    private static void pause(int miliSeconds) {
        try {
            Thread.sleep(miliSeconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static String extractIdFromTweet(String tweetJson) {
        return new JsonParser()
                .parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
