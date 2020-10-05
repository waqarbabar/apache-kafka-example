package com.practice.streams.tutorials4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String INPUT_TOPIC = "twitter_topic";
    private static final String IMPORTANT_TWEETS_TOPIC = "important_tweets_topic";


    public static void main(String[] args) {
        StreamFilterTweets streamFilterTweets = new StreamFilterTweets();

        //1 create properties
        Properties properties = streamFilterTweets.getProducerProperties();

        //2 create topology
        StreamsBuilder builder = streamFilterTweets.createTopology();

        //3 build topology
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

        //start our stream app
        kafkaStreams.start();
    }

    private StreamsBuilder createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputTopic = builder.stream(INPUT_TOPIC);
        KStream<String, String> filteredStream = inputTopic.filter(this::filterTweetsWithGreaterThanOneThausand);
        filteredStream.to(IMPORTANT_TWEETS_TOPIC);
        return builder;
    }

    private boolean filterTweetsWithGreaterThanOneThausand(String key, String jsonTweet) {
        return this.extractFollowersInTweet(jsonTweet) > 1000;
    }

    private Integer extractFollowersInTweet(String tweetJson) {
        try {
         return new JsonParser()
                    .parse(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        }catch (NullPointerException e){
            return 0;
        }
    }

    private Properties getProducerProperties(){
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return properties;
    }
}
