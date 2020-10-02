package practice.tutorial2;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TwitterClient {

    private Logger logger = LoggerFactory.getLogger(TwitterClient.class);

    private static final String CONSUMER_KEY = "FgGGd9mSb3FJew4zh6WjmniP4";
    private static final String CONSUMER_SECRET = "7Kx1qOw3TmF0VL3PpCzad0GXKCtw6ynK7OtT2Gz8PAL8qTjDDq";
    private static final String TOKEN = "1245167467-ySbuQI3W1Pk5UQRidcGpxG5Jnc6TVnimn9p7rYM";
    private static final String SECRET = "BfBCam3xvp4aSShQ2qv1m6YGLwKMP8vAAovIcSdYtSnWK";

    private BlockingQueue<String> msgQueue;

    public TwitterClient(BlockingQueue<String> msgQueue) {
        this.msgQueue = msgQueue;
    }

    public Client create() {
        logger.info("Setting up client");
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("pakistan", "usa", "politics", "sport", "soccer");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        logger.info("successfully setup the client");
        return hosebirdClient;
    }
}
