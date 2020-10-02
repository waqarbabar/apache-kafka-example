package com.practice.consumer.demo.with.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoGroupWithThreads {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoGroupWithThreads.class);
    public static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    public static final String TOPIC_NAME = "waqar_practic_topic";
    public static final String GROUP_ID = "waqar_app_4";
    public static final String RESET_PARAM = "earliest";

    public static void main(String[] args) {
        new ConsumerDemoGroupWithThreads().consume();
    }

    private void consume() {
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("creating the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVER, TOPIC_NAME, GROUP_ID,RESET_PARAM, latch);

        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutDown(consumerRunnable, latch)));

        callLatchAwaits(latch, "Application got interrupted", "Application is closing");
    }

    private void callLatchAwaits(CountDownLatch latch, String errorMsg, String infoMsg) {
        try{
            latch.await();
        } catch (InterruptedException e) {
            logger.error(errorMsg, e);
        }finally {
            logger.info(infoMsg);
        }
    }

    private void shutDown(Runnable consumerRunnable, CountDownLatch latch) {
        logger.info("Caught shutdown hook");
        ((ConsumerRunnable)consumerRunnable).shutdown();
        callLatchAwaits(latch, "", "Application has exited");
    }
}
