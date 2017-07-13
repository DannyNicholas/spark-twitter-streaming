package com.danosoftware.twitterstream.streaming;

import com.danosoftware.twitterstream.factories.ClientFactory;
import com.twitter.hbc.core.Client;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Spark Receiver to consume Twitter stream
 */
public class TwitterStreamReceiver extends Receiver<String> {

    final static Logger logger = LoggerFactory.getLogger(TwitterStreamReceiver.class);

    // factory to create streaming client
    private final ClientFactory factory;

    // indicates when client should be shut-down
    private volatile boolean clientShutdown;

    public TwitterStreamReceiver(final ClientFactory factory) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.factory = factory;
        this.clientShutdown = false;
    }

    /**
     * Start receiver and connect client.
     *
     * Start thread to consume tweets and pass onto Spark.
     */
    @Override
    public void onStart() {

        // blocking queue to hold received stream
        final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // create client and connect to stream
        final Client client = factory.createClient(msgQueue);
        client.connect();

        // start thread to receive items arriving from the stream.
        // pass messages onto Spark for processing.
        // run continuously until client is stopped.
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(() -> {
            logger.info("Starting streaming receiver.");
            while (!clientShutdown) {
                try {
                    String msg = msgQueue.take();
                    logger.trace("Received message: {}.", msg);
                    store(msg);
                } catch (InterruptedException e) {
                    logger.error("InterruptedException while receiving from stream.");
                }
            }
            client.stop();
            logger.info("Streaming receiver stopped.");
        });
    }

    /**
     * Stop receiver and request client shutdown
     */
    @Override
    public void onStop() {
        logger.info("Stopping streaming client.");
        this.clientShutdown = true;
    }
}
