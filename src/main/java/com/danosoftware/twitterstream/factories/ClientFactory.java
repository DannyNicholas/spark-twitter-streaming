package com.danosoftware.twitterstream.factories;

import com.twitter.hbc.core.Client;

import java.util.concurrent.BlockingQueue;

/**
 * Abstract factory interface responsible for creating the streaming client.
 */
public interface ClientFactory {

    /**
     * Creates a client for consuming a stream.
     *
     * @param msgQueue - client will push consumed messages to this queue
     */
    Client createClient(final BlockingQueue<String> msgQueue);
}
