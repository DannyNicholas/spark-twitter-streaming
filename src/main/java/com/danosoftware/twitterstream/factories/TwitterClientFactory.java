package com.danosoftware.twitterstream.factories;

import com.danosoftware.twitterstream.utilities.AuthenticationBuilder;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Factory responsible for creating the twitter streaming client.
 */
public class TwitterClientFactory implements ClientFactory, Serializable {

    // builder to create client authentication
    private final AuthenticationBuilder authenticationBuilder;

    // name of twitter streaming client
    private final String clientName;

    // list of twitter users to follow
    private final List<Long> followings;

    // list of terms to look for in tweets
    private final List<String> terms;

    public TwitterClientFactory(AuthenticationBuilder authenticationBuilder,
                                String clientName,
                                List<Long> followings,
                                List<String> terms) {

        this.authenticationBuilder = authenticationBuilder;
        this.clientName = clientName;
        this.followings = followings;
        this.terms = terms;
    }

    /**
     * Creates a client for consuming a twitter stream.
     *
     * @param msgQueue - client will push consumed messages to this queue
     */
    @Override
    public Client createClient(final BlockingQueue<String> msgQueue) {

        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.followings(followings);
        endpoint.trackTerms(terms);

        final ClientBuilder builder = new ClientBuilder()
                .name(clientName)                                       // optional: mainly for the logs
                .hosts(new HttpHosts(Constants.STREAM_HOST))
                .authentication(authenticationBuilder.build())
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                         // optional: use this if you want to process client events

        return builder.build();
    }
}
