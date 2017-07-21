package com.danosoftware.twitterstream.config;

import com.danosoftware.twitterstream.factories.ClientFactory;
import com.danosoftware.twitterstream.factories.TwitterClientFactory;
import com.danosoftware.twitterstream.streaming.AverageWordsProcessor;
import com.danosoftware.twitterstream.streaming.LanguageTwitterStreamProcessor;
import com.danosoftware.twitterstream.streaming.TwitterStreamProcessor;
import com.danosoftware.twitterstream.streaming.TwitterStreamReceiver;
import com.danosoftware.twitterstream.utilities.AuthenticationBuilder;
import com.google.common.collect.Lists;
import org.apache.spark.streaming.receiver.Receiver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import java.util.List;

/**
 * Provides the Spring beans needed for processing the Twitter stream.
 */
@Configuration
@PropertySource("classpath:authentication.properties")
public class AppConfig {

    // authentication properties
    @Value("${consumerKey:@null}") private String consumerKey;
    @Value("${consumerSecret:@null}") private String consumerSecret;
    @Value("${token:@null}") private String token;
    @Value("${tokenSecret:@null}") private String tokenSecret;

    @Bean
    public static PropertySourcesPlaceholderConfigurer placeHolderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    /**
     * Creates the Spark streaming processor.
     *
     * @param streamReceiver - Spark receiver of the stream
     * @return
     */
    @Bean(name = "twitterStreamProcessor")
    @Autowired
    public TwitterStreamProcessor twitterStreamProcessor(Receiver<String> streamReceiver) {
        return new AverageWordsProcessor(streamReceiver);
    }

    /**
     * Creates the Spark streaming receiver.
     *
     * @param clientFactory - factory for streaming client
     * @return
     */
    @Bean
    @Scope("singleton")
    @Autowired
    public Receiver<String> streamReceiver(ClientFactory clientFactory) {
        return new TwitterStreamReceiver(clientFactory);
    }

    /**
     * Creates factory responsible for creating the streaming client.
     *
     * @param authenticationBuilder - creates authentication
     * @return
     */
    @Bean
    @Scope("singleton")
    @Autowired
    public ClientFactory clientFactory(AuthenticationBuilder authenticationBuilder) {

        return new TwitterClientFactory(authenticationBuilder,
                "TwitterStreamingClient",
                followings(),
                terms());
    }

    /**
     * Creates a builder responsible for creating authentication.
     *
     * @return
     */
    @Bean
    public AuthenticationBuilder authenticationBuilder() {
        return new AuthenticationBuilder()
                .setConsumerKey(consumerKey)
                .setConsumerSecret(consumerSecret)
                .setToken(token)
                .setTokenSecret(tokenSecret);
    }

    /**
     * List of twitter users to follow.
     *
     * @return
     */
    private List<Long> followings() {
        return Lists.newArrayList(1234L, 566788L);
    }

    /**
     * List of terms to look for in tweets.
     *
     * @return
     */
    private List<String> terms() {
        return Lists.newArrayList("twitter", "api");
    }
}
