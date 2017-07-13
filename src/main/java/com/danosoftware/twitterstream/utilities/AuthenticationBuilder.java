package com.danosoftware.twitterstream.utilities;

import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.Serializable;

/**
 * Builder to create Authentication instance.
 *
 * This builder is Serializable allowing it to be passed to a Spark receiver.
 *
 * This builder is needed as actual Authentication instances are not Serializable
 * preventing them being passed to a receiver.
 */
public class AuthenticationBuilder implements Serializable {

    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String tokenSecret;

    public AuthenticationBuilder setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
        return this;
    }

    public AuthenticationBuilder setConsumerSecret(String consumerSecret) {
        this.consumerSecret = consumerSecret;
        return this;
    }

    public AuthenticationBuilder setToken(String token) {
        this.token = token;
        return this;
    }

    public AuthenticationBuilder setTokenSecret(String tokenSecret) {
        this.tokenSecret = tokenSecret;
        return this;
    }

    public Authentication build() {
        return new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
    }
}
