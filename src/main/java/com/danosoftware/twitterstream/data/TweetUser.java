package com.danosoftware.twitterstream.data;

import java.io.Serializable;

/**
 * Class that represents a user profile from a tweet.
 * <p>
 * This is a limited implementation that only holds a subset of all data.
 */
@SuppressWarnings("serial")
public class TweetUser implements Serializable {
    private String lang;

    public String getLang() {
        return lang;
    }
}
