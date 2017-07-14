package com.danosoftware.twitterstream.data;

import java.io.Serializable;

/**
 * Class that represents a single tweet.
 * <p>
 * This is a limited implementation that only holds a subset of all data.
 */
@SuppressWarnings("serial")
public class Tweet implements Serializable {
    private String id;
    private String text;
    private TweetUser user;

    public String getId() {
        return id;
    }

    public String getText() {
        return text;
    }

    public TweetUser getUser() {
        return user;
    }

    public String getLang() {
        if (user != null) {
            return user.getLang();
        } else {
            return "unknown";
        }
    }
}
