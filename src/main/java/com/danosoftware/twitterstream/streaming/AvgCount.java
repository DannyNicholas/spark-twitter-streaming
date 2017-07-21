package com.danosoftware.twitterstream.streaming;

import java.io.Serializable;

/**
 * Accumulator used to hold total and count of samples seen
 */
public class AvgCount implements Serializable {

    public int total;
    public int count;

    public AvgCount(int total, int count) {
        this.total = total;
        this.count = count;
    }

    public double avg() {
        return total / (double) count;
    }
}
