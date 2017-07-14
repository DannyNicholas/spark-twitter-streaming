package com.danosoftware.twitterstream.streaming;

import com.danosoftware.twitterstream.data.Tweet;
import com.danosoftware.twitterstream.utilities.FileUtilities;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Spark processor designed to consume the stream of tweets and write contents to a file.
 */
public class SimpleTwitterStreamProcessor implements TwitterStreamProcessor {

    final static Logger logger = LoggerFactory.getLogger(SimpleTwitterStreamProcessor.class);

    private static final String SPARK_APP_NAME = "CustomerSalesProcessor";
    private static final String SPARK_MASTER_URL = "local[*]";
    private static final long SPARK_BATCH_DURATION_IN_SECONDS = 2;

    // write results to 'spark-results' sub-directory of user's home directory
    private static final String OUTPUT_DIRECTORY = System.getProperty("user.home") + "/spark-results/";

    // full-path to wanted output file
    private static final String OUTPUT_FILEPATH = OUTPUT_DIRECTORY + "tweets.txt";

    // receiver used to consume twitter stream
    private final Receiver<String> streamReceiver;

    public SimpleTwitterStreamProcessor(Receiver<String> streamReceiver) {

        this.streamReceiver = streamReceiver;

        // initialise file for writing
        initialiseOutput();
    }

    /**
     * Process the twtter stream
     */
    public void go() {

        // initialise Spark context and streaming context
        final SparkConf sparkConf = new SparkConf().setAppName(SPARK_APP_NAME).setMaster(SPARK_MASTER_URL);
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(SPARK_BATCH_DURATION_IN_SECONDS));

        // consume stream from receiver
        JavaDStream<String> stream = jssc.receiverStream(streamReceiver);

        // extract JSON string tweets into Tweet object instances
        JavaDStream<Tweet> tweets = stream.mapPartitions(new ParseJson());

        // write tweets to file
        tweets.foreachRDD((rdd) -> {
            List<String> outputText = new ArrayList<>();
            for (Tweet aTweet : rdd.collect()) {
                outputText.add(aTweet.getId() + " - " + aTweet.getText());
            }
            FileUtilities.appendText(OUTPUT_FILEPATH, outputText);
            logger.info("Number of tweets received is: {}.", outputText.size());
            return null;
        });

        jssc.start();

        // block until computation completed or stopped
        jssc.awaitTermination();

        jssc.stop();
        jssc.close();
    }

    /**
     * Initialise output file for writing.
     */
    private void initialiseOutput() {
        logger.info("Customer Sales results will be written to '{}'.", OUTPUT_FILEPATH);

        // create results directory
        FileUtilities.createDirectory(OUTPUT_DIRECTORY);

        // delete output files before starting
        FileUtilities.deleteIfExists(OUTPUT_FILEPATH);
    }
}

