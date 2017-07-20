package com.danosoftware.twitterstream.streaming;

import com.danosoftware.twitterstream.data.Tweet;
import com.danosoftware.twitterstream.utilities.FileUtilities;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Spark processor designed to find the average number of words per tweet in the stream of tweets
 */
public class AverageWordsProcessor implements TwitterStreamProcessor {

    final static Logger logger = LoggerFactory.getLogger(AverageWordsProcessor.class);

    private static final String SPARK_APP_NAME = "AverageWordsProcessor";
    private static final String SPARK_MASTER_URL = "local[*]";
    private static final long SPARK_BATCH_DURATION_IN_SECONDS = 2;

    // write results to 'spark-results' sub-directory of user's home directory
    private static final String OUTPUT_DIRECTORY = System.getProperty("user.home") + "/spark-results/";

    // full-path to wanted output file
    private static final String OUTPUT_FILEPATH = OUTPUT_DIRECTORY + "average-words.txt";

    // receiver used to consume twitter stream
    private final Receiver<String> streamReceiver;

    public AverageWordsProcessor(Receiver<String> streamReceiver) {

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

        // extract tweet text
        JavaDStream<String> tweetText = tweets
                .filter((aTweet) -> aTweet.getLang().equals("en"))
                .filter((aTweet) -> aTweet.getText() != null)
                .map((aTweet) -> aTweet.getText());

        // count number of words per tweet
        JavaDStream<Integer> wordCount = tweetText.map((text) -> Arrays.asList(text.split("\\s+")).size());

        FileUtilities.writeToFile(wordCount, OUTPUT_FILEPATH);

        // average number of words per tweet
        wordCount.foreachRDD((wordCounts) -> {

            AvgCount initial =  new AvgCount(0, 0);

            Function2<AvgCount, Integer, AvgCount> addAndCount = (acc, value) -> {
                acc.total += value;
                acc.count += 1;
                return acc;
            };

            Function2<AvgCount, AvgCount, AvgCount> combine = (acc1, acc2) -> {
                acc1.total += acc2.total;
                acc1.count += acc2.count;
                return acc1;
            };

            AvgCount accumulator = wordCounts.aggregate(
                    initial,
                    addAndCount,
                    combine);

            Double average = accumulator.avg();

            FileUtilities.appendText(OUTPUT_FILEPATH,
                    Arrays.asList("Average: " + average, "------------------------"));

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
        logger.info("Results will be written to '{}'.", OUTPUT_FILEPATH);

        // create results directory
        FileUtilities.createDirectory(OUTPUT_DIRECTORY);

        // delete output files before starting
        FileUtilities.deleteIfExists(OUTPUT_FILEPATH);
    }
}

