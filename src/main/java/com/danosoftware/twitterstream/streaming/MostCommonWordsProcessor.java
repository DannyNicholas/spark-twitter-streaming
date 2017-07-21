package com.danosoftware.twitterstream.streaming;

import com.danosoftware.twitterstream.data.Tweet;
import com.danosoftware.twitterstream.utilities.FileUtilities;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Spark processor designed to find the most common words in the stream of tweets
 */
public class MostCommonWordsProcessor implements TwitterStreamProcessor {

    final static Logger logger = LoggerFactory.getLogger(MostCommonWordsProcessor.class);

    private static final String SPARK_APP_NAME = "MostCommonWordsProcessor";
    private static final String SPARK_MASTER_URL = "local[*]";
    private static final long SPARK_BATCH_DURATION_IN_SECONDS = 2;

    // write results to 'spark-results' sub-directory of user's home directory
    private static final String OUTPUT_DIRECTORY = System.getProperty("user.home") + "/spark-results/";

    // full-path to wanted output file
    private static final String OUTPUT_FILEPATH = OUTPUT_DIRECTORY + "common-words.txt";

    // receiver used to consume twitter stream
    private final Receiver<String> streamReceiver;

    public MostCommonWordsProcessor(Receiver<String> streamReceiver) {

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

        // split into individual words
        JavaDStream<String> words = tweetText.flatMap((text) -> Arrays.asList(text.split("\\s+")));

        // find words longer than 5 characters
        JavaDStream<String> longWords = words.filter((aWord) -> aWord.length() >= 5);

        // create counts for long words
        JavaPairDStream<String, Long> wordCount = longWords
                .mapToPair((aWord) -> new Tuple2<>(aWord, 1L))
                .reduceByKeyAndWindow((a, b) -> a + b,
                        Durations.seconds(30),
                        Durations.seconds(30));

        // filter for words greater than 10 occurrences
        JavaPairDStream<String, Long> filteredCount = wordCount.filter((pair) -> pair._2 > 10);

        // write filtered pair to file
        FileUtilities.writePairToFile(filteredCount, OUTPUT_FILEPATH);

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

