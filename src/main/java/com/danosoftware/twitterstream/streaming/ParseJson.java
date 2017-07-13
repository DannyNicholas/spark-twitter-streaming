package com.danosoftware.twitterstream.streaming;

import com.danosoftware.twitterstream.data.Tweet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Function to extract a collection of tweets from a collection of lines of
 * text.
 * 
 * It is assumed that each line of text represents a fully formed JSON string.
 * Otherwise a JSON Syntax Exception will the thrown and the line will be
 * skipped.
 *
 */
@SuppressWarnings("serial")
public class ParseJson implements FlatMapFunction<Iterator<String>, Tweet>
{
    private static Logger logger = LoggerFactory.getLogger(ParseJson.class);

    public Iterable<Tweet> call(Iterator<String> lines) throws Exception
    {
        List<Tweet> tweets = new ArrayList<Tweet>();

        // GSON used to extract objects from JSON
        Gson gson = new GsonBuilder().create();

        while (lines.hasNext())
        {
            String line = lines.next();
            try
            {
                /*
                 * Create new Tweet from current line of JSON. Note GSON will
                 * only map values where JSON field names match Tweet class
                 * field names.
                 */
                Tweet tweet = gson.fromJson(line, Tweet.class);

                // add Tweet to collection
                tweets.add(tweet);
            }
            catch (JsonSyntaxException e)
            {
                // skip records on failure
                logger.error("Error reading line: {}. Exception follows.", line, e);
            }
        }

        return tweets;
    }
}
