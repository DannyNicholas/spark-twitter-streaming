package com.danosoftware.twitterstream.main;

import com.danosoftware.twitterstream.config.AppConfig;
import com.danosoftware.twitterstream.streaming.TwitterStreamProcessor;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;

/**
 * Main class used to start the Twitter streaming processing
 */
public class Main {

    /**
     * Retrieve the Twitter Streaming Processor from Spring. Start the processor.
     * @param args - unused arguments
     */
    public static void main(String[] args) {
        AbstractApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        TwitterStreamProcessor processor = (TwitterStreamProcessor) context.getBean("twitterStreamProcessor");
        processor.go();
        context.close();
    }
}
