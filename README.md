# Processing a Twitter Stream using Apache Spark

This project is a demonstration of Spark processing a real-time Twitter stream.

In this demonstration, a Spark receiver connects to a Twitter stream via a client. This stream of tweets is then pushed into Spark for processing.

**NOTE**: This is a very simple starter project that focuses on connecting Spark to a twitter stream. At this stage the Spark processing simply involves extracting individual fields from the tweet and saving them to a text file. A future implementation could do some more advanced processing and write these results to a suitable scalable database (such as Apache Cassandra).


### Getting Started

Start by downloading or cloning this repostory to a directory of your choice.

Build the projects using Maven.

```sh
$ cd <path-to-cloned-repository>
$ mvn clean install
```


### Twitter Streaming API

To connect to a Twitter Stream you need a developer account. Once you create a twitter application, you will be given the API tokens needed.

https://apps.twitter.com/

This Spark application needs these Twitter API tokens to successfully connect to the stream. These tokens should be provided in the file:

`resources/authentication.properties`

The format of this file should look like this:

```
consumerKey=my-consumer-key
consumerSecret=my-consumer-secret
token=my-token
tokenSecret=my-token-secret
```

Ensure this file is kept secret and not commited to any public respository. See `resources/authentication.properties.sample` as an example.


### Running the Application

Start the application by running the Java main method in `Main.java`.

This will create a receiver that connects to the Twitter stream using the client. The stream will be processed by extracting specific fields from each tweet and savng them to a file at:

```
<user-home>/spark-results/tweets.txt
``` 


### Configuring the Application

The majority of configuration is held in the Java class `AppConfig.java`.

In particular, you can choose:

- Which users' tweets to follow (see `followings()` method)
- Which users' terms to search for (see `terms()` method)

This application uses the Hosebird Client (HBC) for connecting to the Twitter stream. Please see this project for full details of how to customise the Twitter stream filter:

https://github.com/twitter/hbc


### Extract Tweet Information

The default implementation only extracts the following fields from each tweet:

- `id`
- `text`

It is possible to extract more fields for processing by modifying the `Tweet.java` class. The `ParseJson.java` class is responsible for creating each tweet from the source JSON and returns instances of the `Tweet.java` class.

As an example, the JSON structure of a tweet can be seen at `tweet-example.json`.


### Summary of Java classes

Below is a quick summary of the most important classes used in this application:

- `Main.java` : Starts up application
- `AppConfig.java` : Configures the application
- `Tweet.java` : Represents an individual tweet
- `ParseJson` : Used to extract the wanted tweet fields from the JSON tweet structure
- `TwitterStreamReceiver` : Spark receiver that connects and consumes the stream via the client
- `TwitterStreamProcessor` : Processes the stream of tweets using Spark