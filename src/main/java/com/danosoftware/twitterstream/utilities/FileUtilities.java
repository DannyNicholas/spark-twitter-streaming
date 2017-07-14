package com.danosoftware.twitterstream.utilities;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class FileUtilities {

	private static Logger logger = LoggerFactory.getLogger(FileUtilities.class);

	// do not allow construction
	private FileUtilities() {
	}

	/**
	 * Append the supplied list of text strings to a text filename.
	 * 
	 * If the text file does not exist, then create it first.
	 * 
	 * @param fileName
	 *            - full path of file
	 * @param text
	 *            - list of strings to be appended
	 */
	public static void appendText(final String fileName, final List<String> text) {
		Path filePath = Paths.get(fileName);
		try {
			Files.write(filePath, text, Charset.forName("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		} catch (IOException e) {
			logger.error("Error while writing to file '{}'.", filePath.getFileName());
			throw new RuntimeException(e);
		}
	}

	/**
	 * Delete a file at the supplied pathname if it exists.
	 * 
	 * @param fileName
	 *            - full path of file
	 */
	public static void deleteIfExists(final String fileName) {
		Path filePath = Paths.get(fileName);
		try {
			Files.deleteIfExists(filePath);
		} catch (IOException e) {
			logger.error("Error while deleting file '{}'.", filePath.getFileName());
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create the directory (and parent directories) specified by supplied
	 * directory.
	 * 
	 * @param directory
	 *            - directory path
	 */
	public static void createDirectory(final String directory) {
		Path directoryPath = Paths.get(directory);
		try {
			Files.createDirectories(directoryPath);
		} catch (IOException e) {
			logger.error("Error while creating directory '{}'.", directoryPath.getFileName());
			throw new RuntimeException(e);
		}
	}

	/**
	 * Write Spark DStream pair to a text file.
	 *
	 * @param pairStream - Spark DStream pair
	 * @param fileName - filename to write to
	 * @param <K> - key type
	 * @param <V> - value type
	 */
	public static <K, V> void writePairToFile(JavaPairDStream<K, V> pairStream, final String fileName) {

		pairStream.foreachRDD((pairs) -> {
			List<String> outputText = new ArrayList<>();
			for (Tuple2<K, V> item : pairs.collect()) {
				outputText.add(item._1.toString() + " : " + item._2.toString());
			}
			outputText.add("------------------------");
			FileUtilities.appendText(fileName, outputText);
			return null;
		});
	}

	/**
	 * Write Spark DStream to a text file.
	 *
	 * @param stream - Spark DStream
	 * @param fileName - filename to write to
	 * @param <T> - stream type
	 */
	public static <T> void writeToFile(JavaDStream<T> stream, final String fileName) {

		stream.foreachRDD((items) -> {
			List<String> outputText = new ArrayList<>();
			for (T anItem : items.collect()) {
				outputText.add(anItem.toString());
			}
			outputText.add("------------------------");
			FileUtilities.appendText(fileName, outputText);
			return null;
		});
	}

	
}
