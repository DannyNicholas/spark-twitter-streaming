package com.danosoftware.twitterstream.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

	
}
