package com.aksain.spark.basics.rdds;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author Amit Kumar
 * 
 * Demonstrates the counting of words in input list of sentences.
 *
 */
public class WordCountDemo {
	public static void main(String[] args) {
		// Prepare the spark configuration by setting application name and master node "local" i.e. embedded mode
		final SparkConf sparkConf = new SparkConf().setAppName("Word Count Demo").setMaster("local");
		
		// Create the Java Spark Context by passing spark config
		try(final JavaSparkContext jSC = new JavaSparkContext(sparkConf)) {
			// Create list of sentences
			final List<String> sentences = Arrays.asList(
					"All Programming Tutorials",
					"Getting Started With Apache Spark",
					"Developing Java Applications In Apache Spark",
					"Getting Started With RDDs In Apache Spark"
			);
			// Split the sentences into words, convert words to key, value with key as word and value 1, 
			// and finally count the occurrences of a word
			final Map<String, Object> wordsCount = jSC.parallelize(sentences)
																.flatMap((x) -> Arrays.asList(x.split(" ")))
																.mapToPair((x) -> new Tuple2<String, Integer>(x, 1))
																.countByKey();
			
			System.out.println(wordsCount);
		}
	}
}
