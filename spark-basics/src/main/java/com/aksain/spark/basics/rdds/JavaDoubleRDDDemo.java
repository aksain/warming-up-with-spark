package com.aksain.spark.basics.rdds;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Amit Kumar
 * 
 * Demonstrates the usage of JavaDoubleRDD with a use case involving following steps -
 * 		- Filter out the numbers greater than 10
 * 		- Transform the numbers by calculating their squares
 * 		- Find out sum of all the transformed numbers
 * 
 */
public class JavaDoubleRDDDemo {
	
	public static void main(String[] args) {
		// Prepare the spark configuration by setting application name and master node "local" i.e. embedded mode
		final SparkConf sparkConf = new SparkConf().setAppName("Java Double RDD Demo").setMaster("local");
		
		// Create the Java Spark Context by passing spark config.
		try(final JavaSparkContext jSC = new JavaSparkContext(sparkConf)) {
			//Create Java Double RDD of type double with list of doubles
			final JavaDoubleRDD doubleRDD = jSC.parallelizeDoubles(Arrays.asList(1.5, 2.2, 3.1, 4.2, 50.1, 61.3, 72.8, 8.2, 9.5, 19.6, 31.7, 42.8, 53.3, 6.6, 7.4, 23.1));
			// Create a new Java Double RDD by removing doubles greater than 10 from double RDD
			final JavaDoubleRDD filteredRDD = doubleRDD.filter((x) -> (x > 10 ? false : true));
			// Create a new transformed RDD by transforming the numbers to their squares
			final JavaDoubleRDD transformedRDD = filteredRDD.mapToDouble((x) -> ( x * x) );
			// Calculate the sum of all transformed doubles. Since reduce is a value function, it will trigger actual execution
			final double sumTransformed = transformedRDD.reduce( (x, y) -> (x + y) );
			
			System.out.println(sumTransformed);
		}
	}
}
