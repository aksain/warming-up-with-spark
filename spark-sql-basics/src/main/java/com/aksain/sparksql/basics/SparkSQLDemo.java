package com.aksain.sparksql.basics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Demonstrates reading of a JSON file using Spark SQL.
 * 
 * @author Amit Kumar
 */
public class SparkSQLDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Create Spark Session to create connection to Spark
		final SparkSession sparkSession = SparkSession.builder().appName("Spark SQL Demo").master("local[5]").getOrCreate();
		
		// Load JSON file data into DataFrame using SparkSession
		final Dataset<Row> jsonDataFrame = sparkSession.read().json("src/main/resources/data.json");
		// Print Schema to see column names, types and other metadata
		jsonDataFrame.printSchema();
		
		// Query name column from JSON where age column value is equal to 30
		
		// DSL API with conditional expression
		System.out.println("DSL API with Condition Expression:");
		jsonDataFrame.select("name").where("age = 30").show();
		// Pure DSL API
		System.out.println("Pure DSL API:");
		jsonDataFrame.select("name").where(jsonDataFrame.col("age").equalTo(30)).show();
		
		// Create a view on DataFrame and execute the query on created view using SparkSession
		System.out.println("SQL Query:");
		jsonDataFrame.createOrReplaceTempView("people");
		sparkSession.sql("SELECT name FROM people WHERE age = 30").show();
	}
}