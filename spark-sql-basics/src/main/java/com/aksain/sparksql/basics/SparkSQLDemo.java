/**
 * 
 */
package com.aksain.sparksql.basics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author Amit Kumar
 *
 */
public class SparkSQLDemo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		final SparkSession sparkSession = SparkSession.builder().appName("Spark SQL Demo").master("local[10]").getOrCreate();
		
		final Dataset<Row> sourceJSON = sparkSession.read().json("src/main/resources/data.json");
		sourceJSON.printSchema();
		sourceJSON.createOrReplaceTempView("people");
		
		final Dataset<Row> namesDF = sparkSession.sql("SELECT name FROM people WHERE age = 30");
		namesDF.show();
		
		Dataset<Row> sourceCSV = sparkSession.read().csv("src/main/resources/data.csv");
		sourceCSV.printSchema();
		sourceCSV.show();
		sourceCSV.createOrReplaceTempView("people");
		for(String col : sourceCSV.columns()) {
			System.out.println(col);
		}
	}

}
