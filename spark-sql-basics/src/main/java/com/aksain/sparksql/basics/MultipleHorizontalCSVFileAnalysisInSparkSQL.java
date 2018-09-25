package com.aksain.sparksql.basics;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Demonstrates analysing CSV data by executing SQL like queries in Apache Spark SQL.
 * 
 * @author amit-kumar
 */
public class MultipleHorizontalCSVFileAnalysisInSparkSQL {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Create Spark Session to create connection to Spark
		final SparkSession sparkSession = SparkSession.builder().appName("Spark CSV Analysis Demo").master("local[5]")
				.getOrCreate();

		// Get DataFrameReader using SparkSession
		final DataFrameReader dataFrameReader = sparkSession.read();
		// Set header option to true to specify that first row in file contains
		// name of columns
		dataFrameReader.option("header", "true");
		
		// Read CSV files
		final Dataset<Row> csvDataFrame = dataFrameReader.csv("src/main/resources/data-horiz-part1.csv", "src/main/resources/data-horiz-part2.csv");
		
		// Print Schema to see column names, types and other metadata
		csvDataFrame.printSchema();

		// Create view and execute query to convert types as, by default, all columns have string types
		csvDataFrame.createOrReplaceTempView("ROOM_OCCUPANCY_RAW");
		final Dataset<Row> roomOccupancyData = sparkSession
				.sql("SELECT CAST(id as int) id, CAST(date as string) date, CAST(Temperature as float) Temperature, "
						+ "CAST(Humidity as float) Humidity, CAST(Light as float) Light, CAST(CO2 as float) CO2, "
						+ "CAST(HumidityRatio as float) HumidityRatio, CAST(Occupancy as int) Occupancy FROM ROOM_OCCUPANCY_RAW");
		
		// Print Schema to see column names, types and other metadata
		roomOccupancyData.printSchema();
		
		// Create view to execute query to get filtered data
		roomOccupancyData.createOrReplaceTempView("ROOM_OCCUPANCY");
		sparkSession.sql("SELECT * FROM ROOM_OCCUPANCY WHERE Temperature >= 23.6 AND Humidity > 27 AND Light > 500 "
				+ "AND CO2 BETWEEN 920 and 950").show();
	}
}