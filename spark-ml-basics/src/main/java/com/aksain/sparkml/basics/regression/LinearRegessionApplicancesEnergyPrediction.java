package com.aksain.sparkml.basics.regression;

import java.io.IOException;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LinearRegessionApplicancesEnergyPrediction {

	public static void main(String[] args) throws IOException {
		// Create Spark Session to create connection to Spark
		final SparkSession sparkSession = SparkSession.builder().appName("Spark Linear Regression Demo")
				.master("local[5]").getOrCreate();

		// Get DataFrameReader using SparkSession and set header option to true
		// to specify that first row in file contains name of columns
		final DataFrameReader dataFrameReader = sparkSession.read().option("header", true);
		final Dataset<Row> trainingData = dataFrameReader.csv("src/main/resources/energydata_complete.csv");

		// Create view and execute query to convert types as, by default, all
		// columns have string types
		trainingData.createOrReplaceTempView("TRAINING_DATA");
		final Dataset<Row> typedTrainingData = sparkSession
				.sql("SELECT cast(Appliances as float) Appl_Energy, cast(T1 as float) T1, cast(RH_1 as float) RH_1, cast(T2 as float) T2, cast(RH_2 as float) RH_2, cast(T3 as float) T3, cast(RH_3 as float) RH_3, "
						+ "cast(T4 as float) T4, cast(RH_4 as float) RH_4, cast(T5 as float) T5, cast(RH_5 as float) RH_5, cast(T6 as float) T6, cast(RH_6 as float) RH_6, "
						+ "cast(T7 as float) T7, cast(RH_7 as float) RH_7, cast(T8 as float) T8, cast(RH_8 as float) RH_8, cast(T9 as float) T9, cast(RH_9 as float) RH_9, "
						+ "cast(T_out as float) T_OUT, cast(Press_mm_hg as float) PRESS_OUT, cast(RH_out as float) RH_OUT, cast(Windspeed as float) WIND, "
						+ "cast(Visibility as float) VIS FROM TRAINING_DATA");
		
		// Combine multiple input columns to a Vector using Vector Assembler
		// utility
		final VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] { "T1", "RH_1", "T2", "RH_2", "T3", "RH_3", "T4", "RH_4", "T5", "RH_5", "T6", "RH_6", "T7", "RH_7", "T8", "RH_8", "T9", "RH_9", 
						"T_OUT", "PRESS_OUT", "RH_OUT", "WIND", "VIS"})
				.setOutputCol("features");
		final Dataset<Row> featuresData = vectorAssembler.transform(typedTrainingData);
		// Print Schema to see column names, types and other metadata
		featuresData.printSchema();

		// Split the data into training and test sets (30% held out for
		// testing).
		Dataset<Row>[] splits = featuresData.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> trainingFeaturesData = splits[0];
		Dataset<Row> testFeaturesData = splits[1];
		
		
		// Load the model
		PipelineModel model = null;
		try {
			model = PipelineModel.load("src/main/resources/applianceenergyprediction");
		} catch(Exception exception) {
		}
		
		if(model == null) {
			// Train a Linear Regression model.
			final LinearRegression regression = new LinearRegression().setLabelCol("Appl_Energy")
					.setFeaturesCol("features");

			// Using pipeline gives you benefit of switching regression model without any other changes
			final Pipeline pipeline = new Pipeline()
					.setStages(new PipelineStage[] { regression });

			// Train model. This also runs the indexers.
			model = pipeline.fit(trainingFeaturesData);
			model.save("src/main/resources/applianceenergyprediction");
		}

		// Make predictions.
		final Dataset<Row> predictions = model.transform(testFeaturesData);
		predictions.show();
	}

}
