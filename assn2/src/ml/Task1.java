package ml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/* 
* Task 1 is a the first task that needs the record of number of measurements taken by each
* researcher. Firstly data from two datasets is extracted, and then will be filtered based
* on certain criteria. Afterwards two datasets will be joined by sample column and number
* of measurements taken by each researcher can be extracted from joined results.
* The output format of Task 1 is:
* researcher \t numberOfMeasurements
*/
public class Task1 {

	public static void main(String[] args) {
	 	// Path of measurement data	
		String inputDataPath1 = args[0];
		// Path of experiment data
		String inputDataPath2 = args[1];
		String outputDataPath = args[2];
		SparkConf conf = new SparkConf();
		
		conf.setAppName("Task1");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> measurementData = sc.textFile(inputDataPath1),
				experimentData = sc.textFile(inputDataPath2);

		// Remove first line for both input files
		JavaRDD<String> filteredMeasurementData = measurementData.filter(s -> {
			String[] values = s.split(",");
			return !values[0].equals("sample");
		});
		JavaRDD<String> filteredExperimentData = experimentData.filter(s -> {
			String[] values = s.split(",");
			return !values[0].equals("sample");
		});

		// Read from measurement file, extract samples with FSC-A and SSC-A value
		// between 0 and 150,000.
		// Input format:
		// sample, FSC-A, SSC-A, CD48, Ly6G, CD117, SCA1, CD11b, CD150, ...
		// Output format:
		// sample, count
		JavaPairRDD<String, Integer> validSamples = filteredMeasurementData.mapToPair(s -> {
			String[] values = s.split(",");
			// Skip the first line	
			if (values[0].equals("sample")) {
				return new Tuple2<String, Integer>(values[0], 0);
			}
			// Check FSC-A within valid range
			if (Integer.parseInt(values[1]) < 0 || Integer.parseInt(values[1]) > 150000) {
				return new Tuple2<String, Integer>(values[0], 0);
			}
			// Check SSC-A within valid range
			if (Integer.parseInt(values[2]) < 0 || Integer.parseInt(values[2]) > 150000) {
				return new Tuple2<String, Integer>(values[0], 0);
			}
			return new Tuple2<String, Integer>(values[0], 1);
		}).reduceByKey((n1, n2) -> n1 + n2);

		// Read from experiment file, extract samples and corresponding researcher.
		// Input format:
		// sample, date, experiment, day, subject, kind, instrument, researchers
		// Output format:
		// sample, researcher
		JavaPairRDD<String, String> sampleResearcher = filteredExperimentData.flatMapToPair(s-> {
			String[] values = s.split(",");
			ArrayList<Tuple2<String, String>> results = new ArrayList<Tuple2<String, String>>();
			if (values.length < 7) {
				return results.iterator();
			}
			String[] researchers = values[7].split(";");
			for (String researcher: researchers) {
				results.add(new Tuple2<String, String>(values[0], researcher.trim()));
			}
			return results.iterator();
		});

		// Join two RDDs
		// Output format:
		// sample, (researcher, count)
		JavaPairRDD<String, Tuple2<String, Integer>> joinResults = sampleResearcher.join(validSamples);

		// Sum number of measurements
		JavaPairRDD<String, Integer> result = joinResults.values().mapToPair(v -> {
			return new Tuple2<String, Integer>(v._1, v._2);
		}).reduceByKey((n1, n2) -> n1 + n2);
		
		// Swap key value pair for sorting
		JavaPairRDD<Integer, String> swapedResult = result.mapToPair(s -> {
			return new Tuple2<Integer, String>(s._2, s._1);
		}).sortByKey(false);

		// Swap again 
		JavaPairRDD<String, Integer> preFinalResult = swapedResult.mapToPair(s -> {
			return new Tuple2<String, Integer>(s._2, s._1);
		});

		// Map to final output
		JavaRDD<String> finalResult = preFinalResult.map(s -> {
			String output = s._1 + "\t" + String.valueOf(s._2);
			return output;
		});
		
		finalResult.coalesce(1).saveAsTextFile(outputDataPath + "Task1");
		sc.close();
	}
}
