package ml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Random;
import scala.Tuple2;

/* 
* Task 3 is an extension of Task 2. Based on the cluster coordinates produced by Task 2,
* 10% of furthest points from each of its corresponding cluster will be removed and 
* KMeans algorithm will run again with remaining points and other configurations inherited
* from Task 2.
* The output format is the same with Task 2:
* clusterID \t number_of_measurements \t Ly6C \t CD11b \t SCA1
*/

public class Task3 {

	public static void main(String[] args) {
		String inputDataPath, clusterPath, stringk, stringiter, outputDataPath;
		if (args.length == 5) {
			// Data path of measurement files
			inputDataPath = args[0];
			// Data path of precomputed clusters
			clusterPath = args[1];
			// Number of cluster classes
			stringk = args[2];
			// Number of iterations
			stringiter = args[3];
			// Data path of output file
			outputDataPath = args[4];
		} else if (args.length == 4) {
			// Data path of measurement files
			inputDataPath = args[0];
			// Data path of precomputed clusters
			clusterPath = args[1];
			// Number of cluster classes
			stringk = "10";
			// Number of iterations
			stringiter = args[2];
			// Data path of output file
			outputDataPath = args[3];
		} else {
			System.out.println("Invalid command argument.");
			return;
		}
		

		// Initialise Spark environment
		SparkConf conf = new SparkConf();
		conf.setAppName("Task 3");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read in parameters
		int k = Integer.parseInt(stringk);
		int iter = Integer.parseInt(stringiter);

		JavaRDD<String> measurementsData = sc.textFile(inputDataPath);
		JavaRDD<String> clusterData = sc.textFile(clusterPath);

		// Filter the header Line
		// Filter out the lines with FSC-A or SSC-A outside range 1 to 150,000
		JavaRDD<String> filteredMeasurementData = measurementsData.filter(s -> {
			String[] values = s.split(",");
			return !values[0].equals("sample") && Integer.parseInt(values[1]) >= 1 && Integer.parseInt(values[1]) <= 150000
							&& Integer.parseInt(values[2]) >= 1 && Integer.parseInt(values[2]) <= 150000;
		});

    	// Parse the three dimension information into parsedData
		// Turn the three dimensions of each measurements into a Point instance
		JavaRDD<Point> parsedData = filteredMeasurementData.map(s -> {
			String[] sarray = s.split(",");
			double x = Double.parseDouble(sarray[11]);
			double y = Double.parseDouble(sarray[7]);
			double z = Double.parseDouble(sarray[6]);
			return new Point(x,y,z);
		});

		// Read in all clusters and store them as Cluster instances
		JavaRDD<Cluster> allClusters = clusterData.map(s -> {
			String[] values = s.split("\t");
			Cluster cl = new Cluster(Integer.parseInt(values[0]));
			cl.setCentroid(new Point(Double.parseDouble(values[2]), Double.parseDouble(values[3]), 
				Double.parseDouble(values[4])));
			return cl;
		});

		// Compute nearest cluster for all points
		List<Cluster> listCluster = allClusters.collect();
		JavaPairRDD<Integer, Iterable<Point>> assignedPoints = parsedData.mapToPair(p -> Task2.mapNearestCentroid(p, listCluster)).groupByKey(1);
		// A List of group of points for each cluster
		List<Iterable<Point>> pointsByID = assignedPoints.values().collect();
		// Empty ArrayList to store all points
		ArrayList<Point> allPoints = new ArrayList<Point>();

		// Remove outliers and add all remaining points to new ArrayList
		for (Iterable<Point> points: pointsByID) {
			allPoints.addAll(removeOutlier(points));
		}

		JavaRDD<Point> filteredPoints = sc.parallelize(allPoints);

		// Run KMeans with filtered points
		allClusters = Task2.KMeans(filteredPoints, k, iter, allClusters);

		//Return the output RDD to contain cluster id, numberOfMeasurements, centroid values
		JavaPairRDD<Integer, String> clustersNumber = allClusters.mapToPair(cluster -> {
			int clusterid = cluster.id;
			int numberOfMeasurements = cluster.points.size();
			double centroidX = cluster.getCentroid().getX();
			double centroidY = cluster.getCentroid().getY();
			double centroidZ = cluster.getCentroid().getZ();
			String output = String.valueOf(numberOfMeasurements) + "\t" + String.valueOf(centroidX)
										+ "\t" + String.valueOf(centroidY) + "\t" + String.valueOf(centroidZ);
			return new Tuple2<Integer, String>(clusterid, output);
		}).sortByKey(true);

		// Map all the outputs into a String RDD in order to print the results
		JavaRDD<String> outputClusters = clustersNumber.map(info -> {
			String printout = String.valueOf(info._1) + "\t" + info._2;
			return printout;
		});
		outputClusters.coalesce(1).saveAsTextFile(outputDataPath + "Task3");
		sc.close();
	}

	public static List<Point> removeOutlier(Iterable<Point> points) {
		// Convert Iterable to ArrayList
		ArrayList<Point> pointsList = new ArrayList<Point>();
		Iterator<Point> iter = points.iterator();
		while (iter.hasNext()) {
			pointsList.add(iter.next());
		}
		// Sort the ArrayList by distance to nearest cluster
		Collections.sort(pointsList, new Comparator<Point>() {
			@Override
			public int compare(Point a, Point b) {
				if (a.getNearestDist() < b.getNearestDist()) {
					return -1;
				} else if (a.getNearestDist() == b.getNearestDist()) {
					return 0;
				} else {
					return 1;
				}
			}
		});
		// Remove outliers
		int sizeKept = (int) (0.9 * pointsList.size());
		for (int i = pointsList.size() - 1; i > sizeKept; i--) {
			pointsList.remove(i);
		}
		return pointsList;
	}

}
