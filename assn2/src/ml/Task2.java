package ml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Random;
import scala.Tuple2;

/**
This is the implementation for task 2. Task 2 is mainly about kmeans clustering.
The number of k is given from the input argument.
It performs kmeans on Ly6C, CD11b, and SCA1 measurements of each cell.
The output file contains the following information:
clusterID \t number_of_measurements \t Ly6C \t CD11b \t SCA1
**/

public class Task2 {

	public static void main(String[] args) {
		String inputDataPath = "";
		String stringk = "";
		String stringiter = "";
		String outputDataPath = "";
		// If the number of iterations is specified, use the specified one.
		if (args.length == 4){
			inputDataPath = args[0];
			stringk = args[1];
			stringiter = args[2];
			outputDataPath = args[3];
		// Otherwise, use default 10 iterations
		} else if(args.length == 3){
			inputDataPath = args[0];
			stringk = args[1];
			stringiter = "10";
			outputDataPath = args[2];
		}

		SparkConf conf = new SparkConf();
		int k = Integer.parseInt(stringk);
		int iter = Integer.parseInt(stringiter);

		conf.setAppName("Task 2");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> measurementsData = sc.textFile(inputDataPath);

		// Filter the header Line
		// Filter out the lines with FSC-A or SSC-A outside range 1 to 150,000
		System.out.println("Stage 1");
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

		// Perform KMeans clustering with k clusters and the parsedData
		System.out.println("Stage 2");
		// Initialize k clusteres with random centroids
		ArrayList<Cluster> iniClusters = new ArrayList<Cluster>();
		for(int i = 0; i < k; i++) {
				Point centroid = createRandomPoint(0,3);
				Cluster cluster = new Cluster(i+1);
				cluster.setCentroid(centroid);
				iniClusters.add(cluster);
		}
		JavaRDD<Cluster> allClusters = sc.parallelize(iniClusters);
		allClusters = KMeans(parsedData, k, iter, allClusters);

		//Return the output RDD to contain cluster id, numberOfMeasurements, centroid values
		System.out.println("Stage 3");
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
		outputClusters.coalesce(1).saveAsTextFile(outputDataPath + "Task2");
		sc.close();
	}

	// This is the function to perform KMeans clustering algorithm
	// This function return a JavaRDD with Cluster class instances
	public static JavaRDD<Cluster> KMeans(JavaRDD<Point> parsedData, int k, int iter, JavaRDD<Cluster> allClusters) {
		//Run the kmeans iteration 10 times
		for (int i = 0; i < iter; i++){
			// Assign points to the nearest clusters, output the interger to be the id of the assigned cluster
			List<Cluster> listCluster = allClusters.collect();
			JavaPairRDD<Integer, Iterable<Point>> assignedPoints = parsedData.mapToPair(p -> mapNearestCentroid(p, listCluster)).groupByKey(1);
			JavaPairRDD<Integer, ArrayList<Point>> iterateAssignedPoints = assignedPoints.mapToPair((ip) -> {
				ArrayList<Point> iterPoints = new ArrayList<Point>();
				for (Point point: ip._2){
					iterPoints.add(point);
				}
				return new Tuple2<Integer, ArrayList<Point>>(ip._1, iterPoints);
			});

			// Calculate new clusters and update the cluster RDD
			allClusters = iterateAssignedPoints.map((iap) -> calculateNewCluster(iap._1, iap._2));
		}
		return allClusters;
	}

  // Create random points for centroids
	public static Point createRandomPoint(int min, int max) {
		Random r = new Random();
		double x = min + (max - min) * r.nextDouble();
		double y = min + (max - min) * r.nextDouble();
		double z = min + (max - min) * r.nextDouble();
		return new Point(x,y,z);
	}

  // Update the centroids result
	// Calculate the average dimension variables among all the points in that cluster to obtain the new centroids
	// This function returns a Cluster instance for the updated cluster
	public static Cluster calculateNewCluster(Integer clusterid, ArrayList<Point> points) throws Exception {
		double averageX = 0.0;
		double averageY = 0.0;
		double averageZ = 0.0;
		int number = points.size();
		Cluster cluster = new Cluster(clusterid);
		for(Point p: points){
			averageX += p.getX();
			averageY += p.getY();
			averageZ += p.getZ();
			cluster.addPoint(p);
		}
		averageX = averageX / number;
		averageY = averageY / number;
		averageZ = averageZ / number;
		Point centroid = new Point(averageX, averageY, averageZ);
		cluster.setCentroid(centroid);
		return cluster;
	}

	// Map the points to its nearest centroid
	// This function calculates the nearest centroid to the point
	// The return is a tuple of the cluster id and the point itself
	public static Tuple2<Integer, Point> mapNearestCentroid(Point p, List<Cluster> allClusters){
				double minDistance = Double.MAX_VALUE;
				int closestCentroidId = -1;
				// check all cluster centers
				int size = allClusters.size();
				//int size = 1;
				for (int j = 0; j < size; j++) {
					// compute distance
					Point centroid = allClusters.get(j).getCentroid();
					//Point centroid = new Point(1.2,2.2,0.5);
					if (centroid == null) {
						continue;
					} else {
						double distance = distance(p,centroid);

						// update nearest cluster if necessary
						if (distance < minDistance) {
							minDistance = distance;
							closestCentroidId = allClusters.get(j).id;
							p.setClusterNumber(closestCentroidId);
							p.setNearestDist(minDistance);
						}
					}
				}

				// emit a new record with the center id and the data point.
				return new Tuple2<Integer,Point>(closestCentroidId, p);
		}

		// The helper function to calculate the distance between two points
		public static double distance(Point p, Point centroid) {
				return Math.sqrt(Math.pow((centroid.getY() - p.getY()), 2) + Math.pow((centroid.getX() - p.getX()), 2)
												+ Math.pow((centroid.getZ() - p.getZ()), 2));
		}
}
