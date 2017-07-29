package ml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.io.Serializable;

public class Cluster implements Serializable{

	// The global variable points is defined to contain the array list of all the points assigned to this cluster
	public ArrayList<Point> points;
	// The global variable centroid is defined to store the centroid point for this cluster
	public Point centroid;
	// The global variable id is defined to store the cluster id
	public int id;

	//Creates a new Cluster
	public Cluster(int id) {
		this.id = id;
		this.points = new ArrayList<Point>();
		this.centroid = null;
	}

	public List getPoints() {
		return points;
	}

	public void addPoint(Point point) {
		this.points.add(point);
	}

	public void setPoints(ArrayList<Point> points) {
		this.points = points;
	}

	public Point getCentroid() {
		return centroid;
	}

	public void setCentroid(Point centroid) {
		this.centroid = centroid;
	}

	public int getId() {
		return id;
	}

	public void clear() {
		points.clear();
	}
}
