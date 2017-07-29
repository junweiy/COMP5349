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

import java.util.Random;

public class Point implements Serializable{

    private double x = 0.0;
    private double y = 0.0;
    private double z = 0.0;
    private int clusterNumber = -1;
    private double distToNearestCluster = -1.0;

    public Point(double x, double y, double z)
    {
        this.setX(x);
        this.setY(y);
        this.setZ(z);
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getX()  {
        return this.x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getY() {
        return this.y;
    }

    public void setZ(double z) {
        this.z = z;
    }

    public double getZ()  {
        return this.z;
    }

    public void setClusterNumber(int n) {
        this.clusterNumber = n;
    }

    public int getClusterNumber() {
        return this.clusterNumber;
    }

    public double getNearestDist() {
        return this.distToNearestCluster;
    }

    public void setNearestDist(double dist) {
        this.distToNearestCluster = dist;
    }

    //Calculates the distance between two points.
    protected static double distance(Point p, Point centroid) {
        return Math.sqrt(Math.pow((centroid.getY() - p.getY()), 2) + Math.pow((centroid.getX() - p.getX()), 2)
                        + Math.pow((centroid.getZ() - p.getZ()), 2));
    }

    //Creates random point
    protected static Point createRandomPoint(int min, int max) {
    	Random r = new Random();
    	double x = min + (max - min) * r.nextDouble();
      double y = min + (max - min) * r.nextDouble();
    	double z = min + (max - min) * r.nextDouble();
    	return new Point(x,y,z);
    }

    public String toString() {
    	return "("+x+","+y+")";
    }
}
