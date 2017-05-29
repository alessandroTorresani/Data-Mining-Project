package bdmp2.project2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;


public class App 
{
	public static void main( String[] args ) throws IOException
	{
		final double EPS = 0.9;
		final int MINPTS = 5;
		final int cellSize = 1;
		final int gridInterval = 30;
		final int dimension = 2;
		final double splitvalue =gridInterval/2;
		final boolean linear = false;

		// System variable to make Spark work on Windows
		System.setProperty("hadoop.home.dir", "C:/hadoop");

		// Create and clean folders used for execution
		Utilities.initializeFolders();

		// Sample a new dataset
		//Utilities.createLargeDataset(800,cellSize, gridInterval, dimension);

		// Reads the dataset
		List<Point> points = Utilities.readDataset("dataset.txt", dimension);
		
		


		/*--------------------------------
		 *|			LINEAR PART 	     |	
		 *-------------------------------- 		
		 */
		if (linear){
			long startTime = System.currentTimeMillis();
			
			//Build Map of the neighbors
			Map<Point,List<Point>> neighbors = Utilities.buildNeighborsIndex(points, EPS); 
			
			// Cluster the points
			Map<String, List<Point>> clusters = Utilities.UDBScanWithProcessing(points, EPS, MINPTS,neighbors);
			System.out.println("Points clustered: output stored at "+System.getProperty("user.home")+"/Documents/bdmpFiles/output/");

			// Print cluster's info
			//Utilities.printClusteringInfo(clusters);
			
			//Utilities.postProcess(clusters, MINPTS);
			
			Utilities.printClusteringInfo(clusters);

			// Save clusters on the disk
			try {
				Utilities.saveClusters(clusters);
			} catch (FileNotFoundException e) {
				// TODO: handle exception
				System.err.println(e.getMessage());
			}
			
			long stopTime = System.currentTimeMillis();
			System.out.println("Linear Time: " + (stopTime-startTime)/1000);
			/* COST OF DBSCAN: O(n*m) where m is the cost of the neighbor query. So without indexing the cost of m is O(n) and thus the global cost is O(n^2)
			 * IDEAS for parallel part
			 * SIMPLE ONE: do only the "find neighbors part" in parallel. For example divide the data-set in 4 parts and assign each part to a cluster. The problem is
			 * how to divide the data-set in a way that every cluster can compute its part independently?
			 * I think that we have to implement for sure an indexing structure because we have to work on spatial areas. R-Tree can be a solution.
			 * HARD ONE: do the clustering in parallel. Do a local cluster in each area and at the end merge the results. Since the performance of this solution 
			 * depends on the smallest thread, an algorithm for dividing the work evenly is needed
			 */
		} else {
			/*--------------------------------
			 *|			PARALLEL PART 	     |	
			 *-------------------------------- 		
			 */
			long startTime = System.currentTimeMillis();

			// Spark configuration
			SparkConf conf = new SparkConf();
			conf.setAppName("MrClustering").setMaster("local[2]"); // 2 local workers
			JavaSparkContext sc = new JavaSparkContext(conf);

			// Make the dataset available to all workers (READ-ONLY variable)
			Broadcast<List<Point>> broadcastDataset = sc.broadcast(points);
			
			// Convert the dataset into a JavaRDD object and assign to each point a partition id
			JavaRDD<Point> pointsRDD = sc.parallelize(points);
			JavaPairRDD<String, Point> partitions = SparkUtilities.createPartitions(pointsRDD,splitvalue);

			//JavaPairRDD<String,Point> first_partion = partitions.filter(s -> s._1.equals('0'));
			//JavaPairRDD<String,Point> second_partion = partitions.filter(s -> s._1.equals('1'));

			// Collect all points belonging to the same partition
			JavaPairRDD<String,Iterable<Point>> partitionsGrouped = partitions.groupByKey();
			
			// Expand partitions
			JavaPairRDD<String,Iterable<Point>> expandedPartitions = partitionsGrouped.mapToPair(new PairFunction<Tuple2<String,Iterable<Point>>, String, Iterable<Point>>() {

				@Override
				public Tuple2<String, Iterable<Point>> call(Tuple2<String, Iterable<Point>> t) throws Exception {
					List<Point> expanded = new ArrayList<>();
					if(t._1.equals("0")){
						Iterator it = t._2.iterator();
						while(it.hasNext()){
							expanded.add((Point) it.next());
						}
						for(Point p : broadcastDataset.value()){
							if(!expanded.contains(p) && p.average()[0] <= splitvalue + cellSize){ // Look only at the first dimension
								expanded.add(p);
							}
						}
					} else if(t._1().equals("1")){
						Iterator it = t._2.iterator();
						while(it.hasNext()){
							expanded.add((Point) it.next());
						}
						for(Point p : broadcastDataset.value()){
							if(!expanded.contains(p) && p.average()[0] >= splitvalue - cellSize){ // Look only at the first dimension
								expanded.add(p);
							}
						}
					}
					return new Tuple2<String, Iterable<Point>>(t._1, expanded);
				}
			}).cache();	// Make this RDD persistent

			// Now apply a local UDBSCAN locally
			JavaRDD<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> local_clusters = expandedPartitions.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Point>>, Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>>() {

				@Override
				public Iterator<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> call(Tuple2<String, Iterable<Point>> t) throws Exception {
					List<Point> points = new ArrayList<>(); // First convert the Iterable<Point> to a List<Point>
					Iterator it = t._2.iterator();
					while(it.hasNext()){
						points.add((Point) it.next());
					}
					
					//Build Map of the neighbors
					Map<Point,List<Point>> neighbors = Utilities.buildNeighborsIndex(points, EPS); 
					
					// Apply a local cluster
					Map<String,List<Point>> clusters = Utilities.UDBScan(points, EPS, MINPTS,neighbors);
					clusters.keySet();

					/*
					 * 	Mark clustered points with partition id
					 *  QUESTION: maybe we can make another version of UDBSCAN that directly marks points with a partition parameter once that they 
					 *  are merged into the cluster.
					 *  NOT WORK BECAUSE POINTS ARE LOCAL VARIABLES INSIDE THE WORKERS
					 */
//					for(Map.Entry<String, List<Point>> s : clusters.entrySet()){
//						for(Point p : s.getValue()){
//							p.clustered[Integer.parseInt(t._1)] = true;
//						}
//					}
					
					// Create the list containing the clusters
					List<Tuple2<String,Iterable<Point>>> a2= new ArrayList<>();
					for(Map.Entry<String, List<Point>> s : clusters.entrySet()){
						a2.add(new Tuple2<String, Iterable<Point>>(s.getKey(), s.getValue()));
					}

					// Create list containing the partition id and the clusters
					List<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> list = new ArrayList();
					list.add(new Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>(t._1, a2));

					return list.iterator();
				}

			}).cache(); // Make this RDD persistent

			// Save output to the disk
			local_clusters.saveAsTextFile(System.getProperty("user.home")+"/Documents/bdmpFiles/output/spark");
			
			// From javaRDD to list. Separate the output of the two partitions.
			List<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> localclusters = local_clusters.collect();
			List<Map<String,List<Point>>> maps = SparkUtilities.mapClusters(localclusters);
			
			// Merge local clusters. Clusters, that contains duplicated points should be merged.
			Map<String,List<Point>> clusters = SparkUtilities.mergeClusters(maps);
			
			// Check statistic (optinal)
			Map<String,String> duplicates = TestingUtilities.checkDuplicates(clusters);
			TestingUtilities.checkDuplicatesSameSet(clusters);
			System.out.println("Duplicates points: " + TestingUtilities.getPointDuplicates(clusters, "6", "10"));
			System.out.println(duplicates.toString());
			
			// Print clustering info
			//Utilities.printClusteringInfo(clusters);
			
			Utilities.postProcess(clusters, MINPTS);
			
			Utilities.printClusteringInfo(clusters);
			
			// Save clusters on the disk 
			try {
				Utilities.saveClusters(clusters);
			} catch (FileNotFoundException e) {
				// TODO: handle exception
				System.err.println(e.getMessage());
			}
	
			long stopTime = System.currentTimeMillis();
			System.out.println("Spark Time: " + (stopTime-startTime)/1000);
						
			// Close and stop SparkContext
			sc.stop();
			sc.close();
			
		}
	}
}
