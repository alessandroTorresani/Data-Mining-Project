package bdmp2.project2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;


public class App 
{
	public static void main( String[] args ) throws IOException
	{
		double eps;
		int minPts;
		final int cellSize = 1;
		final int gridInterval = 80;
		final int dimension = 2;
		final double splitvalue =gridInterval/2;
		boolean linear = true;
		
		// System variable to make Spark work on Windows
		System.setProperty("hadoop.home.dir", "C:/hadoop");

		// Create and clean folders used for execution
		Utilities.initializeFolders();

		// Ask if using a new dataset or creating a new one
		if(Utilities.datasetChoice() == true){
			Utilities.createLargeDataset(10000,cellSize, gridInterval, dimension);
		}
		
		// Reads the dataset
		List<Point> points = Utilities.readDataset("dataset.txt", dimension);
		
		// Ask for algorithm choice
		linear = Utilities.algorithmChoice();
		
		//Ask for Epsilon value
		eps = Utilities.epsChoice();
		
		//Ask for minimum points value
		minPts = Utilities.minPtsChoice();
		
		
		/*--------------------------------
		 *|			LINEAR PART 	     |	
		 *-------------------------------- 		
		 */
		if (linear){
			System.out.println("Performing clustering");
			long startTime = System.currentTimeMillis();
			
			//Build Map of the neighbors
			Map<Point,List<Point>> neighbors = Utilities.buildNeighborsIndex(points, eps); 
			
			// Cluster the points
			Map<String, List<Point>> clusters = Utilities.UDBScanWithProcessing(points, eps, minPts,neighbors);
			System.out.println("Points clustered: output stored at "+System.getProperty("user.home")+"/Documents/bdmpFiles/output/");
			
			//Print clustering statistics
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
			JavaPairRDD<String,Iterable<Point>> expandedPartitions = partitionsGrouped.mapToPair(new PairFunction<Tuple2<String,Iterable<Point>>, 
					String, Iterable<Point>>() {

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
							if(!expanded.contains(p) && p.average()[0] >= splitvalue){ // Look only at the first dimension
								expanded.add(p);
							}
						}
					}
					return new Tuple2<String, Iterable<Point>>(t._1, expanded);
				}
			}).cache();	// Make this RDD persistent

			// Now apply a local UDBSCAN locally
			JavaRDD<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> local_clusters = expandedPartitions.flatMap(
					new FlatMapFunction<Tuple2<String,Iterable<Point>>, Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>>() {

				@Override
				public Iterator<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> call(Tuple2<String, Iterable<Point>> t) throws Exception {
					List<Point> points = new ArrayList<>(); // First convert the Iterable<Point> to a List<Point>
					Iterator<Point> it = t._2.iterator();
					while(it.hasNext()){
						points.add(it.next());
					}
					
					//Build Map of the neighbors
					Map<Point,List<Point>> neighbors = Utilities.buildNeighborsIndex(points, eps); 
					
					// Apply a local cluster
					Map<String,List<Point>> clusters = Utilities.UDBScan(points, eps, minPts,neighbors);
					clusters.keySet();

					// Create the list containing the clusters
					List<Tuple2<String,Iterable<Point>>> list1= new ArrayList<>();
					for(Map.Entry<String, List<Point>> s : clusters.entrySet()){
						list1.add(new Tuple2<String, Iterable<Point>>(s.getKey(), s.getValue()));
					}

					// Create list containing the partition id and the clusters
					List<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> list2 = new ArrayList();
					list2.add(new Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>(t._1, list1));

					return list2.iterator();
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
			//Map<String,String> duplicates = TestingUtilities.checkDuplicates(clusters);
			//TestingUtilities.checkDuplicatesSameSet(clusters);
			//System.out.println("Duplicates points: " + TestingUtilities.getPointDuplicates(clusters, "6", "10"));
			//System.out.println(duplicates.toString());
			
			// Remove clusters that have less points than minimum points
			Utilities.postProcess(clusters, minPts);
			
			//Print clustering statistics
			Utilities.printClusteringInfo(clusters);
			
			// Save clusters on the disk 
			try {
				Utilities.saveClusters(clusters);
			} catch (FileNotFoundException e) {
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
