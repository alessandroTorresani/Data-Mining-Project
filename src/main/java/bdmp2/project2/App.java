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
    	final double EPS = 0.7;
    	final int MINPTS = 8;
    	final int cellSize = 2;
    	final int gridInterval = 100;
    	final int dimension = 3;
    	final double splitvalue = 50.0;
    	// Create and clean folders used for execution
    	Utilities.initializeFolders();
    	
    	// Sample a new dataset
    	Utilities.createLargeDataset(30000,cellSize, gridInterval, dimension);
    	
    	/*--------------------------------
    	 *|			PARALLEL PART 	     |	
    	 *-------------------------------- 		
    	 */
    	
    	// Spark configuration
    	SparkConf conf = new SparkConf();
		conf.setAppName("MrClustering").setMaster("local[2]"); // 2 local workers
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Read the data-set of point and 
		List<Point> points = Utilities.readDataset("dataset.txt", dimension);
		Broadcast<List<Point>> broadcastDataset = sc.broadcast(points);
		
		// Convert it into a JavaRDD object
		JavaRDD<Point> pointsr = sc.parallelize(points);
		JavaPairRDD<String, Point> partitions = MrUtilities.createPartitions(pointsr,splitvalue);
		
		/*
		 * obtain points of the 2 partitions. Only for testing purpose. Uncomment if you want to examine them
		 */
		//JavaPairRDD<String,Point> first_partion = partitions.filter(s -> s._1.equals('0'));
		//JavaPairRDD<String,Point> second_partion = partitions.filter(s -> s._1.equals('1'));
		
		// Expand partitions
		JavaPairRDD<String,Iterable<Point>> partitionsGrouped = partitions.groupByKey();
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
						if(!expanded.contains(p) && p.average()[0] <= splitvalue + EPS){ // Look only at the first dimension
							expanded.add(p);
						}
					}
				} else if(t._1().equals("1")){
					Iterator it = t._2.iterator();
					while(it.hasNext()){
						expanded.add((Point) it.next());
					}
					for(Point p : broadcastDataset.value()){
						if(!expanded.contains(p) && p.average()[0] >= splitvalue - EPS){ // Look only at the first dimension
							expanded.add(p);
						}
					}
				}
				return new Tuple2<String, Iterable<Point>>(t._1, expanded);
			}
		});
		
		// Now apply a local UDBSCAN locally
		JavaRDD<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> local_clusters = expandedPartitions.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Point>>, Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>>() {

			@Override
			public Iterator<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> call(Tuple2<String, Iterable<Point>> t) throws Exception {
				List<Point> points = new ArrayList<>(); // First convert the Iterable<Point> to a List<Point>
				Iterator it = t._2.iterator();
				while(it.hasNext()){
					points.add((Point) it.next());
				}
				// Apply a local cluster
				Map<String,List<Point>> clusters = Utilities.UDBScan(points, EPS, MINPTS);
				clusters.keySet();
				
				/*
				 * Problem now. UDBSScan returns a map. But the output of the function call should return an iterator. So we have to build a list from the map.
				 * But since we may have different clusters, our list should contain pairs <cluster_id,points>.
				 */
//						List<Point> a = new ArrayList();
//						for(Map.Entry<String, List<Point>> s : clusters.entrySet()){
//							a = s.getValue();
//						}
				
				// Mark clustered points with partition id
				for(Map.Entry<String, List<Point>> s : clusters.entrySet()){
					for(Point p : s.getValue()){
						p.clustered[Integer.parseInt(t._1)] = true;
					}
				}
				
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
		
		// Merge local clusters. Clusters, whose Points have been marked by both partition, should be merged together.
		
		// Save output to the disk
		local_clusters.saveAsTextFile(System.getProperty("user.home")+"/Documents/bdmpFiles/output/spark");
		
		
    	
		/*--------------------------------
    	 *|			LINEAR PART 	     |	
    	 *-------------------------------- 		
    	 */
		/*
    	//List<Point> points = Utilities.readDataset("dataset.txt", dimension);

    	//System.out.println(points.toString());

    	//System.out.println("Points: " );
    	//System.out.println(points.toString());
    	
    	Map<String, List<Point>> clusters = Utilities.UDBScan(points, EPS, MINPTS);
    	System.out.println("Points clustered: output stored at "+System.getProperty("user.home")+"/Documents/bdmpFiles/output/");

    	int size = clusters.keySet().size();
    	System.out.println("Number of clusters: "+size);

    	// Get the total number of points clustered
    	int length = 0;
    	for(Map.Entry<String, List<Point>> entry : clusters.entrySet()){
    		length += entry.getValue().size();
    	}
    	System.out.println("Number of elements clustered: "+length);

        //System.out.println(clusters.toString());
        try {
        	Utilities.saveClusters(clusters);
        } catch (FileNotFoundException e) {
			// TODO: handle exception
        	System.err.println(e.getMessage());
		}
        
        /* COST OF DBSCAN: O(n*m) where m is the cost of the neighbor query. So without indexing the cost of m is O(n) and thus the global cost is O(n^2)
         * IDEAS for parallel part
         * SIMPLE ONE: do only the "find neighbors part" in parallel. For example divide the data-set in 4 parts and assign each part to a cluster. The problem is
         * how to divide the data-set in a way that every cluster can compute its part independently?
         * I think that we have to implement for sure an indexing structure because we have to work on spatial areas. R-Tree can be a solution.
         * HARD ONE: do the clustering in parallel. Do a local cluster in each area and at the end merge the results. Since the performance of this solution 
         * depends on the smallest thread, an algorithm for dividing the work evenly is needed
         */
        
        
    }
    
    //dirty
}
