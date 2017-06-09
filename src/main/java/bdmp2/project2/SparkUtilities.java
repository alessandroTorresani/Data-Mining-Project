package bdmp2.project2;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkUtilities {
	/*
	 * Assign each point to a partition. Decision is taken looking at point's position.
	 * @param dataset - List of all points
	 * @param interval - Split location where a partition ends and the other one begins
	 */
	public static JavaPairRDD<String, Point> createPartitions(JavaRDD<Point> dataset, final double splitInterval){
		JavaPairRDD<String, Point> pair1 = dataset.mapToPair(new PairFunction<Point, String, Point>() {

			public Tuple2<String, Point> call(Point t) throws Exception {
				if (t.average()[0] < splitInterval){
					return new Tuple2<String,Point>("0",t);
				} else {
					return new Tuple2<String, Point>("1", t);
				}
			}
			
		});
		return pair1;
	}
	
	/*
	 * Creates a Map for each worker's output
	 * @param localclusters - Output of local UDBSCAN performed by each worker
	 */
	public static List<Map<String,List<Point>>> mapClusters(List<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> localclusters){
		List<Map<String,List<Point>>> maps = new ArrayList<Map<String,List<Point>>>();
		Iterable<Tuple2<String,Iterable<Point>>> first = localclusters.get(0)._2;
		Iterable<Tuple2<String,Iterable<Point>>> second = localclusters.get(1)._2;
		
		Iterator<Tuple2<String,Iterable<Point>>> it1 = first.iterator();
		Iterator<Tuple2<String,Iterable<Point>>> it2 = second.iterator();
		
		Map<String,List<Point>> map1 = new HashMap<String,List<Point>>();
		Map<String,List<Point>> map2 = new HashMap<String,List<Point>>();
		
		while(it1.hasNext()){
			Tuple2<String,Iterable<Point>> cluster = it1.next();
			List<Point> list = new ArrayList<>();
			Iterator<Point> itp = cluster._2.iterator();
			while(itp.hasNext()){
				list.add(itp.next());
			}
			map1.put("cluster"+cluster._1, list);
			
		}
		while(it2.hasNext()){
			Tuple2<String,Iterable<Point>> cluster = it2.next();
			List<Point> list = new ArrayList<>();
			Iterator<Point> itp = cluster._2.iterator();
			while(itp.hasNext()){
				list.add(itp.next());
			}
			map2.put("cluster"+cluster._1, list);
			
		}
		maps.add(map1);
		maps.add(map2);
	
		return maps;
	}
	
	/*
	 * Given the output of each worker, it merges the clusters that have border points in common
	 * @param maps -  Output of each worker of the shape <worker_id,<Cluster_id,[points]>>  
	 */
	public static Map<String,List<Point>> mergeClusters(List<Map<String,List<Point>>> maps){
		Map<String,List<Point>> clusters = new HashMap<>();
		Map<String,List<Point>> map1 = maps.get(0);
		Map<String,List<Point>> map2 = maps.get(1);
		
		//Create a map that associates to every point the clusters in which it is contained
		Map<Integer,Map<String,String>> pointsPartitions = new HashMap<Integer,Map<String,String>>();
		
		for(Map.Entry<String, List<Point>> entry : map1.entrySet()){
			for (Point p : entry.getValue()){ 
				Map<String,String> partitions2 = new HashMap<String,String>();
				partitions2.put("1", entry.getKey());
				partitions2.put("2", null);
				pointsPartitions.put(p.id, partitions2);
			}
		}
		
		for(Map.Entry<String, List<Point>> entry : map2.entrySet()){
			for (Point p : entry.getValue()){
				//Pair<String,String> partitions = pointsPartitions.get(p.id);
				Map<String,String> partitions = pointsPartitions.get(p.id);
				if (partitions == null){
					partitions = new HashMap<String,String>();
					partitions.put("2", entry.getKey());
					pointsPartitions.put(p.id, partitions);
				} else {
					partitions.put("2", entry.getKey());
					pointsPartitions.put(p.id, partitions);
				}
			}
		}
		
		//First create clusters starting from the border points
		Map<String,String> clusterCorrespondance1 = new HashMap<String,String>();
		Map<String,String> clusterCorrespondance2 = new HashMap<String,String>();
		List<Integer> entriesToDelete = new ArrayList<Integer>();
		int index = 0;
		for(Map.Entry<Integer, Map<String,String>> entry : pointsPartitions.entrySet()){
			String firstID = entry.getValue().get("1");
			String secondID = entry.getValue().get("2");
			if (firstID != null && secondID != null){
				String correspondingID1 = clusterCorrespondance1.get(firstID);
				String correspondingID2 = clusterCorrespondance2.get(secondID);
				if(correspondingID1 != null && correspondingID2 == null){
					List<Point> previousValue = clusters.get(correspondingID1);
					previousValue.add(getPoint(entry.getKey(), firstID, map1));
					clusterCorrespondance2.put(secondID, correspondingID1);
					clusters.put(correspondingID1, previousValue);
					entriesToDelete.add(entry.getKey());
				} else if (correspondingID1 == null && correspondingID2 != null){
					List<Point> previousValue = clusters.get(correspondingID2);
					previousValue.add(getPoint(entry.getKey(), secondID, map2));
					clusterCorrespondance1.put(firstID, correspondingID2);
					clusters.put(correspondingID2, previousValue);
					entriesToDelete.add(entry.getKey());
				} else if (correspondingID1 == null && correspondingID2 == null){
					List<Point> cluster = new ArrayList<Point>();
					cluster.add(getPoint(entry.getKey(), firstID, map1));
					clusters.put(""+index, cluster);
					clusterCorrespondance1.put(firstID, ""+index);
					clusterCorrespondance2.put(secondID, ""+index);
					entriesToDelete.add(entry.getKey());
					index++;
				} else if (correspondingID1 != null && correspondingID2 != null && correspondingID1.equals(correspondingID2)){
					List<Point> previousValue = clusters.get(correspondingID1);
					previousValue.add(getPoint(entry.getKey(), firstID, map1));
					clusters.put(correspondingID1, previousValue);
					entriesToDelete.add(entry.getKey());
				} else if(correspondingID1 != null && correspondingID2 != null && !correspondingID1.equals(correspondingID2)){
					//First I should merge the two different cluster identified by correspondingID1 and correspondingID2
					combineClusters(clusters, correspondingID1, correspondingID2);
					index--;
					
					//Then I should edit the entries in clusterCorrespondance1
					for(Map.Entry<String,String> correspondace : clusterCorrespondance1.entrySet()){
						if(correspondace.getValue().equals(correspondingID2)){
							clusterCorrespondance1.put(correspondace.getKey(),correspondingID1);
						}
					}
					
					//And the entries in clusterCorrespondance2
					for(Map.Entry<String,String> correspondace : clusterCorrespondance2.entrySet()){
						if(correspondace.getValue().equals(correspondingID2)){
							clusterCorrespondance2.put(correspondace.getKey(),correspondingID1);
						}
					}
					entriesToDelete.add(entry.getKey());
				}
			} else if (firstID == null && secondID == null){
				System.out.println("first and second are null");
			}
		}

		// remove points already merged
		for(Integer i : entriesToDelete){
			pointsPartitions.remove(i);
		}
		
		/*
		 *  Than assign the remaining point to the clusters according to the information contained in clusterCorrespondance1 and
		 *  clusterCorrespondance2
		 */
		for(Map.Entry<Integer, Map<String,String>> entry : pointsPartitions.entrySet()){
			String firstID = entry.getValue().get("1");
			String secondID = entry.getValue().get("2");
			if(firstID != null && secondID == null){
				String correspondingID = clusterCorrespondance1.get(firstID);
				if(correspondingID == null ){
					List<Point> cluster = new ArrayList<Point>();
					cluster.add(getPoint(entry.getKey(), firstID, map1)); //Add point
					clusters.put(""+index, cluster);
					clusterCorrespondance1.put(firstID, ""+index);
					index++;
				} else {
					List<Point> previousValue = clusters.get(correspondingID);
					previousValue.add(getPoint(entry.getKey(), firstID, map1));
					clusters.put(correspondingID, previousValue);
				}
			} else if (firstID == null && secondID != null){
				String correspondingID = clusterCorrespondance2.get(secondID);
				if(correspondingID == null ){
					List<Point> cluster = new ArrayList<Point>();
					cluster.add(getPoint(entry.getKey(), secondID, map2)); //Add point
					clusters.put(""+index, cluster);
					clusterCorrespondance2.put(secondID, ""+index);
					index++;
				}else {
					List<Point> previousValue = clusters.get(correspondingID);
					previousValue.add(getPoint(entry.getKey(), secondID, map2));
					clusters.put(correspondingID, previousValue);
				}
			} else if (firstID != null && secondID != null){
				System.out.println("IF that should not happen; FirstID: " + firstID + ", SecondID: " + secondID);
				String correspondingID1 = clusterCorrespondance1.get(firstID);
				String correspondingID2 = clusterCorrespondance2.get(secondID);
				if(correspondingID1 != null && correspondingID2 == null){
					List<Point> previousValue = clusters.get(correspondingID1);
					previousValue.add(getPoint(entry.getKey(), firstID, map1));
					previousValue.add(getPoint(entry.getKey(), secondID, map2));
					clusterCorrespondance2.put(secondID, correspondingID1);
					clusters.put(correspondingID1, previousValue);
				} else if (correspondingID1 == null && correspondingID2 != null){
					List<Point> previousValue = clusters.get(correspondingID2);
					previousValue.add(getPoint(entry.getKey(), firstID, map1));
					previousValue.add(getPoint(entry.getKey(), secondID, map2));
					clusterCorrespondance1.put(firstID, correspondingID2);
					clusters.put(correspondingID2, previousValue);

				} else if (correspondingID1 == null && correspondingID2 == null){
					List<Point> cluster = new ArrayList<Point>();
					cluster.add(getPoint(entry.getKey(), firstID, map1));
					cluster.add(getPoint(entry.getKey(), secondID, map2));
					clusters.put(""+index, cluster);
					clusterCorrespondance1.put(firstID, ""+index);
					clusterCorrespondance2.put(secondID, ""+index);
					index++;
				} else if (correspondingID1 != null && correspondingID2 != null){
//					System.out.println("Point : " + entry.getKey() + ", FirstId = " + firstID +" ,SecondId: " + secondID + " "
//							+ ",CorID1 : " + correspondingID1 +", corID2: " + correspondingID2);
					List<Point> previousValue = clusters.get(correspondingID1);
					previousValue.add(getPoint(entry.getKey(), firstID, map1));
					previousValue.add(getPoint(entry.getKey(), secondID, map2));
					clusters.put(correspondingID1, previousValue);
				}
			} else if (firstID == null && secondID == null){
				System.out.println("first and second are null");
			}
		}

		return clusters;
	}
	
	/* 
	 * Given an id, it returns the point with that id contained in a specific cluster
	 * @param id - target point id
	 * @param clusterId - Cluster in which the point can be found
	 * @param clusters - map containing the clusters
	 */
	public static Point getPoint(Integer id, String clusterId, Map<String,List<Point>> clusters){
		for(Point p : clusters.get(clusterId)){
			if (p.id == id)
				return p;
		}
		System.out.println("Return null");
		return null;
	}
	
	/*
	 * Given a map, it finds two clusters and it merges them
	 * @param clusters - map of clusters
	 * @param cluster1Id - id of the first cluster to merge
	 * @param cluster2Id - id of the second cluster to merge
	 */
	public static void combineClusters(Map<String,List<Point>> clusters, String cluster1Id, String cluster2Id){
		List<Point> cluster1 = clusters.get(cluster1Id);
		List<Point> cluster2 = clusters.get(cluster2Id);
		
		//Assuming that cluster1 doesn't intersect with cluster2
		for(Point p : cluster2){
			cluster1.add(p);
		}
		
		clusters.remove(cluster2Id);
	}
	
	
	
	
}

	
