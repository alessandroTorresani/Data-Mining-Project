package bdmp2.project2;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.util.Pair;
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
	public static Map<String,List<Point>> mapClusters(List<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> localclusters){
		Map<String,List<Point>> map = new HashMap<String,List<Point>>();
		Iterable<Tuple2<String,Iterable<Point>>> first = localclusters.get(0)._2;
		Iterable<Tuple2<String,Iterable<Point>>> second = localclusters.get(1)._2;
		
		Iterator<Tuple2<String,Iterable<Point>>> it1 = first.iterator();
		Iterator<Tuple2<String,Iterable<Point>>> it2 = second.iterator();
		while(it1.hasNext()){
			Tuple2<String,Iterable<Point>> cluster = it1.next();
			List<Point> list = new ArrayList<>();
			Iterator<Point> itp = cluster._2.iterator();
			while(itp.hasNext()){
				list.add(itp.next());
			}
			map.put("0"+cluster._1, list);
		}
		while(it2.hasNext()){
			Tuple2<String,Iterable<Point>> cluster = it2.next();
			List<Point> list = new ArrayList<>();
			Iterator<Point> itp = cluster._2.iterator();
			while(itp.hasNext()){
				list.add(itp.next());
			}
			map.put("1"+cluster._1, list);
		}
		return map;
	}
	
	/*
	 * Creates a Map for each worker's output - Version 2
	 * @param localclusters - Output of local UDBSCAN performed by each worker
	 */
	public static List<Map<String,List<Point>>> mapClusters2(List<Tuple2<String,Iterable<Tuple2<String,Iterable<Point>>>>> localclusters){
		List<Map<String,List<Point>>> maps = new ArrayList<Map<String,List<Point>>>();
		Iterable<Tuple2<String,Iterable<Point>>> first = localclusters.get(0)._2;
		Iterable<Tuple2<String,Iterable<Point>>> second = localclusters.get(1)._2;
		
		Iterator<Tuple2<String,Iterable<Point>>> it1 = first.iterator();
		Iterator<Tuple2<String,Iterable<Point>>> it2 = second.iterator();
		
		Map<String,List<Point>> map1 = new HashMap<String,List<Point>>();
		Map<String,List<Point>> map2 = new HashMap<String,List<Point>>();
		
		int index = 0;
		while(it1.hasNext()){
			Tuple2<String,Iterable<Point>> cluster = it1.next();
			List<Point> list = new ArrayList<>();
			Iterator<Point> itp = cluster._2.iterator();
			while(itp.hasNext()){
				list.add(itp.next());
			}
			map1.put("cluster"+cluster._1, list);
			index++;
		}
		while(it2.hasNext()){
			Tuple2<String,Iterable<Point>> cluster = it2.next();
			List<Point> list = new ArrayList<>();
			Iterator<Point> itp = cluster._2.iterator();
			while(itp.hasNext()){
				list.add(itp.next());
			}
			map2.put("cluster"+cluster._1, list);
			index++;
		}
		maps.add(map1);
		maps.add(map2);
		
		//Utilities.printClusteringInfo(map1);
	
		//Utilities.printClusteringInfo(map2);
	
		return maps;
	}
	
//	public static void mergeClusters(Map<String,List<Point>> clusters){
//		for(Map.Entry<String, List<Point>> entry : clusters.entrySet()){
//			for(Point p : entry.getValue()){
//				if(p.clustered[0] && p.clustered[1]){
//					String key = findCluster(p, entry.getKey(), clusters);
//					for (Point p1 : clusters.get(key)){
//						if(!entry.getValue().contains(p1)){
//							entry.getValue().add(p1);
//						}
//					}
//					// reset clustered flag(so that no other point of the same cluster will be checked for merging
//					for (Point p2 : entry.getValue()){
//						p2.clustered[0] = false;
//						p2.clustered[1] = false;
//					}
//					clusters.remove(key);
//				}
//			}
//		}
//	}
	
	// I have to find clusters that have at least one point in common and merge them
	public static Map<String,List<Point>> mergeClustersTwo(List<Map<String,List<Point>>> maps) throws FileNotFoundException{
		Map<String,List<Point>> clusters = new HashMap<>();
		Map<String,List<Point>> map1 = maps.get(0);
		Map<String,List<Point>> map2 = maps.get(1);
		
		/*
		 * FIRST THING TO FIX: clusterid in the spark output is different from the id in the map1/map2 MAP.
		 * I need first to fix this problem so that I can check in the spark folder the shape of the different clusters
		 * 22/05/2017: PROBLEM FOUND. The order in which clusters are merged matter. Suppose the following order:
		 * Cluster 1 [map1] and cluster 0 [map2] are merged
		 * Cluster 0 [map1] and cluster 30[map2] are merged
		 * Cluster 0 [map1] and cluster 0[map2] are merged
		 * Cluster 0 [map1] clearly should be merged with cluster 0[map2] AND also cluster 1 [map1]. But SINCE cluster 0 - 30 was already created -> duplicates
		 */
		
		
		// TEST TEST TEST
//		for(Map.Entry<String, List<Point>> entry : map2.entrySet()){
//			for(Point p: entry.getValue()){
//				Map<String,List<Point>> target = getClustersToMerge(map1, p);
//				if(target.size()>1){
//					System.out.println(target.size());
//					//System.out.println("Point p:" + p.toString());
//					//System.out.println(target.toString());
//				}
//			}
//		}
//		
//		for(Map.Entry<String, List<Point>> entry : map2.entrySet()){
//			for(Point p: entry.getValue()){
//				Map<String,List<Point>> target = getClustersToMerge(map2, p);
//				if(target.size()>1){
//					System.out.println(target.size());
//					//System.out.println("Point p:" + p.toString());
//					//System.out.println(target.toString());
//				}
//			}
//		}
		// END TEST
		
		
		Set<String> entriesToDelete1 = new HashSet<String>();
		Set<String> entriesToDelete2 = new HashSet<String>();
		if (map1.size()<=0)
		{
			clusters.putAll(map2);
		} 
		else if(map2.size()<=0)
		{
			clusters.putAll(map1);
		} 
		else 
		{
			for(Map.Entry<String, List<Point>> entry : map1.entrySet()){
				for(Map.Entry<String, List<Point>> entry2 : map2.entrySet()){
					List<Point> tmp = new ArrayList<Point>(entry.getValue());
					tmp.retainAll(entry2.getValue()); // Get the intersection between entry and entry2
					if(tmp.size()>0){ // If the intersection is not empty
						System.out.println("Merging [map1][map2]:" + entry.getKey() + ", " + entry2.getKey());
						System.out.println(entry.getKey() +" has size: " + entry.getValue().size());
						System.out.println(entry2.getKey() +" has size: " + entry2.getValue().size());
						Utilities.savePoints(entry.getValue(), "map1-"+entry.getKey());
						Utilities.savePoints(entry2.getValue(), "map2-"+entry2.getKey());
						Utilities.savePoints(tmp, "map1-map2-"+entry.getKey()+"-"+entry2.getKey()+"tmp");
						if(entriesToDelete1.contains(entry.getKey())){ //If we already have merged this cluster in the past
							System.out.println(entry.getKey() +"[map1] already merged, updating previous value ");
							List<Point> tmp2 = new ArrayList<Point>(entry2.getValue()); 
							tmp2.removeAll(tmp); // Remove the elements in common from the second list
							String key = findCluster(clusters, entry.getValue().get(0));
							List<Point> previousValue = clusters.get(key);
							Utilities.savePoints(previousValue, "map1-"+entry.getKey()+"-previous");
							System.out.println("Previous list has size " + previousValue.size());
							previousValue.addAll(tmp2);
							Utilities.savePoints(previousValue, "map1-"+entry.getKey()+"-previous-after");
							System.out.println("Now has size " + previousValue.size());
							clusters.put(key, previousValue);
							entriesToDelete2.add(entry2.getKey());
						} else if (entriesToDelete2.contains(entry2.getKey())){
							System.out.println(entry2.getKey() +"[map2] already merged, updating previous value ");
							List<Point> tmp1 = new ArrayList<Point>(entry.getValue());
							tmp1.removeAll(tmp); // Remove the elements in common from the first list
							String key = findCluster(clusters, entry2.getValue().get(0));
							List<Point> previousValue = clusters.get(key);
							System.out.println("Previous list has size " + previousValue.size());
							Utilities.savePoints(previousValue, "map2-"+entry2.getKey()+"-previous");
							previousValue.addAll(tmp1);
							Utilities.savePoints(previousValue, "map2-"+entry2.getKey()+"-previous-after");
							System.out.println("Now has size " + previousValue.size());
							clusters.put(key, previousValue);
							entriesToDelete1.add(entry.getKey());
						} 
						else {
							System.out.println("Those keys are news, creating a new cluster");
							List<Point> tmp1 = new ArrayList<Point>(entry.getValue());
							List<Point> tmp2 = new ArrayList<Point>(entry2.getValue());
							tmp1.removeAll(tmp); // Remove the elements in common from the first list
							tmp2.removeAll(tmp); // Remove the elements in common from the second list
							tmp.addAll(tmp1); // Merge first list with the points in common
							tmp.addAll(tmp2); // Merge the second list with the points in common 
							System.out.println("of size: " + tmp.size());
							Utilities.savePoints(tmp, "map1-map2-"+entry.getKey()+"-"+entry2.getKey()+"-new-cluster");
							clusters.put((clusters.size()+1)+"",tmp);
							entriesToDelete1.add(entry.getKey());
							entriesToDelete2.add(entry2.getKey());
						}
						System.out.println("Duplicates: " + SparkUtilities.checkDuplicates(clusters).toString());
						if(entry.getKey().equals("cluster0") && entry2.getKey().equals("cluster21")){
							System.out.println("IFIFIFIFIFI");
							Utilities.savePoints(getPointDuplicates(clusters, "2", "3"), "cluster2-cluster3-duplicates");
						}
					}
				}
			}
		}
		
		System.out.println("After merging: " + SparkUtilities.checkDuplicates(clusters).toString());
		
		// Remove merged clusters
		for(String s : entriesToDelete1){
			
			map1.remove(s);
		}
		for(String s : entriesToDelete2){
			map2.remove(s);
		}
		
		//System.out.println("Entries1 to delete: " + entriesToDelete1.toString());
		//System.out.println("Entries2 to delete: " + entriesToDelete2.toString());
		
		// Add remaining clusters
		for(Map.Entry<String, List<Point>> entry : map1.entrySet()){
			clusters.put((clusters.size()+1)+"", entry.getValue());
		}
		System.out.println(SparkUtilities.checkDuplicates(clusters).toString());

		for(Map.Entry<String, List<Point>> entry : map2.entrySet()){
			clusters.put((clusters.size()+1)+"", entry.getValue());
		}
		System.out.println(SparkUtilities.checkDuplicates(clusters).toString());
		
		return clusters;
	}
	
	// TEST ETST
	public static Map<String,List<Point>> mergeClustersThree(List<Map<String,List<Point>>> maps){
		Map<String,List<Point>> clusters = new HashMap<>();
		Map<String,List<Point>> map1 = maps.get(0);
		Map<String,List<Point>> map2 = maps.get(1);
		
		Set<String> entriesToDelete1 = new HashSet<String>();
		Set<String> entriesToDelete2 = new HashSet<String>();
		
		for(Map.Entry<String, List<Point>> entry : map1.entrySet()){
			Map<String,List<Point>> merge = getClustersToMerge(entry.getValue(), map2,entry.getKey());
			System.out.println("merge size: " + merge.size());
			if(merge.size()>0){
				//Get the intersection
				List<Point> intersection = getIntersection(entry.getValue(), merge);
				
				// Remove the intersection from the clusters
				List<Point> entryCopy = new ArrayList<Point>(entry.getValue());
				entryCopy.removeAll(intersection);
				
				for(Map.Entry<String, List<Point>> entry2 : merge.entrySet()){
					entry2.getValue().removeAll(intersection);
					entriesToDelete2.add(entry2.getKey());
				}
				
				List<Point> result = new ArrayList<Point>();
				result.addAll(intersection);
				result.addAll(entryCopy);
				for(Map.Entry<String, List<Point>> entry2 : merge.entrySet()){
					intersection.addAll(entry2.getValue());
				}
				
				entriesToDelete1.add(entry.getKey());
				
				// SECOND STAGE
				Map<String, List<Point>> merge2 = getClustersToMerge(result, map1,entry.getKey());
				System.out.println("merge2 size: " + merge2.size());
				if(merge2.size()>0){
					List<Point> intersection2 = getIntersection(result, merge2);
					
					result.removeAll(intersection2);
					
					for(Map.Entry<String, List<Point>> entry2 : merge2.entrySet()){
						entry2.getValue().removeAll(intersection2);
						entriesToDelete1.add(entry2.getKey());
					}
					
					List<Point> result2 = new ArrayList<Point>();
					result2.addAll(intersection2);
			
					for(Map.Entry<String, List<Point>> entry2 : merge2.entrySet()){
						intersection.addAll(entry2.getValue());
					}
					result.addAll(result2);
				}
				
				
				clusters.put((clusters.size()+1)+"",result);
			}
		}
		
		// Remove merged clusters
		for(String s : entriesToDelete1){
			
			map1.remove(s);
		}
		for(String s : entriesToDelete2){
			map2.remove(s);
		}
		
		// Add remaining clusters
		for(Map.Entry<String, List<Point>> entry : map1.entrySet()){
			clusters.put((clusters.size()+1)+"", entry.getValue());
		}
		System.out.println(SparkUtilities.checkDuplicates(clusters).toString());

		for(Map.Entry<String, List<Point>> entry : map2.entrySet()){
			clusters.put((clusters.size()+1)+"", entry.getValue());
		}
		System.out.println(SparkUtilities.checkDuplicates(clusters).toString());
		
		return clusters;
		
	}
	
	public static Map<String,List<Point>> getClustersToMerge(List<Point> cluster, Map<String,List<Point>> dataset, String key){
		Map<String,List<Point>> target = new HashMap<>();
		for(Point p : cluster){
			for (Map.Entry<String, List<Point>> entry : dataset.entrySet()){
				if(entry.getValue().contains(p) && !entry.getKey().equals(key)){
					target.put(entry.getKey(), entry.getValue());
				}
			}
		}
		return target;
	}
	
	public static List<Point> getIntersection(List<Point> l1, Map<String,List<Point>> m2){
		List<Point> intersection = new ArrayList<Point>();
		for (Map.Entry<String, List<Point>> entry : m2.entrySet()){
			List<Point> list1 = new ArrayList<Point>(l1);
			list1.retainAll(entry.getValue());
			intersection.addAll(list1);
		}
		return l1;
	}
	
	//THIS ONE IS THE RIGHT ONE. 
	/*
	 * STEP 1: create a Map that associates to each point the partition id in MAP1 and MAP2
	 * STEP 2: create a Map that associates to every clusterId in MAP1, MAP2 a clustedId into a new MAP CLUSTERS.
	 * STEP 3: for each point in the map of STEP 1 copy the point into the right place in map CLUSTERS. If there is already a cluster put inside it,
	 * otherwise create a new one
	 */
	public static Map<String,List<Point>> mergeClustersFour(List<Map<String,List<Point>>> maps){
		Map<String,List<Point>> clusters = new HashMap<>();
		Map<String,List<Point>> map1 = maps.get(0);
		Map<String,List<Point>> map2 = maps.get(1);
		
		//STEP 1
		Map<Integer,Pair<String,String>> pointsPartitions = new HashMap<Integer,Pair<String,String>>();  
		for(Map.Entry<String, List<Point>> entry : map1.entrySet()){
			for (Point p : entry.getValue()){ 
				
				Pair<String,String> partitions = new Pair<String, String>(entry.getKey(), null);
				pointsPartitions.put(p.id, partitions);
			}
		}
		
		for(Map.Entry<String, List<Point>> entry : map2.entrySet()){
			for (Point p : entry.getValue()){
				Pair<String,String> partitions = pointsPartitions.get(p.id);
				if (partitions == null){
					partitions = new Pair<String,String>(null,entry.getKey());
					pointsPartitions.put(p.id, partitions);
				} else {
					Pair <String,String> partitionsNew = new Pair<String, String>(partitions.getFirst(), entry.getKey()); 
					pointsPartitions.put(p.id, partitionsNew);
				}
			}
		}
		System.out.println("Size: " + pointsPartitions.size());
		
		//STEP 2
		Map<String,String> clusterCorrespondance1 = new HashMap<String,String>();
		Map<String,String> clusterCorrespondance2 = new HashMap<String,String>();
		List<Integer> entriesToDelete = new ArrayList<Integer>();
		int index = 0;
		
		// First perform only points that should be merged
		for(Map.Entry<Integer, Pair<String,String>> entry : pointsPartitions.entrySet()){
			String firstID = entry.getValue().getFirst();
			String secondID = entry.getValue().getSecond();
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
					System.out.println("Point : " + entry.getKey() + ", FirstId = " + firstID +" ,SecondId: " + secondID + " "
							+ ",CorID1 : " + correspondingID1 +", corID2: " + correspondingID2);
					List<Point> previousValue = clusters.get(correspondingID1);
					previousValue.add(getPoint(entry.getKey(), firstID, map1));
					clusters.put(correspondingID1, previousValue);
					entriesToDelete.add(entry.getKey());
				}
			} else if (firstID == null && secondID == null){
				System.out.println("first and second are null");
			}
		}
		
		System.out.println("Entries to remove " + entriesToDelete.toString());
		// remove points already merged
		for(Integer i : entriesToDelete){
			pointsPartitions.remove(i);
		}
		
		
		// Second Stage, only points marked by one worker
		for(Map.Entry<Integer, Pair<String,String>> entry : pointsPartitions.entrySet()){
			String firstID = entry.getValue().getFirst();
			String secondID = entry.getValue().getSecond();
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
				System.out.println("IF che non dovrebbe verificarsi");
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
					System.out.println("Point : " + entry.getKey() + ", FirstId = " + firstID +" ,SecondId: " + secondID + " "
							+ ",CorID1 : " + correspondingID1 +", corID2: " + correspondingID2);
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
	
	public static Point getPoint(Integer id, String clusterId, Map<String,List<Point>> map){
		for(Point p : map.get(clusterId)){
			if (p.id == id)
				return p;
		}
		System.out.println("Return null");
		return null;
	}
	
	// END TEST END TEST
	
	
	
	public static String findCluster(Map<String,List<Point>> clusters,Point p){
		String targetKey = null;
		for (Map.Entry<String, List<Point>> entry: clusters.entrySet()){
			if(entry.getValue().contains(p)){
				targetKey = entry.getKey();
			}
		}
		return targetKey;
	}
	
	public static Map<String,String> checkDuplicates(Map<String,List<Point>> clusters){
		Map<String,String> target = new HashMap<>();
		for(Map.Entry<String, List<Point>> entry: clusters.entrySet()){
			for(Point p: entry.getValue()){
				for(Map.Entry<String, List<Point>> entry2: clusters.entrySet()){
					if(entry2.getKey() != entry.getKey()){
						if(entry2.getValue().contains(p)){
							target.put(entry.getKey(), entry2.getKey());
						}
					}
				}
			}
		}
		return target;
	}
	
	public static Map<String,String> checkDuplicates2(Map<String,List<Point>> clusters){
		Map<String,String> target = new HashMap<>();
		for(Map.Entry<String, List<Point>> entry: clusters.entrySet()){
			for(Point p: entry.getValue()){
				for(Map.Entry<String, List<Point>> entry2: clusters.entrySet()){
					if(entry2.getKey() != entry.getKey()){
						if(entry2.getValue().contains(p)){
							target.put(entry.getKey(), entry2.getKey());
							entry2.getValue().remove(p);
						}
					}
				}
			}
		}
		return target;
	}
	
	public static List<Point>getPointDuplicates(Map<String,List<Point>> clusters, String target1, String target2){
		List<Point> pointsIncommon = new ArrayList<>();
		for(Point p : clusters.get(target1)){
			for (Point p2 : clusters.get(target2)){
				if (p.equals(p2)){
					pointsIncommon.add(p);
				}
			}
		}
		return pointsIncommon;
	}
	
	public static void checkDuplicatesSameSet(Map<String,List<Point>> clusters){
		for(Map.Entry<String, List<Point>> entry: clusters.entrySet()){
			for(Point p : entry.getValue()){
				int frequency = Collections.frequency(entry.getValue(), p); 
				if (frequency > 1){
					System.out.println("Point " + p + " found " + frequency +" in Cluster with id: " + entry.getKey());
				}
			}
		}
	}
}

	
