package bdmp2.project2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestingUtilities {

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
