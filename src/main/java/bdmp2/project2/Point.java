package bdmp2.project2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hdfs.server.namenode.HostFileManager.EntrySet;
import org.netlib.util.doubleW;

public class Point implements Serializable {
	int id;
	int dimension;
	Map<String,Double> cells;
	boolean visited;
	boolean[] clustered; // Used to check if a point has been clustered by different workers

	public Point(int id, int dimension){
		cells = new HashMap<String, Double>();
		this.id = id;
		this.dimension = dimension;
		this.visited = false;
		clustered = new boolean[4]; // CHANGE IT IF YOU HAVE MORE THAN 4 WORKERS
	}
	
	public Map<String, Double> getCells() {
		return cells;
	}
	
	public void setCells(Map<String, Double> cells) {
		this.cells = cells;
	}
	
	public void setCell(String interval, double probability){
		cells.put(interval, probability);
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, Double> entry : cells.entrySet()){
			sb.append(id + "," + entry.getKey() + "," + entry.getValue() + "\n");
		}
		return sb.toString();
	}
	
	
	@Override
	public boolean equals(Object o){
		if (o == null) return false;
		if (!(o instanceof Point)) return false;
		return (id == ((Point) o).id);
	}
	
	//Return the average(random variable) of the point
	public double[] average(){
		double[] average = new double[this.dimension];
		for (Map.Entry<String, Double> s : cells.entrySet()){
			Interval[] intervals = Utilities.parseLocation(s.getKey());
			for(int i=0; i<dimension; i++){
				int x1 = intervals[i].x1;
				int x2 = intervals[i].x2;
				average[i] = average[i] + (((x1+x2)/2.0)*s.getValue());
			}
		}
		return average;
	}
	
}
