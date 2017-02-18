package bdmp2.project2;

import java.util.HashMap;
import java.util.Map;

public class Point {
	int id;
	Map<String,Double> cells;
	boolean visited;

	public Point(int id){
		cells = new HashMap<String, Double>();
		this.id = id;
		this.visited = false;
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
			sb.append("[ ID = " + id + ", " + entry.getKey() + ", " + entry.getValue() + "]\n");
		}
		return sb.toString();
	}
	
	@Override
	public boolean equals(Object o){
		if (o == null) return false;
		if (!(o instanceof Point)) return false;
		return (id == ((Point) o).id);
	}
	
}
