package bdmp2.project2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Utilities {

	/*
	 * Stores alla possible grid locations in a List of strings
	 * @param cellSize - Dimension of the cell
	 * @param gridInterval - Maximum values representable in the grid
	 * @param dimension - Dimension of the space (2D,3D,4D)
	 */
	public static List<String> initializeGrid(int cellSize, int gridInterval, int dimension){
		List<String> cells = new ArrayList<String>();
		switch(dimension){

		case 2 : // 2D case
			for(int i=0; i<gridInterval; i=i+cellSize){
				for(int j=0;j<gridInterval;j=j+cellSize){
					String cell = i+"-"+(i+cellSize)+","+j+"-"+(j+cellSize);
					cells.add(cell);
				}
			}
			break;

		case 3 : // 3D case
			for(int i=0; i<gridInterval; i=i+cellSize){
				for(int j=0;j<gridInterval;j=j+cellSize){
					for(int z=0;z<gridInterval;z=z+cellSize){
						String cell = i+"-"+(i+cellSize)+","+j+"-"+(j+cellSize)+","+z+"-"+(z+cellSize);
						cells.add(cell);
					}
				}
			}
			break;

		case 4 : 
			for(int i=0; i<gridInterval; i=i+cellSize){
				for(int j=0;j<gridInterval;j=j+cellSize){
					for(int z=0;z<gridInterval;z=z+cellSize){
						for(int v=0;v<gridInterval;v=v+cellSize){
							String cell = i+"-"+(i+cellSize)+","+j+"-"+(j+cellSize)+","+z+"-"+(z+cellSize)+","+v+"-"+(v+cellSize);
							cells.add(cell);
						}
					}
				}
			}
			break;

		default :
			System.err.println("Insupported dimension, please choose a dimension between 2 and 4");
		}

		System.out.println(cells.size());
		Collections.sort(cells); //if is sorted is easier to assign probabilities to adjacent cells
		System.out.println(cells.toString());
		return cells;
	}
	 
	/*
	 * Returns a predefined data-set of uncertain points
	 */
	public static List<Point> createDataset(){
		List<Point> points = new ArrayList<Point>();
		
		int i = 0;
		Point p1 = new Point(i);
		p1.setCell("15-20,15-20", 0.5);
		p1.setCell("0-5,15-20", 0.2);
		p1.setCell("5-10,15-20", 0.2);
		p1.setCell("5-10,20-25", 0.1);
		i++;
		
		Point p2 = new Point(i);
		p2.setCell("20-25,20-25", 0.5);
		p2.setCell("20-25,15-20", 0.2);
		p2.setCell("15-20,15-20", 0.2);
		p2.setCell("15-20,20-25", 0.1);
		i++;
		
		Point p3 = new Point(i);
		p3.setCell("0-5,0-5", 0.5);
		p3.setCell("0-5,5-10", 0.2);
		p3.setCell("5-10,5-10", 0.2);
		p3.setCell("5-10,0-5", 0.1);
		i++;
		
		Point p4 = new Point(i);
		p4.setCell("5-10,0-5", 0.5);
		p4.setCell("20-25,5-10", 0.2);
		p4.setCell("15-20,5-10", 0.2);
		p4.setCell("15-20,0-5", 0.1);
		
		points.add(p1);
		points.add(p2);
		points.add(p3);
		points.add(p4);
		
		return points;
	}
	
	public static List<Point> createLargeDataset(int numPoints, int cellSize, int gridInterval, int dimension){
		List<Point> points = new ArrayList<Point>();
		List<Interval> ints = new ArrayList<Interval>();;
		int s1 = cellSize; //avoid points border
		int s2 = cellSize*2; //avoid pints border
		int index = 0;
		while(s1<gridInterval-cellSize){ //avoid points border
			Interval i = new Interval();
			i.x1 = s1;
			i.x2 = s2;
			ints.add(i);
			//possibleChoices[index] = s1+"-"+s2;
			s1+=cellSize;
			s2+=cellSize;
			index++;
		}
		for(int z=0; z<numPoints; z++){
			//Choose a starting point
			Interval[] center = new Interval[dimension];
			for(int i=0; i<dimension; i++){
				int centerIndex = 0 + (int)(Math.random() * ints.size()); 
				center[i] = ints.get(centerIndex);
			}
			
			Interval[] c1 = new Interval[dimension];
			for(int i=0; i<dimension; i++){
				c1[i] = new Interval(center[i].x1 + cellSize, center[i].x2 + cellSize);
			}
			
			Interval[] c2 = new Interval[dimension];
			for(int i=0; i<dimension; i++){
				c2[i] = new Interval(center[i].x1 - cellSize, center[i].x2 - cellSize);
			}
			
			Point p = new Point(z);
			StringBuilder sb = new StringBuilder();
			for(int i=0; i< center.length; i++){
				if(i!=center.length-1){
					sb.append(center[i].x1+"-"+center[i].x2+",");
				}else {
					sb.append(center[i].x1+"-"+center[i].x2);
				}
			}
			p.setCell(sb.toString(), 0.5);
			
			sb.setLength(0);
			for(int i=0; i< c1.length; i++){
				if(i!=c1.length-1){
					sb.append(c1[i].x1+"-"+c1[i].x2+",");
				} else {
					sb.append(c1[i].x1+"-"+c1[i].x2);
				}
			}
			p.setCell(sb.toString(), 0.3);
			
			sb.setLength(0);
			for(int i=0; i< c2.length; i++){
				if(i!=c2.length-1){
					sb.append(c2[i].x1+"-"+c2[i].x2+",");
				} else {
					sb.append(c2[i].x1+"-"+c2[i].x2);
				}
			}
			p.setCell(sb.toString(), 0.2);
			points.add(p);
			
		}
		return points;
	}
	
	/*
	 * Computes the density-based clustering using a modified version of DBScan that uses KL Divergence as measure of distance
	 * @param points - Data-set to cluster
	 * @param eps - maximum distance to consider a point as a neighbor
	 * @param minPts - minimum number of neighbors to start a cluster
	 */
	public static Map<String, List<Point>> UDBScan(List<Point> points, double eps, int minPts){
		Map<String, List<Point>> clusters = new HashMap<String, List<Point>>();
		List<Point> neighborPts = new ArrayList<Point>();
		int index=0;
		
		for(Point p : points){
			if(p.visited){
				continue;
			}
			p.visited = true;
			neighborPts = getNeighbors(p, eps, points);
			System.out.println("neighborPts size: "+neighborPts.size());
			if(neighborPts.size()<minPts){
				//Mark them as noise
			} else {
				clusters.put(""+index, expandCluster(p, neighborPts, points, eps, minPts));
				System.out.println("cluster size:" + clusters.size());
				index++;
			}
			
		}
		return clusters;
	}
	
	
	
	/*
	 * Returns p's neighbors within distance eps 
	 * @param p - Source point
	 * @param eps - maximum distance to consider a point as a neighbor
	 * @param Points - Data-set of points
	 */
	public static List<Point> getNeighbors(Point p, double EPS, List<Point> points){
		List<Point> neighborPts = new ArrayList<Point>();
		
		for(Point pp : points){
			if(p.equals(pp)){
				neighborPts.add(pp);
			}
			else if(KLDivergence(p, pp)>=EPS){
				neighborPts.add(pp);
			}
		}
		
		return neighborPts;
	}
	
	/*
	 * Builds a cluster after that from a point p are discovered #minPts neighbors within distance eps
	 * @param p - Source point
	 * @param neighbors - discovered neighbors of P where size(neighbors) >= minPts
	 * @param eps - maximum distance to consider a point as a neighbor
	 * @param minPts - minimum number of neighbors to start a cluster
	 * 
	 */
	public static List<Point> expandCluster(Point p, List<Point> neighborPts, List<Point> points, double eps, int minPts){
		int index = 0;
		while(neighborPts.size()>index){
			Point pp = neighborPts.get(index);
			if(!pp.visited){
				pp.visited=true;
				List<Point> neighborPts2 = getNeighbors(pp, eps, points);
				if(neighborPts2.size()>=minPts){
					merge(neighborPts,neighborPts2);
					// edit probabilities
				}
			}
			index++;
		}
		return neighborPts;
	}
	
	/*
	 * Computes the KL Divergence between two points modeled as random distributions.
	 * Since KL Divergence is not symmetric we take the sum of the distances between p1-p2 and p2-p1
	 * @param p1 - First random distribution
	 * @param p2 - Second random distribution
	 */
	public static double KLDivergence(Point p1, Point p2){
		double divergence1 = 0;
		for(Map.Entry<String, Double> entry : p1.getCells().entrySet()){
			if(p2.getCells().containsKey(entry.getKey())){
				double prob2 = p2.getCells().get(entry.getKey());
				divergence1 += entry.getValue()*Math.log(entry.getValue()/prob2);
			}
		}
		double divergence2 = 0;
		for(Map.Entry<String, Double> entry : p2.getCells().entrySet()){
			if(p1.getCells().containsKey(entry.getKey())){
				double prob1 = p1.getCells().get(entry.getKey());
				divergence2 += entry.getValue()*Math.log(entry.getValue()/prob1);
			}
		}
		return divergence1+divergence2;
	}
	
	/*
	 * Merges two List avoiding duplicates
	 * @param a - First list
	 * @param b - Second list
	 */
	public static List<Point> merge(List<Point> a, List<Point> b){
		Iterator<Point> it = b.iterator();
		while(it.hasNext()){
			Point p = it.next();
			if(!a.contains(p)){
				a.add(p);
			}
		}
		return a;
	}
}
