package bdmp2.project2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;


public class Utilities {

	/*
	 * Sample a Data-set of point distributed at random 
	 * @param numPoints: number of uncertain points
	 * @param cellSize: unit length of the each cell
	 * @param gridInterval: maximum value representable by our space
	 * @param dimension: choose the dimension of the space (2D,3D,4D,...)
	 */ 
	public static void createLargeDataset(int numPoints, int cellSize, int gridInterval, int dimension) throws FileNotFoundException{
		List<Point> points = new ArrayList<Point>();
		List<Interval> intervals = new ArrayList<Interval>();
		
		int s1 = cellSize; //avoid points on the border
		int s2 = cellSize*2; //avoid pints on the border

		while(s1<gridInterval-cellSize){ //avoid points on the border
			Interval i = new Interval();
			i.x1 = s1;
			i.x2 = s2;
			intervals.add(i);
			s1+=cellSize;
			s2+=cellSize;
		}

		for(int z=0; z<numPoints; z++){
			//Choose a starting point at random
			Interval[] center = new Interval[dimension];

			for(int i=0; i<dimension; i++){
				int centerIndex = 0 + (int)(Math.random() * intervals.size()); 
				center[i] = intervals.get(centerIndex);
			}

			/*
			 * 1) increment only column
			 * 2) increment column and row
			 * 3) increment only row
			 * 4) increment row and decrease column
			 * 5) decrease only column
			 * 6) decrease column and row
			 * 7) decrease only row
			 * 8) increase column and decrease row
			 */
			
			double[] probabilities = getRandomProbabilities(9);
			int index = 0;
			Point p = new Point(z,dimension);
			p.setCell(buildCell(center), probabilities[index]);
			Interval[] cell;
			while(index<9){
				index++;
				switch(index){
				case 1 : cell = new Interval[dimension];
				for(int i = 0; i < dimension; i++){
					if(i==1){
						cell[i] = new Interval(center[i].x1 + cellSize, center[i].x2+cellSize);
					} else {
						cell[i] = new Interval(center[i].x1 , center[i].x2);
					}
				}
				p.setCell(buildCell(cell), probabilities[index]);
				break;
				case 2 : cell = new Interval[dimension];
				for(int i = 0; i < dimension; i++){
					if(i<2){
						cell[i] = new Interval(center[i].x1 + cellSize, center[i].x2 + cellSize);
					} else {
						cell[i] = new Interval(center[i].x1 , center[i].x2);
					}
				}
				p.setCell(buildCell(cell), probabilities[index]);
				break;
				case 3 : cell = new Interval[dimension];
				for(int i = 0; i < dimension; i++){
					if(i==0){
						cell[i] = new Interval(center[i].x1 + cellSize, center[i].x2 + cellSize);
					} else {
						cell[i] = new Interval(center[i].x1 , center[i].x2);
					}
				}
				p.setCell(buildCell(cell), probabilities[index]);
				break;
				case 4 : cell = new Interval[dimension];
				for(int i = 0; i < dimension; i++){
					if(i==0){
						cell[i] = new Interval(center[i].x1 + cellSize, center[i].x2 + cellSize);
					} else if (i==1) {
						cell[i] = new Interval(center[i].x1 - cellSize, center[i].x2 - cellSize);
					} else {
						cell[i] = new Interval(center[i].x1 , center[i].x2);
					}
				}
				p.setCell(buildCell(cell), probabilities[index]);
				break;
				case 5: cell = new Interval[dimension];
				for(int i = 0; i < dimension; i++){
					if(i==1){
						cell[i] = new Interval(center[i].x1 - cellSize, center[i].x2 - cellSize);
					} else {
						cell[i] = new Interval(center[i].x1 , center[i].x2);
					}
				}
				p.setCell(buildCell(cell), probabilities[index]);
				break;
				case 6 : cell = new Interval[dimension];
				for(int i = 0; i < dimension; i++){
					if(i<2){
						cell[i] = new Interval(center[i].x1 - cellSize, center[i].x2 - cellSize);
					} else {
						cell[i] = new Interval(center[i].x1, center[i].x2);
					}
				}
				p.setCell(buildCell(cell), probabilities[index]);
				break;
				case 7 : cell = new Interval[dimension];
				for(int i = 0; i < dimension; i++){
					if(i==0){
						cell[i] = new Interval(center[i].x1 - cellSize, center[i].x2 - cellSize);
					} else {
						cell[i] = new Interval(center[i].x1 , center[i].x2);
					}
				}
				p.setCell(buildCell(cell), probabilities[index]);
				break;
				case 8: cell = new Interval[dimension];
				for(int i = 0; i < dimension; i++){
					if(i==0){
						cell[i] = new Interval(center[i].x1 - cellSize, center[i].x2 - cellSize);
					} else if (i==1) {
						cell[i] = new Interval(center[i].x1 + cellSize, center[i].x2 + cellSize);
					} else {
						cell[i] = new Interval(center[i].x1 , center[i].x2);
					}
				}
				p.setCell(buildCell(cell), probabilities[index]);
				break;
				}
			}
			points.add(p);
			
			
		}
		StringBuilder sb = new StringBuilder();
		for(Point p : points){
			sb.append(p.toString());
		}
		
		PrintWriter pw = new PrintWriter(new File(System.getProperty("user.home")+"/Documents/bdmpFiles/input/"+"dataset.txt"));
		pw.write(sb.toString());
		pw.close();

	}
	
	/*
	 * Reads a data-set stored on the file system and returns it as a list of points
	 * @param filename - Name of the file containing the data-set
	 * @param dimension - Dimension of the space (2D,3D,4D)
	 */
	public static List<Point> readDataset(String filename, int dimension){
		List<Point> points = new ArrayList<Point>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(System.getProperty("user.home")+"/Documents/bdmpFiles/input/"+filename));
			String line="";
			int counter = 0;
			Point p = new Point(0,dimension);
			while((line = br.readLine()) != null){
				String[] parts = line.split(",");
				if (parts.length > 0){
					if(counter == 9){
						points.add(p);
						p = new Point(Integer.parseInt(parts[0]),dimension);
						counter = 0;
					}
					if(dimension == 2){
						p.setCell(parts[1]+","+parts[2], Double.parseDouble(parts[3]));
					} else if (dimension == 3){
						p.setCell(parts[1]+","+parts[2]+","+parts[3], Double.parseDouble(parts[4]));
					} else if (dimension == 4){
						p.setCell(parts[1]+","+parts[2]+","+parts[3]+","+parts[4], Double.parseDouble(parts[5]));
					}	
				}
				counter++;
			}
			points.add(p); // add the last point
		} catch(Exception ex){
			ex.printStackTrace();
		} finally {
			try
            {
                br.close();
                
            }
            catch(IOException ie)
            {
                System.out.println("Error occured while closing the BufferedReader");
                ie.printStackTrace();
            }
		}
		return points;
	}
	
	/*
	 * Indexes the structure of the dataset. To every point it stores its eps-neighbors
	 * @param points - Data-set to index
	 * @param eps - maximum distance to consider a point a neighbor
	 */
	public static Map<Point,List<Point>> buildIndex(List<Point> points, double eps){
		Map<Point,List<Point>> pointsIndex = new HashMap<Point, List<Point>>(); 
		List<Point> neighborPts = new ArrayList<Point>();
		for (Point p : points){
			neighborPts = getNeighbors(p, eps, points);
			pointsIndex.put(p, neighborPts);
		}
		return pointsIndex;
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
			if(neighborPts.size()<minPts){
				//Mark them as noise
			} else {
				p.clustered = true;
				clusters.put(""+index, createAndExpandCluster(p, neighborPts, points, eps, minPts));
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
			else if(probabilisticDistance(p, pp)<=EPS){
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
	public static List<Point> createAndExpandCluster(Point p, List<Point> neighborPts, List<Point> points, double eps, int minPts){
		int index = 0;
		List<Point> cluster = new ArrayList<>();
		cluster.add(p);
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
			if(!pp.clustered){
				pp.clustered = true;
				cluster.add(pp);
			}
			index++;
		}
		return cluster;
	}
	
	
	/*
	 * Computes the KL Divergence between two points modeled as random distributions.
	 * Since KL Divergence is not symmetric we take the sum of the distances between p1-p2 and p2-p1
	 * @param p1 - First random distribution
	 * @param p2 - Second random distribution
	 */
	public static double KLDivergence(Point p1, Point p2){
		double divergence1 = 0;
		for(Map.Entry<String, Double> entry1 : p1.getCells().entrySet()){
			if(p2.getCells().containsKey(entry1.getKey())){
				double prob2 = p2.getCells().get(entry1.getKey());
				divergence1 += entry1.getValue()*Math.log(entry1.getValue()/prob2);
			} 
		}
		double divergence2 = 0;
		for(Map.Entry<String, Double> entry2 : p2.getCells().entrySet()){
			if(p1.getCells().containsKey(entry2.getKey())){
				double prob1 = p1.getCells().get(entry2.getKey());
				divergence2 += entry2.getValue()*Math.log(entry2.getValue()/prob1);
			}
		}
		return divergence1+divergence2;
	}
	
	/*
	 * Computes the similarity between the random distributions of two points
	 * @param p1 - First point
	 * @param p2 - Second point
	 */
	public static double probabilisticDistance(Point p1, Point p2){
		
		Set<String> usedKeys = new HashSet<String>();
		double distance1 = 0;
		for(Map.Entry<String, Double> entry : p1.getCells().entrySet()){
			if(p2.getCells().containsKey(entry.getKey())){
				double prob2 = p1.getCells().get(entry.getKey());
				distance1 += Math.abs(prob2 - entry.getValue());
				usedKeys.add(entry.getKey());
			} else {
				distance1 += entry.getValue();
			}
		}
		double distance2 = 0;
		for(Map.Entry<String, Double> entry : p2.getCells().entrySet()){
			if(!usedKeys.contains(entry.getKey())){
				if(p1.getCells().containsKey(entry.getKey())){
					double prob1 = p2.getCells().get(entry.getKey());
					distance2 += Math.abs(prob1 - entry.getValue());
				} else {
					distance2 += entry.getValue();
				}
			}
		}
		return distance1 + distance2;
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
	
	/*
	 * Stores each cluster to the file system into separate files
	 * @param clusters - set of all clusters to store
	 */
	public static void saveClusters(Map<String,List<Point>> clusters) throws FileNotFoundException{
		Set<String> keys = clusters.keySet();
		Iterator<String> it = keys.iterator();
		while(it.hasNext()){
			String key = it.next();
			List<Point> cluster = clusters.get(key);
			PrintWriter pw = new PrintWriter(new File(System.getProperty("user.home")+"/Documents/bdmpFiles/output/"+"cluster"+key+".txt"));
			Iterator<Point> itPoint = cluster.iterator();
			StringBuilder sb = new StringBuilder();
			while(itPoint.hasNext()){
				Point p = itPoint.next();
				sb.append(p.toString());
			}
			pw.write(sb.toString());
			pw.close();
		}
	}
	
	/*
	 * Generates a vector of random probabilities
	 * @param size - size of the vector
	 */
	private static double[] getRandomProbabilities(int size)
	{
		Random rand = new Random();
		double randomProbabilities[] = new double[size], sum = 0.0;
		
		for (int i = 0; i < size; i++){
			randomProbabilities[i] = rand.nextDouble();
			sum = sum + randomProbabilities[i];
		}
		
		// Divide obtaining double array that sums to 1
		for(int i = 0; i < size; i++){
			randomProbabilities[i] = roundTo2decimals(randomProbabilities[i]/sum);
		}
		
		return randomProbabilities;
	}
	
	/*
	 * Rounds a double to 2 decimals
	 * @param num - number to round
	 */
	public static double roundTo2decimals(double num){
		DecimalFormat df = new DecimalFormat("#.##");
    	double rounded = Double.parseDouble(df.format(num).replaceAll(",", "."));
		return rounded;
	}
	
	/*
	 * COnverts an interval into its string representation
	 * @param cell - interval to convert
	 */
	private static String buildCell(Interval[] cell){
		StringBuilder sb = new StringBuilder();
		for(int i=0; i< cell.length; i++){
			if(i!=cell.length-1){
				sb.append(cell[i].x1+"-"+cell[i].x2+",");
			} else {
				sb.append(cell[i].x1+"-"+cell[i].x2);
			}
		}
		return sb.toString();
	}
	
	/*
	 * Initializes input and output folder on the file system
	 */
	public static void initializeFolders() throws IOException{
		File input = new File(System.getProperty("user.home")+"/Documents/bdmpFiles/input/");
		input.mkdirs(); 
		
		File output = new File(System.getProperty("user.home")+"/Documents/bdmpFiles/output/");
		if (output.isDirectory()){ // If output folder contains elements remove them
			File[] files = output.listFiles();
			int numberOfFiles = files.length;
			for (int i = 0; i < numberOfFiles; i++){
				if(files[i].isDirectory()){
					File[] subDirFiles = files[i].listFiles();
					for (File file : subDirFiles){
						file.delete();
					}
					files[i].delete();
				} else {
					files[i].delete();
				}
			}
		}
		output.mkdirs();
	}
	
	/*
	 * Converts a string that represent a location into a Interval object
	 * @param s: string to convert
	 */
	public static Interval[] parseLocation(String s){
		String[] parts = s.split(",");
		Interval[] intervals = new Interval[parts.length];
		int index=0;
		for (String part : parts){
			String[] innerParts = part.split("-");
			Interval x = new Interval(Integer.parseInt(innerParts[0]),Integer.parseInt(innerParts[1]));
			intervals[index]=x;
			index++;
		}
		return intervals;
	}
	
	public static void printClusteringInfo(Map<String,List<Point>> clusters){
		int size = clusters.keySet().size();
		System.out.println("Number of clusters: "+size);

		// Get the total number of points clustered
		int length = 0;
		for(Map.Entry<String, List<Point>> entry : clusters.entrySet()){
			length += entry.getValue().size();
		}
		System.out.println("Number of elements clustered: "+length);
	}
	
	public static void markPointsAsClustered(List<Point> points){
		for(Point p : points){
			p.clustered = true;
		}
	}
	
	public static void postProcess(Map<String,List<Point>> clusters, int minPts){
		List<String> entriesToDelete = new ArrayList<String>();
		for(Map.Entry<String, List<Point>> entry : clusters.entrySet()){
			if(entry.getValue().size() < minPts){
				entriesToDelete.add(entry.getKey());
			}
		}
		
		for(String s : entriesToDelete){
			clusters.remove(s);
		}
	}
	
	public static void savePoints(List<Point> points, String filename) throws FileNotFoundException{
		Iterator<Point> it = points.iterator();
		PrintWriter pw = new PrintWriter(new File(System.getProperty("user.home")+"/Documents/bdmpFiles/Tmp/"+filename+".txt"));
		StringBuilder sb = new StringBuilder();
		while(it.hasNext()){
			Point p = it.next();
			sb.append(p.toString());
		}
		pw.write(sb.toString());
		pw.close();
	}
	
	
}
