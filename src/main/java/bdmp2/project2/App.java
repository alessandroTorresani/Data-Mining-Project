package bdmp2.project2;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class App 
{
    public static void main( String[] args ) throws FileNotFoundException
    {
    	final double EPS = 0.4;
    	final int MINPTS = 10;
    	final int cellSize = 1;
    	final int gridInterval = 30;
    	final int dimension = 2;
    	
    	//List<String> gridPositions = Utilities.initializeGrid(cellSize, gridInterval, dimension);
    	
    	 //Utilities.createLargeDataset(3000,cellSize, gridInterval, dimension);
    	 List<Point> points = Utilities.readDataset("dataset.txt", dimension);
    	 System.out.println(points.toString());
    	 
    	//System.out.println("Points: " );
    	//System.out.println(points.toString());
        //List<Point> points = Utilities.createDataset();
    	/*Map<String, List<Point>> clusters = Utilities.UDBScan(points, EPS, MINPTS);
    	int size = clusters.keySet().size();
    	System.out.println("Number of Clusters: "+size);
    	
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
		}*/
    }
    
    //dirty
}
