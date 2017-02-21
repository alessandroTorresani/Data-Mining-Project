package bdmp2.project2;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class App 
{
    public static void main( String[] args )
    {
    	final double EPS = 1.4;
    	final int MINPTS = 2;
    	final int cellSize = 5;
    	final int gridInterval = 30;
    	final int dimension = 3;
    	
    	//List<String> gridPositions = Utilities.initializeGrid(cellSize, gridInterval, dimension);
    	
    	//List<Point> points = Utilities.createLargeDataset(1000,cellSize, gridInterval, dimension);
    	//System.out.println("Points: " );
    	//System.out.println(points.toString());
        List<Point> points = Utilities.createDataset();
    	Map<String, List<Point>> clusters = Utilities.UDBScan(points, EPS, MINPTS);
    	System.out.println("Number of Clusters: "+clusters.keySet().size());
        System.out.println(clusters.toString());
       /* try {
        	Utilities.saveClusters(clusters);
        } catch (FileNotFoundException e) {
			// TODO: handle exception
        	System.err.println(e.getMessage());
		}*/
    }
    
    //dirty
}
