package bdmp2.project2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;


public class App 
{
    public static void main( String[] args )
    {
    	final double EPS = 0.2;
    	final int MINPTS = 2;
    	final int cellSize = 5;
    	final int gridInterval = 100;
    	final int dimension = 3;
    	
    	//List<String> gridPositions = Utilities.initializeGrid(cellSize, gridInterval, dimension);
    	
    	List<Point> points = Utilities.createLargeDataset(10000,cellSize, gridInterval, dimension);
    	//System.out.println(points.toString());
        //List<Point> points = Utilities.createDataset();
        //System.out.println(Utilities.KLDivergence(points.get(0), points.get(1)));
    	Map<String, List<Point>> clusters = Utilities.UDBScan(points, EPS, MINPTS);
       // System.out.println(clusters.toString());
        System.out.println("Number of Clusters: "+clusters.keySet().size());
    }
    
    //dirty
   
}
