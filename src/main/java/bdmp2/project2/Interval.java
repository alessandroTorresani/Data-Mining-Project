package bdmp2.project2;
import org.apache.commons.math3.util.Pair;

public class Interval {
	int x1;
	int x2;
	
	public Interval(int x1, int x2){
		this.x1 = x1;
		this.x2 = x2;
	}
	
	public Interval(){
		x1=0;
		x2=0;
	}
	
	@Override
	public String toString(){
		return ("["+x1+"-"+x2+"]");
	}
}
