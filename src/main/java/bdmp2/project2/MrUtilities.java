package bdmp2.project2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MrUtilities {
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
}
