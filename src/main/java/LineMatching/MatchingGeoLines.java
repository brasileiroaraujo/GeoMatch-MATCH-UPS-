//package LineMatching;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;
//
//import LineDependencies.GPSLine;
//import LineDependencies.GeoLine;
//import LineDependencies.ShapeLine;
//import PointDependencies.GPSPoint;
//import PointDependencies.ShapePoint;
//import scala.Tuple2;
//
//public class MatchingGeoLines {
//
//	public static void main(String[] args) throws Exception {
//		 String textFile = "bus_data/shapes/shapes.txt";
//         String textFile2 = "bus_data/gps/gps_data_2016_10_30.csv";
//
//         SparkConf sparkConf = new SparkConf().setAppName("JavaDeduplication").setMaster("local");
//         JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//         
//         JavaRDD<String> shapePoints = ctx.textFile(textFile, 1);
//         JavaRDD<String> gpsPoints = ctx.textFile(textFile2, 1);
//
//         JavaPairRDD<String, Iterable<ShapePoint>> rddShapePair = shapePoints.mapToPair(new PairFunction<String, String, ShapePoint>() {
// 			@Override
// 			public Tuple2<String, ShapePoint> call(String s) throws Exception {
// 				ShapePoint shapePoint = ShapePoint.createShapePoint(s);
// 				return new Tuple2<String, ShapePoint>(shapePoint.getId(), shapePoint);
// 			}
// 		}).groupByKey();
//         
//        // groupedShape.saveAsTextFile("groupedShape");
//        
//         JavaPairRDD<String, Iterable<GPSPoint>> rddGPSPair = gpsPoints.mapToPair(new PairFunction<String, String, GPSPoint>() {
//  			@Override
//  			public Tuple2<String, GPSPoint> call(String s) throws Exception {
//  				GPSPoint gpsPoint = GPSPoint.createGPSPoint(s);
//  				return new Tuple2<String, GPSPoint>(gpsPoint.getBusCode(), gpsPoint);
//  			}
//  		}).groupByKey();
//        
//          JavaRDD<GeoLine> rddShapeLine = rddShapePair.flatMap(new FlatMapFunction<Tuple2<String, Iterable<ShapePoint>>, GeoLine>() {
//        	  List<GeoLine> output;
//        	  GeoLine shapeLine;
//        	  
//			@Override
//			public List<GeoLine> call(Tuple2<String, Iterable<ShapePoint>> pair) throws Exception {
//				 output = new ArrayList<GeoLine>();
//				 shapeLine = new ShapeLine(pair._1);
//				for (ShapePoint shapePoint : pair._2) {
//					shapeLine.addGeoPoint(shapePoint);
//				}
//				output.add(shapeLine);
//				return output;
//			}
//		});
//
//         rddShapeLine.saveAsTextFile("rddShapeLine");
//          
//          JavaRDD<GeoLine> rddGPSLine = rddGPSPair.flatMap(new FlatMapFunction<Tuple2<String, Iterable<GPSPoint>>, GeoLine>() {
//        	  List<GeoLine> output;
//        	  GeoLine gpsLine;
//        	  
//			@Override
//			public List<GeoLine> call(Tuple2<String, Iterable<GPSPoint>> pair) throws Exception {
//				 output = new ArrayList<GeoLine>();
//	        	 gpsLine = new GPSLine(pair._1);
//				for (GPSPoint shapePoint : pair._2) {
//					gpsLine.addGeoPoint(shapePoint);
//				}
//				output.add(gpsLine);
//				return output;
//			}
//		});
//          
//        rddGPSLine.saveAsTextFile("rddGPSLine");
//       
//        ctx.stop();
//		ctx.close();
//	}
//}
