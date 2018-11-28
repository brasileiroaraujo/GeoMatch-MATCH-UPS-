package PointMatching20;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import LineDependencies.GeoObject;

public class RunGeoPointContext {

	public static void main(String[] args) throws Exception {
		String dataSource1 = args[0];
		String dataSource2 = args[1];
		String dataSourceContext = args[2];
		double thresholdLinguistic = Double.parseDouble(args[3]);
		double thresholdDistance = Double.parseDouble(args[4]);
		String outputPath = args[5];
		Integer amountPartition = Integer.parseInt(args[6]);
		String sourceType = args[7];
		
		
//		SparkConf sparkConf = new SparkConf().setAppName("GeoMatchingSpark").setMaster("local");
		SparkSession spark = SparkSession
				  .builder()
//				  .master("local")
				  .config("spark.some.config.option", "some-value")
				  .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
				  .getOrCreate();
		
		
		ContextMatchingBusStops20 mp = new ContextMatchingBusStops20();
		Dataset<GeoObject> busStops = ContextMatchingBusStops20.generateDataFrames(dataSource1, dataSource2, dataSourceContext, sourceType, spark);
		ContextMatchingBusStops20.run(busStops, thresholdLinguistic, thresholdDistance, amountPartition, spark).javaRDD().saveAsTextFile(outputPath);
		
	}

}
