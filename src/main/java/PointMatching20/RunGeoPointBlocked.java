package PointMatching20;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import LineDependencies.GeoObject;

public class RunGeoPointBlocked {

	public static void main(String[] args) throws Exception {
		String dataSource1 = args[0];
		String dataSource2 = args[1];
		double thresholdLinguistic = Double.parseDouble(args[2]);
		double thresholdDistance = Double.parseDouble(args[3]);
		String outputPath = args[4];
		Integer amountPartition = Integer.parseInt(args[5]);
		String sourceType = args[6];
		
		
//		SparkConf sparkConf = new SparkConf().setAppName("GeoMatchingSpark").setMaster("local");
		SparkSession spark = SparkSession
				  .builder()
//				  .master("local")
				  .config("spark.some.config.option", "some-value")
				  .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
				  .getOrCreate();
		
		
		MatchingBusStops20Blocked mp = new MatchingBusStops20Blocked();
		Dataset<GeoObject> busStops = MatchingBusStops20.generateDataFrames(dataSource1, dataSource2, sourceType, spark);
		MatchingBusStops20.run(busStops, thresholdLinguistic, thresholdDistance, amountPartition, spark).javaRDD().saveAsTextFile(outputPath);

	}

}
