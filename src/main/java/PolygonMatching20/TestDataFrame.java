package PolygonMatching20;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import com.vividsolutions.jts.io.ParseException;

import scala.Tuple2;

public class TestDataFrame {
	
	public static void main(String[] args) throws ParseException {
		SparkSession spark = SparkSession
				  .builder()
				  .master("local")
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
				  .getOrCreate();
		
//		// Create an instance of a Bean class
//		GeoPolygonDF p = new GeoPolygonDF("MULTIPOLYGON(((7183228.47021484 678119.910217285,7183152.82019043 678085.080200195,7183186.38018799 678010.750183105,7183261.77020264 678044.390197754,7183228.47021484 678119.910217285)))",
//				"Teste", "Pref", 1, 1);
//		
//		List<GeoPolygonDF> plist = new ArrayList<GeoPolygonDF>();
//		plist.add(p);
//		
//		
//		// Encoders are created for Java beans
//		Encoder<GeoPolygonDF> personEncoder = Encoders.javaSerialization(GeoPolygonDF.class);
//		System.out.println();
//		Dataset<GeoPolygonDF> javaBeanDS = spark.createDataset(
//		  plist,
//		  personEncoder
//		);
//		javaBeanDS.show();
		
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);
		
		Person person2 = new Person();
		person2.setName("Tiago");
		person2.setAge(25);
		
		
		List<Person> plist = new ArrayList<Person>();
		plist.add(person);
		plist.add(person2);
		
		
		// Encoders are created for Java beans
//		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Encoder<Person> personEncoder = Encoders.javaSerialization(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(
		  plist,
		  personEncoder
		);
		
//		javaBeanDS.show();
		Encoder<Tuple2<Integer, Person>> tupleEncoder = Encoders.tuple(Encoders.INT(), personEncoder);
		Dataset<Tuple2<Integer, Person>> pairedPerson = javaBeanDS.flatMap(new FlatMapFunction<Person, Tuple2<Integer, Person>>() {
			@Override
			public Iterator<Tuple2<Integer, Person>> call(Person p) throws Exception {
				List<Tuple2<Integer, Person>> list = new ArrayList<Tuple2<Integer, Person>>();
				list.add(new Tuple2<Integer, Person>(0, p));
				return list.iterator();
			}
			
		}, tupleEncoder);
		
		KeyValueGroupedDataset<Integer, Tuple2<Integer, Person>> grouped = pairedPerson.groupByKey(new MapFunction<Tuple2<Integer, Person>, Integer>() {

			@Override
			public Integer call(Tuple2<Integer, Person> value) throws Exception {
				return value._1();
			}
		}, Encoders.INT());

//		Dataset<Row> grouped = pairedPerson.groupBy("value").agg(org.apache.spark.sql.functions.collect_list("_2")).toDF("value","_2");
		
		Dataset<Tuple2<Integer, Person>> code = grouped.flatMapGroups(new FlatMapGroupsFunction<Integer, Tuple2<Integer, Person>, Tuple2<Integer, Person>>() {

			@Override
			public Iterator<Tuple2<Integer, Person>> call(Integer key, Iterator<Tuple2<Integer, Person>> values)
					throws Exception {
				@SuppressWarnings("unchecked")
				List<Tuple2<Integer, Person>> ls = IteratorUtils.toList(values);
				for (Tuple2<Integer, Person> person3 : ls) {
					System.out.println(person3._1 + ": " + person3._2.getName());
				}
				return ls.iterator();
			}
		}, tupleEncoder);
		
		code.show();
		
//		grouped.foreach(new ForeachFunction<Row>() {
//			
//			@Override
//			public void call(Row r) throws Exception {
//				WrappedArray w = (WrappedArray)r.getAs("_2");
//				List<Object> ls = JavaConversions.asJavaList(w.toList());
//				for (Person person3 : ls) {
//					System.out.println(person3.getName());
//				}
//				
//			}
//		});
		
//		grouped.show();
		
	}

}
