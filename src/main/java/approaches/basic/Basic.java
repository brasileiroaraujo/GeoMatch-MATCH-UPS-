package approaches.basic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;
import utils.StringSimilarity;

public class Basic {

	public static final Double LIMIAR = 0.7;
	
	public static void main(String[] args) {

		if (args.length < 1) {
			System.err.println("Please provide the input file full path as argument");
			System.exit(0);
		}
		
		SparkConf conf = new SparkConf().setAppName("com.wordexample.spark.WordCount").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> file = context.textFile(args[0]);
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public  List<String> call(String s) throws Exception {
				return Arrays.asList(s.split(" "));
			}
		});

		JavaPairRDD<String, String> key_word_pair = words.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				return new Tuple2<String, String>(s.toLowerCase().substring(0, 1), s);
			}
		});


		JavaPairRDD<String, Iterable<String>> group_by_key = key_word_pair.groupByKey();
				
		JavaRDD<Tuple3<String, String, Double>> similarity = group_by_key.flatMap(new FlatMapFunction<Tuple2<String,Iterable<String>>, Tuple3<String, String, Double>>() {

			@Override
			public List<Tuple3<String, String, Double>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				
				List<Tuple3<String, String, Double>> list = new ArrayList<Tuple3<String, String, Double>>();				
				
				Iterable<String> words =  tuple._2;
				
				int count = 0;
				for (Iterator iterator = words.iterator(); iterator.hasNext();) {		
					String w1 = (String) iterator.next();
					count++;	
					int it = 0;
					for (Iterator iterator2 = words.iterator(); iterator2.hasNext();) {
						for (int i = it; i < count; i++) {
							it++;
							iterator2.next();
						}
						if (iterator2.hasNext()) {
							String w2 = (String) iterator2.next();
							Double similarity = StringSimilarity.similarity(w1, w2);
							if (similarity >= LIMIAR) {
								list.add(new Tuple3<String, String, Double>(w1, w2, similarity));
							}
						}
					}
					
				}				
				return list;
			}
		});
		
		for (Tuple3<String, String, Double> tuple :similarity.collect()){
			System.out.println(tuple);
		}
		
		
		
		System.out.println("DONE!");

	}

}
