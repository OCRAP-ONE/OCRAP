package ocrap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class WordFrequencyExcluded {
	
	public static void main(String[] args) throws Exception {
		
		SparkConf sparkConf = new SparkConf().setAppName("OCRAP_WordFrequencyExcluded").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sparkContext.textFile(args[0], 1);
		JavaRDD<String> wordsToExclude = sparkContext.textFile(args[1], 1);
		
		// shared variable: broadcast variable (read-only)
		final Broadcast<List<String>> wordsBlackList = sparkContext.broadcast(wordsToExclude.collect());
		
		// transformation: split lines into words
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String line) {
				//String lineReplaced = line.toLowerCase();
				return Arrays.asList(line.split(" "));
			}
		});
		
		// transformation: form key value pairs of words such that word (key) and 1 (value)
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String word) {
				if(wordsBlackList.value().contains(word)) {
					return new Tuple2<String, Integer>(word, 0);
				} else {
					return new Tuple2<String, Integer>(word, 1);
				}
			}
		});
		
		// action: sum occurences of each word (key)
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		// transformation: swap pairs in order to sort by value rather than by key
		JavaPairRDD<Integer, String> swappedPair = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
				return item.swap();
			}
		});
		
		// action: sort in descending order rather than in ascending order
		JavaPairRDD<Integer, String> swappedPairSorted = swappedPair.sortByKey(false);
		
		// action: swap pairs back after sorting
		JavaPairRDD<String, Integer> swappedPairSortedSwapBack = swappedPairSorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
				return item.swap();
			}
		});

		// action: collect 100 most frequent words
		List<Tuple2<String, Integer>> output = swappedPairSortedSwapBack.take(100);

		File file = new File(args[2]);
		BufferedWriter out = new BufferedWriter(new FileWriter(file));

		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + "\t" + tuple._2());
			out.write(tuple._1() + "\t" + tuple._2() +"\n");
		}
		
		out.close();
		sparkContext.stop();
	}
}
