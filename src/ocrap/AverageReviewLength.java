package ocrap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AverageReviewLength {
	
	public static void main(String[] args) throws Exception {
		
		SparkConf sparkConf = new SparkConf().setAppName("OCRAP_AverageReviewLength").setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaPairRDD<String, String> wholeTextFiles = sparkContext.wholeTextFiles(args[0], 1);
		
		// transformation: extract text from files
		JavaRDD<String> wholeText = wholeTextFiles.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
			@Override
			public Iterable<String> call(Tuple2<String, String> wholeTextFile) {
				return Arrays.asList(wholeTextFile._2());
			}
		});
		
		// transformation: split text into reviews
		JavaRDD<String> reviews = wholeText.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String wholeText) {
				return Arrays.asList(wholeText.split("\\n\\n"));
			}
		});
		
		// transformation: form key-value-pairs such that review id (key) and review description (value)
		JavaPairRDD<String, String> reviewDescriptionPairs = reviews.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String review) {
				Tuple2<String, String> result = new Tuple2<String, String>("FLAG", "INVALID");
				String[] lines = review.split("\\n");
				String tmp = null, key = null;
				for (String line : lines) {
					String[] elements = line.split(" ", 2);
					if (elements[0].equals("product/productId:")) {
						tmp = elements[1];
					} if (elements[0].equals("review/userId:")) {
						key = tmp + elements[1];
					} if (elements[0].equals("review/text:")) {
						result = new Tuple2<String, String>(key, elements[1]);
					}
				}
				return result;
			}
		});
		
		// action: count review ids (how many reviews?)
		List<Tuple2<String, String>> numberOfReviewDescriptionPairs = reviewDescriptionPairs.collect();
		Integer numberOfReviews = numberOfReviewDescriptionPairs.size();
		
		// transformation: transform into key-value-pairs such that review id (key) and 1 (value) for each word in the description
		JavaPairRDD<String, Integer> reviewWordPairs = reviewDescriptionPairs.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, Integer>() {
			@Override
			public Iterable<Tuple2<String, Integer>> call(Tuple2<String, String> reviewDescriptionTuple) {
				List<Tuple2<String, Integer>> result = new ArrayList<Tuple2<String, Integer>>();
				String[] words = reviewDescriptionTuple._2().split(" ");
				for (String word : words) {
					result.add(new Tuple2<String, Integer>(reviewDescriptionTuple._1(), 1));
				}
				return result;
			}
		});
		
		// action: count words for each review id (key)
		JavaPairRDD<String, Integer> reviewWordCountPairs = reviewWordPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		// action: collect result
		List<Tuple2<String, Integer>> output = reviewWordCountPairs.collect();
		
		// action: sum length of review description (count total words in review description)
		Integer totalWords = new Integer(0);
		for (Tuple2<String, Integer> tuple : output) {
			totalWords += tuple._2();
		}
		
		// calculate number of average words in a review
		Float averageReviewLength = (float) totalWords/numberOfReviews;
		
		File file = new File(args[1]);
		BufferedWriter out = new BufferedWriter(new FileWriter(file));
		
		System.out.println(averageReviewLength);
		out.write(averageReviewLength + "\n");
		
		out.close();
		sparkContext.stop();
	}
}
