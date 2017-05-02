package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by Spiroskleft@gmail.com on 2/5/2017.
 */
public class NewsExercise {

    private static String inputFile;
    private static String outputDirectory;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις πρώτο
        inputFile = args[0];
        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις δέυτερο
        outputDirectory = args[1];

        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> wordCount = context.textFile(inputFile);

        JavaRDD<String> words = wordCount.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println("********************************* Words: " + words.count());

        System.out.println("********************************* Distinct Words: " + words.distinct().count());


        JavaRDD<String> newYork = context.textFile(inputFile);
        JavaRDD<String> NYcount = newYork.filter(x->x.contains("New York"));

        System.out.println("************************************ New York Stories" + NYcount.count());

       // System.out.println("********************************* Storie About NEW YORK" + words.filter(x->x.contains("New York")));

    }
}
