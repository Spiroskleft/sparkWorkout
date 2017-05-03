package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Spiroskleft@gmail.com on 3/5/2017.
 */
public class CityExercise {

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
        JavaRDD<String> cityInfo = context.textFile(inputFile);





    }

}
