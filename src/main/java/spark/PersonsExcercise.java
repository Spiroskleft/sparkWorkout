package spark;

import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.security.Key;

/**
 * Created by Spiroskleft@gmail.com on 2/5/2017.
 */
public class PersonsExcercise {
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
        JavaRDD<String> PersonInfo = context.textFile(inputFile);

     //   JavaPairRDD<Key,Value> javaPairRDD = PersonInfo.flatMapToPair(s -> )



    }


}
