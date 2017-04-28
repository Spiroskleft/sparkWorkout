package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by tsotzolas on 28/4/2017.
 */
public class WriteToFile {

    /**Διαβάζει απο αρχείο και γράφει σε αρχείο
     *
     * Για να το τρέξεις
     * spark-submit --class spark.WriteToFile  sparkTest-1.0-SNAPSHOT.jar /usr/lib/spark/bin/test.txt /usr/lib/spark/bin/testResult
     *
     * Το /usr/lib/spark/bin/test.txt είναι το arg[0]
     *
     * To /usr/lib/spark/bin/testResult είναι το arg[1]
     */
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις πρώτο
        String inputFile = args[0];
        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις δέυτερο
        String outputDirectory = args[1];

//        JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(1,2,3));

        JavaRDD<String> cRDD = context.textFile(inputFile);
        System.out.println("---------------$$$$$------------->"+cRDD.collect().toString());
        cRDD.saveAsTextFile(outputDirectory);
//
        context.close();

    }




}
