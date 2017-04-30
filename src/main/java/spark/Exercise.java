package spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;

/**
 * Created by tsotzolas on 29/4/2017.
 * ---------------------------------------------------------------------
 * Είναι το πρώτο μέρος απο την εργασία το Working with an RDD την οποία μας έδωσε ο καθηγητής.
 * μέχρι εκεί που λέει να είναι μεγαλύτερο απο 160
 * ---------------------------------------------------------------------
 * <p>
 * Για να το τρέξεις
 * spark-submit --class spark.Exercise  sparkTest-1.0-SNAPSHOT.jar /usr/lib/spark/bin/QuickTourData/sales.txt /usr/lib/spark/bin/testResult
 *
 * Το /usr/lib/spark/bin/QuickTourData/sales.txt είναι το arg[0]
 * και είναι το inputFile
 * <p>
 * To /usr/lib/spark/bin/testResult είναι το arg[1]
 * και είναι εκεί το οποίο γράφει τα αποτελέσματα
 */

public class Exercise {
    private static String inputFile;
    private static String outputDirectory;


    public static void main(String[] args) {
        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις πρώτο
        inputFile = args[0];
        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις δέυτερο
        outputDirectory = args[1];

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> javaRDD = context.textFile(inputFile);

        System.out.println("-----WordCount---------------------->" + javaRDD.count());
        System.out.println("-----------Collect---------------->" + javaRDD.collect().toString());

        //Τα μετατρέπουμε σε Double
        JavaRDD<Double> doubleRDD = javaRDD.map(x -> Double.valueOf(x));

        //Τα αυξάνουμε κατά 25%
        JavaRDD<Double> doubleRDD1= doubleRDD.map(x -> x*1.25);
        System.out.println("--------+25%----------->" + doubleRDD1.collect().toString());

        //Για να τα προσθεσουμε ολα μαζί!
        double totalInTime = doubleRDD1.reduce((a, b) -> a + b);

        System.out.println("--------+SumAllOfThem----------->" + totalInTime);

        //Φτιάχνουμε ένα καινούργιο RDD όπυου βάουμε αυτά που ειναι πάνω απο 160
        JavaRDD<Double> doubleRDD2 = doubleRDD1.filter(x->x>160);

        System.out.println("-------->160----------->" + doubleRDD2.collect().toString());

        //Τα τελικό αποτέλεσμα αποθηκεύουμε σε αρχείο
        doubleRDD2.saveAsTextFile(outputDirectory);

    }


}
