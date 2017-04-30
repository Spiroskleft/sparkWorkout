package spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by tsotzolas on 29/4/2017.
 * ---------------------------------------------------------------------
 * Είναι το δεύτερο μέρος απο την εργασία Working with an RDD την οποία μας έδωσε ο καθηγητής.
 * μέχρι εκεί που λέει να είναι μεγαλύτερο απο 160
 * ---------------------------------------------------------------------
 * <p>
 * Για να το τρέξεις
 * spark-submit --class spark.Exercise  sparkTest-1.0-SNAPSHOT.jar /usr/lib/spark/bin/QuickTourData/news.txt /usr/lib/spark/bin/testResult
 *
 * Το /usr/lib/spark/bin/test.txt είναι το arg[0]
 * <p>
 * To /usr/lib/spark/bin/testResult είναι το arg[1]
 */

public class Exercise1 {
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

        System.out.println("-----LinesCount---------------------->" + javaRDD.count());


        //Χωρίζουμε όλο το κείμενο να είνα σε κάθε γραμμή και μια λέξη
        JavaRDD<String> javaRDD1 = javaRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        System.out.println("-----wordCount---------------------->" + javaRDD1.count());
//        javaRDD1.saveAsTextFile(outputDirectory);

        System.out.println("-----wordCountDistinct---------------------->" + javaRDD1.distinct().count());
//------------------------------------------------------------------------------------------------------------------//

        //Ξαναφορτώνουμε το κείμενο για να μπούνε κάθε ιστορία σε κάθε γραμμή.
        JavaRDD<String> newsRDD = context.textFile(inputFile);

        //Για το πόσες ιστορίες αναφέρονται στο New York
        //Το έκανα χρησιμοποιώντας πόσες εγγραφές αναφέρουν τη λέξη New York
        //Και όταν λέω πόσες γραμμές εννοώ πόσες απο τις 30 ιστορίες
        JavaRDD<String> newsRDD2 = newsRDD.filter(x->x.contains("New York"));

        System.out.println("----------------News for New York-------------------------->"+ newsRDD2.count());

    }


}
