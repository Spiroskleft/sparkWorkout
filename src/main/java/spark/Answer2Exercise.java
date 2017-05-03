package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Spiroskleft@gmail.com on 2/5/2017.
 */

//Για να το τρέξουμε, μπαίνουμε εντός του φακέλου bin στο spark και πληκτρολογούμε:
// ./spark-submit --class spark.NewsExercise  sparkTest-1.0-SNAPSHOT.jar /usr/lib/spark/bin/QuickTourData/news.txt /usr/lib/spark/bin/testResult

public class Answer2Exercise {

    private static String inputFile;
    private static String outputDirectory;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις πρώτο πχ /usr/lib/spark/bin/QuickTourData/news.txt
        inputFile = args[0];
        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις δέυτερο πχ /usr/lib/spark/bin/testResult
        outputDirectory = args[1];

        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> StringToIntegerRDD = context.textFile(inputFile);
        System.out.println("******************************Count of Words: " + StringToIntegerRDD.count() );
        System.out.println("******************************Collect" + StringToIntegerRDD.collect().toString());



        // Μετατροπή σε Double
        JavaRDD<Double> doubleJavaRDD = StringToIntegerRDD.map(Double::valueOf);

        // Αύξηση κατά 25%
        JavaRDD<Double> addJavaRDD = doubleJavaRDD.map(x -> x*1.25);
        System.out.println("***************************** Auksisi 25%: " + addJavaRDD.collect().toString());

        // Συνολικός αριθμός πωλήσεων
        double totalJavaRDD = addJavaRDD.reduce((a, b) -> a + b) ;
        System.out.println("***************************** Total Sales: " + totalJavaRDD);

        // Πωλήσεις μεγαλύτερες από 160
        JavaRDD<Double> greaterJavaRDD = addJavaRDD.filter(x -> x>160);
        System.out.println("***************************** Sales Greater then 160:" + greaterJavaRDD.collect().toString());

        greaterJavaRDD.saveAsTextFile(outputDirectory);


        context.close();
    }

// Τέλος πρώτου μέρους άσκησης

}
