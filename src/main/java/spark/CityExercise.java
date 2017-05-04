package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by Spiroskleft@gmail.com on 3/5/2017.
 */
public class CityExercise {

    private static String inputFile1;
    private static String inputFile2;
    private static String outputDirectory;

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        //Το arg[0] είναι πρώτη παράμετρος στην εντολή που το τρέχουμε
        inputFile1 = args[0];
        //Το arg[1] είναι δεύτερη παράμετρος στην εντολή που το τρέχουμε
        inputFile2 = args[1];
        //Το arg[2] είναι τρίτη παράμετρος στην εντολή που το τρέχουμε
        outputDirectory = args[2];

        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> cityInfoRDD = context.textFile(inputFile1);
        JavaRDD<String> personInfoRDD = context.textFile(inputFile2);

        PairFunction<String, String, String> pairFunction =
                (PairFunction<String, String, String>) s -> TupleNeeded(s);

        JavaPairRDD<String, String> cityInfoPairRDD = cityInfoRDD.mapToPair(pairFunction);
        JavaPairRDD<String, String> personInfoPairRDD = personInfoRDD.mapToPair(pairFunction);

        System.out.println("*************************************************** City Pair:" + cityInfoPairRDD.collect().toString());
        System.out.println("*************************************************** Person Pair:" + personInfoPairRDD.collect().toString());

        // Join των RDDs
        JavaPairRDD<String, Tuple2<String, String>> joinedRDD = personInfoPairRDD.join(cityInfoPairRDD);
        System.out.println("*********************************************** Joined RDDs: " + joinedRDD.collect().toString());


        context.close();
    }

    public static Tuple2 TupleNeeded(String s) {

        return new Tuple2(s.split(",")[0], s.substring(s.indexOf(",") + 1, s.length()));
    }

}
