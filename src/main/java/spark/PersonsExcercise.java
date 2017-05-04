package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

import java.util.Arrays;
import java.util.stream.Stream;

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
        //Το arg[1] είναι αυτό το οποίο δίνεις οταν το τρέχεις δέυτερο
        outputDirectory = args[1];

        sparkConf.setAppName("Hello Spark");
        sparkConf.setMaster("local");

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> personInfo = context.textFile(inputFile);

        //  JavaPairRDD<String, Integer> splittedRDD = personInfo.mapToPair(s -> new Tuple2<String, Integer>(s,1));

////Σκοπός είναι να αφαιρέσουμε το όνομα από την τριπλέτα κάθε γραμμής στο αρχείο PersonInfo.txt, συνεπώς κατασκευάζουμε
//        μία συνάρτηση σε Java την stringConcat προκειμένου να το επιτύχουμε.
        JavaRDD<String> noName = personInfo.flatMap(x -> (Arrays.asList(stringConcat(x))).iterator());
        System.out.println("************************************** RDDnoName: " + noName.collect().toString());

        JavaPairRDD<Integer, Integer> pairs = noName.mapToPair(s -> new Tuple2(s.split(",")[0], s.split(",")[1]));
        System.out.println("************************************* Pairs we want:" + pairs.collect().toString());

//Todo: Given the RDD with (age,salery) pairs, how can the max salary per age be found?


        context.close();
    }

    private static String stringConcat(String x) {


        int k;
        k = x.indexOf(",");
        return x.substring(k + 1);
    }


}
