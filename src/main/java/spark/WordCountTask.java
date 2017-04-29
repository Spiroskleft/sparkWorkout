package spark; /**
 * Created by tsotzo on 28/4/2017.
 */


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;


/**Διαβάζει απο αρχείο και γράφει σε αρχείο
 *
 * Για να το τρέξεις
 * spark-submit --class spark.WordCountTask  sparkTest-1.0-SNAPSHOT.jar /usr/lib/spark/bin/test.txt /usr/lib/spark/bin/testResult
 *
 * Το /usr/lib/spark/bin/test.txt είναι το arg[0]
 *
 * To /usr/lib/spark/bin/testResult είναι το arg[1]
 */

public class WordCountTask {

    private static String inputFile;
    private static String outputDirectory;


    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

    public static void main(String[] args) {
        checkArgument(args.length > 1, "Please provide the path of input file as first parameter.");
        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις πρώτο
        inputFile = args[0];
        //Το arg[0] είναι αυτό το οποίο δίνεις οταν το τρέχεις δέυτερο
        outputDirectory = args[1];

        new WordCountTask().run(args[1]);
    }




    public void run(String inputFilePath) {
        String master = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName(WordCountTask.class.getName())
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> stringJavaRDD = context.textFile(inputFile);


        JavaPairRDD<String, Integer> counts = stringJavaRDD
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        counts.saveAsTextFile(outputDirectory);

        System.out.println("--------->"+counts.toString());
    }



}
