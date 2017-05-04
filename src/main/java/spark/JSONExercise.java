package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;


/**
 * Created by Spiroskleft@gmail.com on 4/5/2017.
 */


public class JSONExercise {


    private static String inputFile;
    private static String outputDirectory;


    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Exercise")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        //Το αρχείο με τα δεδομένα
        inputFile = "/usr/lib/spark/bin/QuickTourData/t.txt";

        //Το output
        outputDirectory = "/usr/lib/spark/bin/Output";


        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df = spark.read().json(inputFile);

        //Κάνουμε προβολή των 5 πρώτων γραμμών
        //   df.take(5);

        // Προβολή των δεδομένων
        df.show();
        // Προβολή Σχήματος
        df.printSchema();

        //Απάντηση Ασκήσεως ! Επιλογή των Users

        // Δημιουργία νέου Dataset για επεξεργασία
        Dataset<Row> df2 = df.select(col("id_str"), col("user.friends_count").gt(10000).as("friends"));
//                +------------------+-------+
//                |            id_str|friends|
//                +------------------+-------+
//                |              null|   null|
//                |              null|   null|
//                |              null|   null|
//                |821758294410797056|  false|
//                |821758295056805889|   true|
//                |821758294700134400|  false|

// Δη,ιουργία view για να κάνουμε ερώτημα στη συνέχεια
        df2.createOrReplaceTempView("countUsers");
// Δημιουργία ερωτήματος KAI αποθήκευση σε αρχείο
        Dataset<Row> df3 = spark.sql("SELECT COUNT(*) FROM countUsers WHERE friends=true ");

        //Αποθήκευση σε αρχείο
        df3.write().format("com.databricks.spark.csv").save(outputDirectory);
    }

}
