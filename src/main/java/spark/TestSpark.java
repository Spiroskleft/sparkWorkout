package spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.List;

/**
 * Created by tsotzo on 28/4/2017.
 */
public class TestSpark {

    /**Για να το τρέξεις
     * spark-submit --class spark.TestSpark  sparkTest-1.0-SNAPSHOT.jar
     */



    public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Hello Spark");
		sparkConf.setMaster("local");

    JavaSparkContext context = new JavaSparkContext(sparkConf);

    JavaRDD<Integer> numbersRDD = context.parallelize(Arrays.asList(1,2,3));

        JavaRDD<String> cRDD = context.textFile("/usr/lib/spark/bin/test.txt");
        System.out.println("---------------$$$$$------------->"+cRDD.collect().toString());

//        JavaRDD<String> lines = context.textFile("");
//        System.out.println("----------@@@@@@-------->"+lines.count());

    JavaRDD<Integer> squaresRDD = numbersRDD.map( n -> n*n );
		System.out.println("----------1----------->"+squaresRDD.collect().toString());

    JavaRDD<Integer> evenRDD = squaresRDD.filter( n -> n%2==0 );
		System.out.println("-----------2---------->"+evenRDD.collect().toString());

    JavaRDD<Integer> multipliedRDD = numbersRDD.flatMap( n->Arrays.asList(n,n*2,n*3).iterator());
		System.out.println("------------3--------->"+multipliedRDD.collect().toString());

		context.close();

}


}
