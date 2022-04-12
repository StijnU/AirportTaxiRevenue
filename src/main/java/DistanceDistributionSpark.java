import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;

/**
 * The DistanceDistributionSpark program calculates the distributions of the trips with a Spark implementation.
 *
 * @author Stijn Uytterhoeven
 * @version 1.0
 * @since 2022-04-10
 */
public class DistanceDistributionSpark {

    /**
     * Function that calculates the flat-surface formula where a spherical earth is projected onto a plane to calculate the distance
     *
     * @param startLat Double latitude coordinates of the starting position
     * @param startLong Double longitude coordinates of the starting position
     * @param endLat Double latitude coordinates of the ending position
     * @param endLong Double longitude coordinates of the ending position
     * @return Double distance in kilometers
     */
    private static double flatSurfaceDistanceInKilometers(double startLat, double startLong, double endLat, double endLong) {
        double coordToRad = Math.PI/180;
        return 6371.009 * Math.sqrt(Math.pow((startLat - endLat) * coordToRad, 2) + Math.pow(Math.cos((startLat + endLat) * coordToRad / 2) * (startLong - endLong) * coordToRad, 2));
    }

    /**
     * Class that implements the Spark Function interface.
     *
     * Class is used to map the input text to the distance of the trip they represent
     */
    public static class CalcDistances implements Function<String, Double> {

        /**
         * Call function which calculates the distance of the input String
         * @param str String input string
         * @return Double distance
         */
        public Double call(String str){
            String[] parsed = str.split(" ");
            double startLat = Double.parseDouble(parsed[2]);
            double startLong = Double.parseDouble(parsed[3]);
            double endLat = Double.parseDouble(parsed[5]);
            double endLong = Double.parseDouble(parsed[6]);

            return flatSurfaceDistanceInKilometers(startLat, startLong, endLat, endLong);
        }
    }

    /**
     * The DirDeleter is a helper class that implements 1 function to delete the output directories
     * so Spark does not throw a FileAlreadyExistsException.
     */
    public static class DirDeleter {
        static void deleteDirectory(File directoryToBeDeleted) {
            File[] allContents = directoryToBeDeleted.listFiles();
            if (allContents != null) {
                for (File file : allContents) {
                    deleteDirectory(file);
                }
            }
            directoryToBeDeleted.delete();
        }
    }

    /**
     * Main function of the DistributionCalculatorSpark program
     * @param args String[] with input and output path
     */
    public static void main(String[] args){
        //Setup the spark context
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("distribution calculator").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //Read lines from textfile
        JavaRDD<String> lines = sc.textFile(args[0]);

        //Transform files to distance
        JavaRDD<Double> dists = lines.map(new CalcDistances());

        //Create pairs (distance, 1)
        JavaPairRDD<Double, Integer> ones = dists.mapToPair(d -> new Tuple2<>(d, 1));

        //Reduce pairs by adding up values with same key
        JavaPairRDD<Double, Integer> counts = ones.reduceByKey((d1, d2) -> d1 + d2);

        //Write to output file

        ArrayList<Long> allRuns = new ArrayList<>();
        for (int i = 0; i < 51; i++) {
            //DirDeleter.deleteDirectory(new File(args[1]));
            //counts.saveAsTextFile(args[1]);
            long start = System.currentTimeMillis();
            counts.collect();
            long end = System.currentTimeMillis();
            System.out.println((end - start));
            if (i != 0)
                allRuns.add((end - start));
        }
        long sum = 0;
        for (long run : allRuns){
            sum += run;
        }
        System.out.println("Mean: " + sum/allRuns.size());
    }
}
