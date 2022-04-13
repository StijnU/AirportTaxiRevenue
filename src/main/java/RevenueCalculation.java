import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;

/**
 * The RevenueCalculation program calculates the revenue with a file.segments as input, it uses
 * a Map/Reduce implementation from hadoop.
 *
 * @author Stijn Uytterhoeven
 * @version 1.0
 * @since 2022-04-10
 */
public class RevenueCalculation {


    /**
     * The DirDeleter is a helper class that implements 1 function to delete the output directories
     * so hadoop does not throw a FileAlreadyExistsException.
     *
     * NOTE: This only works if the code is run locally
     */
    public static class DirDeleter {

        /**
         * Deletes the directory given as Java File class.
         * @param directoryToBeDeleted The directory to delete as Java File class
         */
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
     * The TripReconstructionMapper is an extension on the Hadoop Mapper Class.
     * It maps the input segments as Text to a Key-Value pair with IntWritable as Key and Segment as value
     */
    public static class TripReconstructionMapper extends Mapper<LongWritable, Text, IntWritable, Segment> {
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /**
         * Map function which maps the input to the output
         *
         * Reads the input, parses it, and then creates a segment with the parsed data.
         * Then the pair (taxi-id, segment) is written into context
         *
         * Input    : Text: <taxi-id>, <start date>, <start pos (lat)>, <start pos (long)>, <start status> <end date> <end pos (lat)> <end pos (long)> <end status>
         * Output   : (Key : Int of <taxi-id>, Value : Segment of all data)
         *
         * @param key LongWritable, the key of the input
         * @param ln Text, the input line see above
         * @param context The hadoop mapper context
         */
        public void map(LongWritable key, Text ln, Context context){
            String str = ln.toString();
            String[] vals = str.split(",");

            try {
                int taxiId = Integer.parseInt(vals[0]);
                Date startDate = sdf.parse(vals[1].replace("'", ""));
                double startLat = Double.parseDouble(vals[2]);
                double startLong = Double.parseDouble(vals[3]);
                Date endDate = sdf.parse(vals[5].replace("'", ""));
                double endLat = Double.parseDouble(vals[6]);
                double endLong = Double.parseDouble(vals[7]);
                char status = vals[8].charAt(1);

                Segment segment = new Segment(taxiId, startDate, endDate, startLat, startLong, endLat, endLong, status);
                context.write(new IntWritable(taxiId), segment);

            }
            // Exception means that the input value is not valid, thus this is ignored
            catch (Exception ignored){
            }
        }
    }

    /**
     * The TripReconstructionReducer is an extension on the Hadoop Reducer Class.
     * Reduces the key-value pair (IntWritable, Segment) to another Key-value pair (NullWritable,Segment)
     * Segments are reduced by combining them to trips
     */
    public static class TripReconstructionReducer extends Reducer<IntWritable, Segment, NullWritable, Segment> {

        private ArrayList<Segment> fullTrips = new ArrayList<>();
        private ArrayList<Segment> emptyTrips = new ArrayList<>();

        /**
         * Reduce function which reduces the input segments by combining them
         *
         * First the findTrips function is called which updates the fullTrips and emptyTrips fields with the trips found in the input segments.
         * then writes all trips into context.
         *
         * Input    : key: IntWritable, value: Segment
         * Output   : key: IntWritable, value: Segment
         *
         * @param key the key of the values (taxi-id)
         * @param values iterable of the segments with the same key (taxi-id)
         * @param context the hadoop reducer context
         */
        public void reduce(IntWritable key, Iterable<Segment> values, Context context) throws IOException, InterruptedException {
            ArrayList<Segment> trips = findTrips(values);
            for (Segment trip : trips){
                if (trip.isFull()) {
                    context.write(NullWritable.get(), trip);
                }
            }
        }

        /**
         * Helper function for Reducer
         *
         * This function takes in and Iterable of Segments, and tries to find all the trips within the Iterable
         *
         * the iterable is divided into full taxi and empty taxi segments, and a deque of the split iterables is generated.
         * Then the function change the fullTrips and emptyTrips fields of the class
         *
         * @param segments iterable of segments
         */

        public ArrayList<Segment> findTrips(Iterable<Segment> segments){
            // Wanted to make a deque out of the iterable, this may be an inefficient way to do this
            Deque<Segment> segmentsDeque = new ArrayDeque<>();
            for (Segment segment : segments){
                segmentsDeque.add(new Segment(segment));
            }
            return combineSegments(segmentsDeque);
        }

        /**
         * Helper function for findTrips
         *
         * This function takes in a deque of segments, and returns an ArrayList of all the trips found in the deque
         *
         * Goes over all segments and tries to combine it with another segment, if no combination is possible a trip is found.
         * This trip is then added the the returning ArrayList.
         *
         * @param segments Deque of segments
         * @return ArrayList of trips (combined segments)
         */
        private ArrayList<Segment> combineSegments(Deque<Segment> segments){
            ArrayList<Segment> trips = new ArrayList<>();
            Segment curr = segments.poll();
            boolean combined = false;
            while (curr != null) {
                for (Segment segment : segments) {
                    if (segment.tryCombine(curr)) {
                        combined = true;
                        break;
                    }
                }
                if (!combined){
                    trips.add(curr);
                }
                curr = segments.poll();
                combined = false;
            }
            return trips;
        }
    }

    /**
     * The TripFilter is an extension on the Hadoop Mapper Class.
     * It maps a pair (LongWritable, Text) to a pair (NullWritable, Segment)
     * It filters out all the false segments
     */
    public static class TripFilter extends Mapper<LongWritable, Text, NullWritable, Segment>{
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /**
         * Map function which maps the input to the output
         *
         * Reads the input, parses it, checks whether the read segment is valid for revenue calculation
         * If valid then writes it into context.
         *
         * Input    : Text: String of Segment fields <id> <timeInHours> <startLat> <startLong> <endLat> <endLong> <full> <tooFast> <closeTooAirport>
         * Output   : Key : NulWritable, Value : Segment
         *
         * @param key LongWritable key
         * @param ln Text line output of the first Map/Reduce
         * @param context The hadoop mapper context
         */
        public void map(LongWritable key, Text ln, Context context){
            try {
                Segment segment = new Segment(ln.toString());
                if (!segment.isTooFast() && segment.isCloseToAirport() && segment.isFull()) {
                    context.write(NullWritable.get(), segment);
                }
            }
            catch(Exception ignored) {}
        }
    }

    /**
     * The TripFilterRevenueOverTime is an extension on the Hadoop Mapper Class.
     * It maps a pair (LongWritable, Text) to a pair (Text, DoubleWritable)
     * It filters out all the false and returns key-value pair with key as date
     */
    public static class TripFilterRevenueOverTime extends Mapper<LongWritable, Text, Text, DoubleWritable>{
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        /**
         * Map function which maps the input to the output
         *
         * Reads the input, parses it, checks whether the read segment is valid for revenue calculation
         * If valid then writes it into context.
         *
         * Input    : Text: String of Segment fields <id> <timeInHours> <startLat> <startLong> <endLat> <endLong> <full> <tooFast> <closeTooAirport>
         * Output   : Key : Text, Value : DoubleWritable
         *
         * @param key LongWritable key
         * @param ln Text line output of the first Map/Reduce
         * @param context The hadoop mapper context
         */
        public void map(LongWritable key, Text ln, Context context){
            try {
                Segment segment = new Segment(ln.toString());
                if (!segment.isTooFast() && segment.isCloseToAirport() && segment.getDistance() > 0 && segment.isFull()) {
                    context.write(new Text(sdf.format(segment.getStartDate().getTime())), new DoubleWritable(segment.getDistance() * 1.79 + 3.25));
                }
            }
            catch(Exception ignored) {}
        }
    }

    /**
     * The RevenueCalculationReducer is and extension of the hadoop Reducer Class.
     * Reduces the key-value pair (NullWritable, Segment) to another Key-value pair (NullWritable, Double)
     * Segments are reduced by calculating their revenue and adding these
     */
    public static class RevenueCalculationReducer extends Reducer<NullWritable, Segment, NullWritable, DoubleWritable>{

        /**
         * Reduce function which reduces the input segments by calculating their revenue and adding them up
         *
         * For each segment in the iterable the revenue is called and added up.
         * then writes the revenue into context.
         *
         * Input    : key: NullWritable, value: Segment
         * Output   : key: NullWritable, value: DoubleWritable
         *
         * @param key NullWritable key
         * @param segments Iterable of all the segments
         * @param context Hadooop reducer context.
         */
        public void reduce(NullWritable key, Iterable<Segment> segments, Context context) throws IOException, InterruptedException {
            double totalRevenue = 0.0;
            int i = 0;
            for (Segment segment : segments){
                totalRevenue += segment.getDistance() * 1.79 + 3.25;
                i += 1;
            }
            context.write(NullWritable.get(), new DoubleWritable(totalRevenue));
            context.write(NullWritable.get(), new DoubleWritable(i));
        }
    }

    /**
     * Function which creates the trip reconstruction job
     *
     * The trip reconstruction job is a Hadoop job which takes a .segments file as input and outputs a text file with
     * the reconstructed trips as Strings of the Segment fields
     *
     * @param input Path of the input file
     * @param output Path of the output directory
     * @return Hadoop job
     */
    public static Job tripReconstructionJob(Path input, Path output) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "trip-reconstruction");
        job.setJarByClass(RevenueCalculation.class);

        ChainMapper.addMapper(job, TripReconstructionMapper.class, LongWritable.class, Text.class, IntWritable.class, Segment.class, new Configuration(false));
        ChainReducer.setReducer(job, TripReconstructionReducer.class, IntWritable.class, Segment.class, NullWritable.class, Segment.class, new Configuration(false));
        job.setNumReduceTasks(50);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        DirDeleter.deleteDirectory(new File(output.toString()));
        return job;
    }

    /**
     * Function which creates the revenue calculation over time job
     *
     * The revenue calculation job is a Hadoop job which takes a text file generated by the trip reconstruction Hadoop job
     * as input and outputs a text file with the generated revenue over time
     *
     * @param input Path of the input file
     * @param output Path of the output directory
     * @return Hadoop job
     */
    public static Job revenueCalcOverTimeJob(Path input, Path output) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "revenue-calculation");
        job.setJarByClass(RevenueCalculation.class);

        ChainMapper.addMapper(job, TripFilterRevenueOverTime.class, LongWritable.class, Text.class, Text.class, DoubleWritable.class, new Configuration(false));
        job.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        DirDeleter.deleteDirectory(new File(output.toString()));
        return job;
    }

    /**
     * Function which creates the revenue calculation job
     *
     * The revenue calculation job is a Hadoop job which takes a text file generated by the trip reconstruction Hadoop job
     * as input and outputs a text file with the total generated revenue by airport trips
     *
     * @param input Path of the input file
     * @param output Path of the output directory
     * @return Hadoop job
     */
    public static Job revenueCalcJob(Path input, Path output) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "revenue-calculation");
        job.setJarByClass(RevenueCalculation.class);

        ChainMapper.addMapper(job, TripFilter.class, LongWritable.class, Text.class, NullWritable.class, Segment.class, new Configuration(false));
        ChainReducer.setReducer(job, RevenueCalculationReducer.class, NullWritable.class, Segment.class, NullWritable.class, DoubleWritable.class, new Configuration(false));
        job.setNumReduceTasks(50);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        DirDeleter.deleteDirectory(new File(output.toString()));
        return job;
    }

    /**
     * Main function which executes the program of RevenueCalculation
     * @param args String[] strings of input and output paths
     */
    public static void main(String[] args) throws Exception {

        Job reconstructionJob = tripReconstructionJob(new Path(args[0]), new Path(args[1]));
        if (!reconstructionJob.waitForCompletion(true)) {
            System.exit(1);
        }
        System.out.println("Trips Reconstructed");

        Job revenueCalcJob = revenueCalcJob(new Path(args[1]), new Path(args[2]));
        if (!revenueCalcJob.waitForCompletion(true)) {
            System.exit(1);
        }
        System.out.println("Total Revenue Calculated");

        Job revenueCalcOverTimeJob = revenueCalcOverTimeJob(new Path(args[1]), new Path(args[3]));
        if (!revenueCalcOverTimeJob.waitForCompletion(true)) {
            System.exit(1);
        }
        System.out.println("Revenue over time Calculated");
    }
}
