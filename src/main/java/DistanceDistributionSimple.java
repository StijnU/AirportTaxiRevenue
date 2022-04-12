import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * The DistanceDistributionSimple program calculates the distributions of the trips in a simple manner.
 *
 * @author Stijn Uytterhoeven
 * @version 1.0
 * @since 2022-04-10
 */
public class DistanceDistributionSimple {

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
     * Function which executes the DistanceDistributionSimple program
     *
     * @param args String[] input and output files
     */
    public static void main(String[] args) throws IOException {
        String input = args[0];
        String output = args[1];

        ArrayList<Long> allRuns = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            long start = System.currentTimeMillis();
            //Read input file
            File file = new File(input);
            BufferedReader in = new BufferedReader(new FileReader(file));

            //Init hashmap to store distances
            HashMap<Double, Integer> dists = new HashMap<>();

            //Read lines from input file and store them in the hashmap
            String ln;
            while ((ln = in.readLine()) != null) {
                //Get data from input file
                String[] parsed = ln.split(" ");
                double startLat = Double.parseDouble(parsed[2]);
                double startLong = Double.parseDouble(parsed[3]);
                double endLat = Double.parseDouble(parsed[5]);
                double endLong = Double.parseDouble(parsed[6]);

                //Calculate distance and check if distance already in hashmap
                //if True then add up the value of the distance in hashmap, else put in new pair (distance, 1)
                Integer val;
                Double dist = flatSurfaceDistanceInKilometers(startLat, startLong, endLat, endLong);
                if ((val = dists.putIfAbsent(dist, 1)) != null) {
                    dists.put(dist, val + 1);
                }
            }
            long end = System.currentTimeMillis();
            System.out.println((end - start));
            allRuns.add((end - start));
            // write outputs
            FileWriter wr = new FileWriter(output);
            for (Double dist : dists.keySet()) {
                ln = dist + " " + dists.get(dist) + "\n";
                wr.write(ln);
            }
            wr.close();

        }

        long sum = 0;
        for (long run : allRuns){
            sum += run;
        }
        System.out.println("mean: "+sum/allRuns.size());
    }
}
