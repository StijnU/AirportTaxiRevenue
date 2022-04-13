import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Segment class to easily store/create instances of Segments
 * Implements the Writable interface, so it can be used by Hadoop for Map/Reduce operations
 *
 * @author Stijn Uytterhoeven
 * @version 1.0
 * @since 2022-04-10
 */
public class Segment implements Writable{
    private final double airportLong = -122.37896;
    private final double airportLat = 37.62131;
    private final double maxSpeed = 200.0;
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private int id;
    private double timeInHours;
    private double startLat;
    private double startLong;
    private double endLat;
    private double endLong;
    private boolean full;
    private boolean tooFast;
    private boolean closeToAirport;
    private Date startDate;

    private double distance;

    public Segment() {
    }

    private static double flatSurfaceDistanceInKilometers(double startLat, double startLong, double endLat, double endLong) {
        double coordToRad = Math.PI/180;
        return 6371.009 * Math.sqrt(Math.pow((startLat - endLat) * coordToRad, 2) + Math.pow(Math.cos((startLat + endLat) * coordToRad / 2) * (startLong - endLong) * coordToRad, 2));
    }

    /**
     * Creates a segment with the fields given as a String
     * @param ln String of segment fields
     */
    public Segment(String ln) throws ParseException {
        String[] lns = ln.split(",");
        this.id = Integer.parseInt(lns[0]);
        this.timeInHours = Double.parseDouble(lns[1]);
        this.startLat = Double.parseDouble(lns[2]);
        this.startLong = Double.parseDouble(lns[3]);
        this.endLat = Double.parseDouble(lns[4]);
        this.endLong = Double.parseDouble(lns[5]);
        this.full = Boolean.parseBoolean(lns[6]);
        this.tooFast = Boolean.parseBoolean(lns[7]);
        this.closeToAirport = Boolean.parseBoolean(lns[8]);
        this.startDate = sdf.parse(lns[9]);
        this.distance = calcDistance();

    }

    /**
     * Creates a new segment with the same properties as a given segment
     * @param s Segment
     */
    public Segment(Segment s) {
        this.id = s.getId();
        this.timeInHours = s.getTimeInHours();
        this.startLat = s.getStartLat();
        this.startLong = s.getStartLong();
        this.endLat = s.getEndLat();
        this.endLong = s.getEndLong();
        this.full = s.isFull();
        this.startDate = s.getStartDate();

        this.closeToAirport = this.isCloseTo(airportLong, airportLat);
        this.tooFast = this.getSpeed() > this.maxSpeed;
        this.distance = calcDistance();
    }

    /**
     * Creates a segment with given input data
     *
     * Note: The end status is chosen for the status for the whole segment, this means if the taxi finishes a trip halfway a segment
     * this half is not counted for the revenue. The revenue is thus a bottom value.
     *
     * @param id Int <taxi-id>
     * @param startDate Date <start date>
     * @param endDate Date <end date>
     * @param startLat Double <start pos (lat)>,
     * @param startLon Double <start pos (lat)>,
     * @param endLat Double <start pos (lat)>,
     * @param endLong Double <start pos (lat)>,
     * @param status Char <end status>
     */
    public Segment(int id, Date startDate, Date endDate, double startLat, double startLon, double endLat, double endLong, char status) {
        this.id = id;
        this.timeInHours = (endDate.getTime() - startDate.getTime()) / (double) 3_600_000;
        this.startLat = startLat;
        this.startLong = startLon;
        this.endLat = endLat;
        this.endLong = endLong;
        this.full = ('E' != status);
        this.startDate = startDate;

        this.closeToAirport = this.isCloseTo(airportLong, airportLat);
        this.tooFast = this.getSpeed() > 200;
        this.distance = calcDistance();

    }

    /**
     * Write function used by Hadoop Map/Reduce operations
     *
     * @param out DataOutput of the Segment fields
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeDouble(this.timeInHours);
        out.writeDouble(this.startLat);
        out.writeDouble(this.startLong);
        out.writeDouble(this.endLat);
        out.writeDouble(this.endLong);
        out.writeBoolean(this.full);
        out.writeBoolean(this.tooFast);
        out.writeBoolean(this.closeToAirport);
        out.writeUTF(sdf.format(this.startDate.getTime()));
        out.writeDouble(this.distance);
    }

    /**
     * Read function used by Hadoop Map/Reduce operations
     *
     * @param in DataInput of the Segment fields
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        this.timeInHours = in.readDouble();
        this.startLat = in.readDouble();
        this.startLong = in.readDouble();
        this.endLat = in.readDouble();
        this.endLong = in.readDouble();
        this.full = in.readBoolean();
        this.tooFast = in.readBoolean();
        this.closeToAirport = in.readBoolean();
        try {
            this.startDate = sdf.parse(in.readLine().substring(2));
            this.distance = in.readDouble();
        }
        catch(Exception ignored){}

    }

    /**
     * Function used to check if the segment rides close to a position with certain coordinates
     *
     * @param coordLong Double longitude coordinates of the position
     * @param coordLat Double latitude coordinates of the position
     * @return Boolean True if the segment is close to the position otherwise False
     */
    public boolean isCloseTo(double coordLong, double coordLat){
        // just like line through circle, this function does treat the long and lat coordinates as cartesian
        // 1: get function of line through 2 points
        double rico = (endLat - startLat) / (endLong - startLong);
        double b = -1;
        double c = startLat - startLong * rico;

        // function now is 0 = rico * x + b * y + c
        // now check if function goes through circle with middle point airport and range 1km (1/111 degrees)
        // this is done by calculating distance between circle middle-point and line

        double distance = Math.abs(rico * coordLong + b * coordLat + c) / Math.sqrt(Math.pow(rico, 2) + Math.pow(b, 2));
        return distance <= (double) 1 / 111;
    }

    /**
     * Calculates the average speed of the segment
     *
     * @return Double speed
     */
    public double getSpeed(){
        // calculates the haversine distance on sphere for given angle coordinates
        // Returns the Haversine distance in meters between two points specified in decimal degrees (latitude/longitude). This works correctly even if the dateline is between the two points.
        //Error is at most 4E-1 (40cm) from the actual haversine distance, but is typically much smaller for reasonable distances: around 1E-5 (0.01mm) for distances less than 1000km.
        double dist = flatSurfaceDistanceInKilometers(this.startLat, this.startLong, this.endLat, this.endLong);
        return dist/this.timeInHours;
    }

    /**
     * Returns the value of the distance of this segment
     *
     * @return Double distance
     */
    public double getDistance() {
        return this.distance;
    }

    /**
     * Cacluates the simple flat surface distance between the start and end coordinates
     *
     * @return Double distance
     */
    public double calcDistance(){
        return flatSurfaceDistanceInKilometers(this.startLat, this.startLong, this.endLat, this.endLong);
    }

    /**
     * Tries to combine an input segment with 'this' segment
     *
     * Combining 2 segments is in itself placing 2 segments next to each other and creating 1 large segment of the 2
     *
     * Combination is possible when:
     *      - the start coordinates of 'this' are the same as end coordinates of the input segment
     *      - the end coordinates of 'this' are the same as start coordinates of the input segment
     *      - and in both cases the segments have the same status
     *
     * When combined timeInHours is added up, start or end is changed to start or end of the input segment
     * An AND operation is taken from both tooFast values (if 1 segment is too fast, the whole trip should be invalid)
     * an OR operation is taken from both closeToAirport values (if 1 segment passes close to the airport, the trip should be valid)
     *
     * @param segment Segment the segment trying to comine
     * @return Boolean True if the segments are combined otherwise False
     */
    public boolean tryCombine(Segment segment){
        if (this.full == segment.isFull()){
            if (this.getEndLat() == segment.getStartLat() && this.getEndLong() == segment.getStartLong()){
                this.timeInHours += segment.getTimeInHours();
                this.endLat = segment.getEndLat();
                this.endLong = segment.getEndLong();
                this.tooFast = this.tooFast && segment.isTooFast();
                this.closeToAirport = this.closeToAirport || segment.isCloseToAirport();
                this.distance += segment.getDistance();
                return true;
            }
            else {
                if (this.getStartLat() == segment.getEndLat() && this.getStartLong() == segment.getEndLong()) {
                    this.timeInHours += segment.getTimeInHours();
                    this.startLat = segment.getStartLat();
                    this.startLong = segment.getStartLong();
                    this.tooFast = this.tooFast && segment.isTooFast();
                    this.closeToAirport = this.closeToAirport || segment.isCloseToAirport();
                    this.startDate = segment.getStartDate();
                    this.distance += segment.getDistance();
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Creates a string of the fields
     * @return String of the fields
     */
    public String toString(){
        return this.id + "," + this.timeInHours + "," + this.startLat + "," + this.startLong + "," + this.endLat + "," + this.endLong + "," + this.full + "," + this.tooFast + "," + this.closeToAirport + "," + sdf.format(this.startDate.getTime());
    }

    /**
     * get the tooFast field value
     * @return Boolean tooFast value
     * */
    public boolean isTooFast(){
        return this.tooFast;
    }

    /**
     * get the closeToAirport field value
     * @return Boolean closeToAirport value
     */
    public boolean isCloseToAirport(){
        return this.closeToAirport;
    }

    /**
     * get the id field value
     * @return Int id value
     */
    public int getId(){
        return this.id;
    }

    /**
     * get the start latitude coordinates
     * @return Double startLat value
     */
    public double getStartLat(){
        return this.startLat;
    }

    /**
     * get the start longitude coordinates
     * @return Double startLong value
     */
    public double getStartLong(){
        return this.startLong;
    }

    /**
     * get the end latitude coordinates
     * @return Double endLat value
     */
    public double getEndLat(){
        return this.endLat;
    }

    /**
     * get the end longitude coordinates
     * @return Double endLong value
     */
    public double getEndLong(){
        return this.endLong;
    }

    /**
     * get the full field value
     * @return Boolean full value
     */
    public boolean isFull(){
        return this.full;
    }

    /**
     * get the time of the segment in hours
     * @return Double timeInHours value
     */
    public double getTimeInHours(){
        return this.timeInHours;
    }

    /**
     * get the start date of the segment
     * @return Date start date of the segment
     */
    public Date getStartDate(){
        return this.startDate;
    }
}

