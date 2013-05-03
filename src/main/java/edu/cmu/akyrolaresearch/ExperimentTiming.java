package edu.cmu.akyrolaresearch;

import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.walks.DrunkardMobEngine;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;


/**
 * Awful code
 * User: akyrola
 * Date: 5/3/13; time: 2:17 PM
 * Carnegie Mellon Univ, 2013
 */
public class ExperimentTiming {

    private String host;
    private int maxMemory;

    private int numSources;
    private int firstSource;
    private int walksPerSource;
    private String sourceSelectionType;

    private double runTimeTotal;
    private ArrayList<Double> iterationRuntimes = new ArrayList<Double>();
    private String companionAddress;

    private boolean inEdges = false;
    private boolean weighted = false;

    private String graphName;
    private long numEdges;
    private int numVertices;
    private double initTime;


    public ExperimentTiming() {

    }

    public void configure(DrunkardMobEngine drunkardEngine) {
        GraphChiEngine engine = drunkardEngine.getEngine();
        setNumEdges(engine.numEdges());
        setNumVertices(engine.numVertices());

        setInEdges(!engine.isDisableInEdges());
        setWeighted(!engine.isOnlyAdjacency());
        setMaxMemory((int) (Runtime.getRuntime().maxMemory() / 1024 / 1024));
        try {
            setHost(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static void insert(ExperimentTiming et) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            Connection connect = DriverManager
                    .getConnection("jdbc:mysql://multi6.aladdin.cs.cmu.edu/drunkard?"
                            + "user=drunkard&password=jeejee");
            PreparedStatement pstmt = connect.prepareStatement("insert into drunkardrun(max_memory, num_sources,"
            + "first_source,walks_per_source, source_selection_type, runtime, inittime, time_0, time_1, time_2, time_3, time_4, time_5," +
                    "companion,inedges,weighted,graph,numedges,numvertices, host) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

            int k = 1;
            pstmt.setInt(k++, et.getMaxMemory());
            pstmt.setInt(k++, et.getNumSources());
            pstmt.setInt(k++, et.getFirstSource());
            pstmt.setInt(k++, et.getWalksPerSource());
            pstmt.setString(k++, "consequtive");
            pstmt.setDouble(k++, et.getRunTimeTotal());
            pstmt.setDouble(k++, et.getInitTime());
            for(int i=0; i<=5; i++) {
                if (et.getIterationRuntimes().size() > i) {
                    pstmt.setDouble(k++, et.getIterationRuntimes().get(i));
                } else {
                    pstmt.setDouble(k++, 0);
                }
            }
            pstmt.setString(k++, et.getCompanionAddress());
            pstmt.setBoolean(k++, et.isInEdges());
            pstmt.setBoolean(k++, et.isWeighted());
            pstmt.setString(k++, et.getGraphName());
            pstmt.setLong(k++, et.getNumEdges());
            pstmt.setLong(k++, et.getNumVertices());
            pstmt.setString(k++, et.getHost());

            pstmt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(int maxMemory) {
        this.maxMemory = maxMemory;
    }

    public double getInitTime() {
        return initTime;
    }

    public void setInitTime(double initTime) {
        this.initTime = initTime;
    }

    public int getNumSources() {
        return numSources;
    }


    public void setNumSources(int numSources) {
        this.numSources = numSources;
    }

    public int getFirstSource() {
        return firstSource;
    }

    public void setFirstSource(int firstSource) {
        this.firstSource = firstSource;
    }

    public int getWalksPerSource() {
        return walksPerSource;
    }

    public void setWalksPerSource(int walksPerSource) {
        this.walksPerSource = walksPerSource;
    }

    public String getSourceSelectionType() {
        return sourceSelectionType;
    }

    public void setSourceSelectionType(String sourceSelectionType) {
        this.sourceSelectionType = sourceSelectionType;
    }

    public double getRunTimeTotal() {
        return runTimeTotal;
    }

    public void setRunTimeTotal(double runTimeTotal) {
        this.runTimeTotal = runTimeTotal;
    }

    public ArrayList<Double> getIterationRuntimes() {
        return iterationRuntimes;
    }

    public void setIterationRuntimes(ArrayList<Double> iterationRuntimes) {
        this.iterationRuntimes = iterationRuntimes;
    }

    public String getCompanionAddress() {
        return companionAddress;
    }

    public void setCompanionAddress(String companionAddress) {
        this.companionAddress = companionAddress;
    }

    public boolean isInEdges() {
        return inEdges;
    }

    public void setInEdges(boolean inEdges) {
        this.inEdges = inEdges;
    }

    public boolean isWeighted() {
        return weighted;
    }

    public void setWeighted(boolean weighted) {
        this.weighted = weighted;
    }

    public String getGraphName() {
        return graphName;
    }

    public void setGraphName(String graphName) {
        this.graphName = graphName;
    }

    public long getNumEdges() {
        return numEdges;
    }

    public void setNumEdges(long numEdges) {
        this.numEdges = numEdges;
    }

    public int getNumVertices() {
        return numVertices;
    }

    public void setNumVertices(int numVertices) {
        this.numVertices = numVertices;
    }
}
