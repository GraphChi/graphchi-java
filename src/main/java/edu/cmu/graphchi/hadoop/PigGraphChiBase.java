package edu.cmu.graphchi.hadoop;

import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Special PIG loader that wraps a graphchi application. This
 * allows execution of GraphChi programs under Hadoop/PIG.
 *
 * Generally, if you have an GraphChi application extending PigGraphChibase,
 * you can use it by calling in PIG:
 *   results = LOAD '$GRAPH' USING  my.app.GraphChiApp() as (...)
 *
 *  Above, $GRAPH is a path where you have stored a file in edge list format:
 *   mydata = FOREACH mygraph GENERATE from_id, to_id, edge_value;
 *   STORE mydata INTO '$GRAPH'
 *
 * For an example, see
 * @see edu.cmu.graphchi.apps.pig.PigPagerank
 */
public abstract class PigGraphChiBase  extends LoadFunc implements LoadMetadata {

    private static final Logger logger = ChiLogger.getLogger("pig-graphchi-base");
    private String location;
    private boolean activeNode = false;
    private Job job;
    private boolean ready = false;

    protected PigGraphChiBase() {
    }

    // Example: (vertex:int, value:float)"
    protected abstract String getSchemaString();

    @Override
    public ResourceSchema getSchema(String str, Job job) throws IOException {
        return null;
    }

    @Override
    public ResourceStatistics getStatistics(String s, Job job) throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String s, Job job) throws IOException {
        return null; // Disable partition
    }

    @Override
    public void setPartitionFilter(Expression expression) throws IOException {
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PigTextInputFormat();
    }

    protected abstract int getNumShards();

    protected String getGraphName() {
        return "pigudfgraph";
    }

    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        logger.info("Set location: " + location);
        logger.info("Job: " + job);
        PigTextInputFormat.setInputPaths(job, location);
        this.location = location;
        this.job = job;
    }


    protected abstract void runGraphChi() throws Exception;

    protected abstract FastSharder createSharder(String graphName, int numShards) throws IOException;

    @Override
    public void prepareToRead(final RecordReader recordReader, final PigSplit pigSplit) throws IOException {

        try {

            int j = 0;
            for(String s : pigSplit.getLocations()) {
                System.out.println((j++) + "Split : " + s);
            }
            System.out.println("Num paths: " + pigSplit.getNumPaths());
            System.out.println("" + pigSplit.getConf());
            System.out.println("split index " + pigSplit.getSplitIndex());


            Thread progressThread = new Thread(new Runnable() {
                public void run() {
                    int i = 0;
                    while(!ready) {
                        PigStatusReporter.getInstance().progress();
                        PigStatusReporter.getInstance().setStatus("Status idx: " + i++);
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException ioe) {}
                    }

                }

            });
            progressThread.start();

            if (pigSplit.getSplitIndex() > 0) {
                throw new RuntimeException("Split index > 0 -- this mapper will die (expected, not an error).");
            }

            activeNode = true;

            Thread chiThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        final FastSharder sharder = createSharder(getGraphName(), getNumShards());

                        HDFSGraphLoader hdfsLoader = new HDFSGraphLoader(location, new EdgeProcessor<Float>() {
                            @Override
                            public Float receiveEdge(int from, int to, String token) {
                                try {
                                    sharder.addEdge(from, to, token);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                                return null;
                            }
                        });

                        hdfsLoader.load(pigSplit.getConf());
                        sharder.process();

                        logger.info("Starting to run");
                        runGraphChi();
                        logger.info("Ready");
                    } catch (Exception err) {
                        err.printStackTrace();
                    }
                    ready = true;
                }});
            chiThread.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected String getStatusString() {
        return "Still running: " + System.currentTimeMillis();
    }

    protected abstract Tuple getNextResult(TupleFactory tupleFactory) throws ExecException;

    @Override
    public Tuple getNext() throws IOException {
        if (!activeNode) return null;
        while (!ready) {
            logger.info("Still waiting in getNext()");
            PigStatusReporter.getInstance().setStatus(getStatusString());
            PigStatusReporter.getInstance().progress();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ioe) {
            }
        }
        return getNextResult(TupleFactory.getInstance());
    }

}
