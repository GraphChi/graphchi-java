package com.twitter.pers.pig;

import edu.cmu.graphchi.LoggingInitializer;
import edu.cmu.graphchi.aggregators.ForeachCallback;
import edu.cmu.graphchi.aggregators.VertexAggregator;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.auxdata.VertexData;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.HDFSGraphLoader;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 */
public class PagerankPigUDF extends LoadFunc implements LoadMetadata {

    private String hdfsDir;
    private boolean run = false;
    protected RecordReader in = null;
    private int numShards = 16;
    private ArrayList<Tuple> results = new ArrayList<Tuple>();
    int resultIdx = 0;
    private String location;

    private static Logger logger = LoggingInitializer.getLogger("pagerank-pig");

    public PagerankPigUDF() {
        this.numShards = 8;
    }


    @Override
    public ResourceSchema getSchema(String str, Job job) throws IOException {
        // Utils.getSchemaFromString("(b:bag{f1: chararray, f2: int})");
        System.out.println("getSchema: " + str);
        ResourceSchema s = new ResourceSchema(Utils.getSchemaFromString("(vertex:int, value:float)"));
        return s;
    }

    @Override
    public ResourceStatistics getStatistics(String s, Job job) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String[] getPartitionKeys(String s, Job job) throws IOException {
        System.out.println("Get partition keys: " + s);
        System.out.println(job);
        return null; // Disable partition
    }

    @Override
    public void setPartitionFilter(Expression expression) throws IOException {
        System.out.println("setPartitionFilter: " + expression);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PigTextInputFormat();
    }



    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        System.out.println("Set location: " + location);
        System.out.println("Job: " + job);
        PigTextInputFormat.setInputPaths(job, location);
        this.location = location;
    }


    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        this.in = recordReader;

        try {

            int j = 0;
            for(String s : pigSplit.getLocations()) {
                System.out.println((j++) + "Split : " + s);
            }
            System.out.println("Num paths: " + pigSplit.getNumPaths());
            System.out.println("" + pigSplit.getConf());
            System.out.println("split index " + pigSplit.getSplitIndex());


            if (pigSplit.getSplitIndex() > 0) {
                throw new RuntimeException("Split index > 0");
            }

            final FastSharder sharder = new FastSharder("pigudf", numShards, 4);


            HDFSGraphLoader hdfsLoader = new HDFSGraphLoader(this.location, new EdgeProcessor() {
                @Override
                public void receiveEdge(int from, int to) {
                    try {
                        sharder.addEdge(from, to);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            hdfsLoader.load(pigSplit.getConf());

            sharder.process();

            System.out.println("Starting to run");

            edu.cmu.graphchi.apps.Pagerank.runForPig("pigudf", numShards);

            /* Run graph chi */
            final VertexIdTranslate vertexIdTranslate = edu.cmu.graphchi.apps.Pagerank.runForPig("pigudf", numShards);

            final TupleFactory tupleFactory = TupleFactory.getInstance();
            VertexAggregator.foreach("pigudf", new FloatConverter(), new ForeachCallback<Float>() {
                @Override
                public void callback(int vertexId, Float vertexValue) {
                    try {
                        Tuple t = tupleFactory.newTuple(2);
                        t.set(0, vertexIdTranslate.backward(vertexId));
                        t.set(1, vertexValue);
                        results.add(t);
                    } catch (Exception err) {
                        throw new RuntimeException(err);
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Tuple getNext() throws IOException {
        if (results.size() == resultIdx) return null;
        return results.get(resultIdx++);
    }


}
