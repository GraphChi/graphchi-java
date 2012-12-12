package com.twitter.pers.pig;

import edu.cmu.graphchi.aggregators.ForeachCallback;
import edu.cmu.graphchi.aggregators.VertexAggregator;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.auxdata.VertexData;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.util.UDFContext;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class PagerankPigUDF extends LoadFunc {

    private String hdfsDir;
    private boolean run = false;
    protected RecordReader in = null;
    private int numShards = 16;
    private ArrayList<Tuple> results = new ArrayList<Tuple>();
    int resultIdx = 0;

    public PagerankPigUDF() {
        this.numShards = 8;
    }



    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PigTextInputFormat();  //To change body of implemented methods use File | Settings | File Templates.
    }



    @Override
    public void setLocation(String location, Job job)
            throws IOException {
        System.out.println("Set location: " + location);
        System.out.println("Job: " + job);
        PigTextInputFormat.setInputPaths(job, location);
    }


    @Override
    public void prepareToRead(RecordReader recordReader, PigSplit pigSplit) throws IOException {
        this.in = recordReader;

        try {

            FastSharder sharder = new FastSharder("pigudf", numShards, 4);

            long i = 0;
            // Do the reading now
            while (in.nextKeyValue()) {
                Text value = (Text) in.getCurrentValue();
                byte[] buf = value.getBytes();
                String[] tok = new String(buf).split("\t");
                if (tok.length == 2) {
                    i++;
                    if (i % 40000 == 0) System.out.println(i);
                    sharder.addEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]));
                }
            }
            sharder.process();

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
