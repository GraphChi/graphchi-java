package edu.cmu.graphchi.apps.pig;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.FloatPair;
import edu.cmu.graphchi.datablocks.FloatPairConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.hadoop.PigGraphChiBase;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import edu.cmu.graphchi.vertexdata.VertexIdValue;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * Example application:  WeightedPageRank (http://en.wikipedia.org/wiki/Pagerank)
 * Iteratively computes a pagerank for each vertex by averaging the pageranks
 * of in-neighbors pageranks. Uses edge weights to implemented weighted version of pagerank.
 *
 * This version can be used with <a href="http://pig.apache.org">Pig</a> in a Hadoop cluster.
 *
 * Example PIG script for running this:
 *
 * <pre>
 *     REGISTER graphchi-java-0.2-jar-with-dependencies.jar;
 *
 *     pagerank = LOAD 'graphs/soc-LiveJournal1.txt' USING edu.cmu.graphchi.demo.pig.PigPagerank as (vertex:int, rank:float);
 *
 *     STORE pagerank INTO 'pagerank-livejournal';
 * </pre>
 *
 * (To get the livejournal graph, visit: http://snap.stanford.edu/data/soc-LiveJournal1.html)
 *
 * @see edu.cmu.graphchi.hadoop.PigGraphChiBase
 * @author Jerry Ye
 */
public class PigWeightedPagerank extends PigGraphChiBase implements GraphChiProgram<Float, FloatPair> {

    private static Logger logger = Logger.getLogger("weighted_pagerank");

    public void update(ChiVertex<Float, FloatPair> vertex, GraphChiContext context)  {
        if (context.getIteration() == 0) {
            /* Initialize on first iteration */
            vertex.setValue(1.0f);
        } else {
            /* On other iterations, set my value to be the weighted
               average of my in-coming neighbors pageranks.
             */
            float sum = 0.f;
            for(int i=0; i<vertex.numInEdges(); i++) {
                sum += vertex.inEdge(i).getValue().second;
            }
            vertex.setValue(0.15f + 0.85f * sum);
        }

        /* Accumulate edge weights */
        float edgeWeightSum = 0.f;
        for(int i=0; i<vertex.numOutEdges(); i++) {
            edgeWeightSum += vertex.outEdge(i).getValue().first;
        }

        /* Write my value (divided by my out-degree) to my out-edges so neighbors can read it. */
        for(int i=0; i<vertex.numOutEdges(); i++) {
            FloatPair curValue = vertex.outEdge(i).getValue();
            float edgeWeight = vertex.outEdge(i).getValue().first;
            vertex.outEdge(i).setValue(new FloatPair(edgeWeight, vertex.getValue() * edgeWeight/edgeWeightSum));
        }

    }

    /**
     * Callbacks (not needed for Pagerank)
     */
    public void beginIteration(GraphChiContext ctx) {}
    public void endIteration(GraphChiContext ctx) {}
    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}
    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}
    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {}
    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {}


    /**
     * PIG integration
     */

    // Objects needed for iterating the results
    private Iterator<VertexIdValue<Float>> vertexIterator;


    @Override
    /**
     * Pig column names
     */
    protected String getSchemaString() {
        return "(vertex:int, weight:float)";
    }

    @Override
    protected int getNumShards() {
        return 12;  // Unfortunately, currently hard-coded.
    }

    @Override
    /**
     * Runs the GraphChi program
     */
    protected void runGraphChi() throws Exception {
        /* Run GraphChi */
        GraphChiEngine<Float, FloatPair> engine = new GraphChiEngine<Float, FloatPair>(getGraphName(), getNumShards());
        engine.setEdataConverter(new FloatPairConverter());
        engine.setVertexDataConverter(new FloatConverter());
        engine.setModifiesInedges(false); // Important optimization

        engine.run(this, 4);

        logger.info("Ready.");

        /* Create iterator for the vertex values */
        this.vertexIterator = VertexAggregator.vertexIterator(engine.numVertices(), getGraphName(), new FloatConverter(),
                engine.getVertexIdTranslate());
    }

    @Override
    /**
     * Constructs "sharder", which takes an edge list and
     * converts it to internal binary representation of GraphChi.
     */
    protected FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Float, FloatPair>(graphName, numShards, new VertexProcessor<Float>() {
            public Float receiveVertexValue(int vertexId, String token) {
                return (token == null ? 0.0f : Float.parseFloat(token));
            }
        }, new EdgeProcessor<FloatPair>() {
            public FloatPair receiveEdge(int from, int to, String token) {
                return new FloatPair(Float.parseFloat(token), 0.f);
            }
        }, new FloatConverter(), new FloatPairConverter());
    }

    @Override
    /**
     * Generates the output to the Pig script, tuple by tuple
     */
    protected Tuple getNextResult(TupleFactory tupleFactory) throws ExecException {
        if (vertexIterator.hasNext()) {
            Tuple t = tupleFactory.newTuple(2);
            VertexIdValue<Float> val = vertexIterator.next();
            t.set(0, val.getVertexId());
            t.set(1, val.getValue());
            return t;
        } else {
            return null;
        }
    }
}
