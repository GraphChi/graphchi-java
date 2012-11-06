package com.twitter.pers.bipartite;

import com.twitter.pers.Experiment;
import com.twitter.pers.multicomp.ComputationInfo;
import edu.cmu.graphchi.ChiEdge;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.FloatPair;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.util.HugeFloatMatrix;
import edu.cmu.graphchi.util.IdFloat;
import edu.cmu.graphchi.util.Toplist;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

/**
 *  SALSA but on a generic graph. Compute a separate left-value (hub)
 *  and a right-value (authority) for each vertex.
 *  ANNOYINGLY, this is now reverse siding to the bipartite-versions... Sorry
 */
public class GenericGraphSALSA implements GraphChiProgram<Boolean, Float> {

    HugeFloatMatrix scores;

    public GenericGraphSALSA(int numVertices) {
        scores = new HugeFloatMatrix(numVertices, 2, 1.0f);
    }


    // Bulk synchronous
    @Override
    public void update(ChiVertex<Boolean, Float> vertex, GraphChiContext context) {
        int side = (context.getIteration() % 2);

        float totalWeight = 0.0f;
        int nEdges =  (side == 0 ? vertex.numOutEdges() : vertex.numInEdges());
        for(int i=0; i < nEdges; i++) {
            ChiEdge<Float> edge = (side == 0 ? vertex.outEdge(i) : vertex.inEdge(i));
            totalWeight += edge.getValue();
        }
        if (totalWeight > 0) {
            float myScore = scores.getValue(vertex.getId(), side);
            for(int i=0; i < nEdges; i++) {
                ChiEdge<Float> edge = (side == 0 ? vertex.outEdge(i) : vertex.inEdge(i));
                scores.add(edge.getVertexId(), 1 - side, myScore * edge.getValue() / totalWeight);
            }
        }  else {
            scores.setValue(vertex.getId(), side, 0.0f);
        }

    }

    public void beginIteration(GraphChiContext ctx) {
        int side = (ctx.getIteration() % 2);

        // Zero-out the other side
        scores.setColumn(1 - side, 0.0f);
    }

    public void endIteration(GraphChiContext ctx) {
    }

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    public static void main(String[] args) throws Exception {
        Experiment experiment = new Experiment(args[0]);
        GraphChiEngine<Boolean, Float> engine = new GraphChiEngine<Boolean, Float>(experiment.getGraph(), experiment.getNumShards());
        engine.setVertexDataConverter(null);
        engine.setEdataConverter(new FloatConverter());
        engine.setModifiesInedges(false);
        engine.setModifiesOutedges(false);

        GenericGraphSALSA salsa = new GenericGraphSALSA(engine.numVertices());
        engine.run(salsa, 2 * experiment.getNumIterations());   // Twice the iterations, alternating left and right
        salsa.outputResults(experiment);
    }

    protected void outputResults(Experiment exp) throws IOException {
        /* Output top-lists */
        int ntop = 10000;
        TreeSet<IdFloat> topList = Toplist.topList(scores, 1, ntop);
        HashMap<String, String> placeholders = new HashMap<String, String>();
        placeholders.put("algo", "generic-salsa");

        String outputfile = exp.getOutputName(placeholders);
        BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputfile)));

        for(IdFloat item : topList) {
            writer.write(item.getVertexId() + "\t" + exp.getProperty("topic") + "\t" + item.getValue() +"\n");
        }
        writer.close();
    }

}
