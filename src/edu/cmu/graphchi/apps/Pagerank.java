package edu.cmu.graphchi.apps;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;

/**
 * @author akyrola
 *         Date: 7/11/12
 */
public class Pagerank implements GraphChiProgram<Float, Float> {


    public void update(ChiVertex<Float, Float> vertex, GraphChiContext context) {
        if (context.getIteration() == 0) {
            vertex.setValue(1.0f);
        } else {
            float sum = 0.f;
            for(int i=0; i<vertex.numInEdges(); i++) {
                sum += vertex.inEdge(i).getValue();
            }
            vertex.setValue(0.15f + 0.85f * sum);
        }

        float outValue = vertex.getValue() / vertex.numOutEdges();
        for(int i=0; i<vertex.numOutEdges(); i++) {
            vertex.outEdge(i).setValue(outValue);
        }

        if (vertex.getId() % 100000 == 0 || vertex.getId() == 8737 || vertex.getId() == 2914) {
            System.out.println(vertex.getId() + " => " + vertex.getValue());
        }
    }


    public void beginIteration(GraphChiContext ctx) {}

    public void endIteration(GraphChiContext ctx) {}

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

    public static void main(String[] args) throws  Exception {
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);

        GraphChiEngine<Float, Float> engine = new GraphChiEngine<Float, Float>(baseFilename, nShards);
        engine.setEdataConverter(new FloatConverter());
        engine.setVertexDataConverter(new FloatConverter());
        engine.run(new Pagerank(), 5);

        System.out.println("Ready.");
    }
}
