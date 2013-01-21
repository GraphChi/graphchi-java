package edu.cmu.graphchi.apps;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;

import java.util.TreeSet;

/**
 * Tests that the system works.
 * @author akyrola
 *         Date: 7/11/12
 */
public class SmokeTest implements GraphChiProgram<Integer, Integer> {


    public void update(ChiVertex<Integer, Integer> vertex, GraphChiContext context) {
        if (context.getIteration() == 0) {
            vertex.setValue(vertex.getId() + context.getIteration());
        } else {
            int curval = vertex.getValue();
            int vexpected = vertex.getId() + context.getIteration() - 1;
            if (curval != vexpected) {
            	throw new RuntimeException("Mismatch (vertex). Expected: " + vexpected + " but had " +
               curval);
            		
            }
            for(int i=0; i<vertex.numInEdges(); i++) {
                 int has = vertex.inEdge(i).getValue();
                 int correction = vertex.getId() > vertex.inEdge(i).getVertexId() ? +1 : 0;
                 int expected = vertex.inEdge(i).getVertexId() + context.getIteration() - 1 + correction;
                 if (expected != has) 
                	 throw new RuntimeException("Mismatch (edge): " + expected + " expected but had "+ has +
                			 	". Iteration:" + context.getIteration() + ", edge:" + vertex.inEdge(i).getVertexId() 
                			 		+ " -> " + vertex.getId());
            }
            vertex.setValue(vertex.getId() + context.getIteration());

        }
         int val = vertex.getValue();
         for(int i=0; i<vertex.numOutEdges(); i++) {
            vertex.outEdge(i).setValue(val);
         }
    }


    public void beginIteration(GraphChiContext ctx) {}

    public void endIteration(GraphChiContext ctx) {}

    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {}

    public void endInterval(GraphChiContext ctx, VertexInterval interval) {}

    public static void main(String[] args) throws  Exception {
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);

        GraphChiEngine<Integer, Integer> engine = new GraphChiEngine<Integer, Integer>(baseFilename, nShards);
        engine.setEdataConverter(new IntConverter());
        engine.setVertexDataConverter(new IntConverter());
        engine.setModifiesInedges(false); // Important optimization
        engine.run(new SmokeTest(), 5);

        System.out.println("Ready.");

    }
}
