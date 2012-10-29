package com.twitter.pers.bipartite;

import com.twitter.pers.multicomp.ComputationInfo;
import com.yammer.metrics.Metrics;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.metrics.SimpleMetricsReporter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * SALSA - stochastic version of HITS algorithm.
 * Power-iteration implementation of Lempel, Moran (2000).
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class SALSA extends BipartiteHubsAndAuthorities {

    protected SALSA(List<ComputationInfo> computations, int maxLeftVertex, int maxRightVertex, float  cutOff)
            throws IOException {
        super(computations, maxLeftVertex, maxRightVertex, cutOff);
    }

    @Override
    public void update(ChiVertex<Float, Float> vertex, GraphChiContext context) {

        boolean isLeft = vertex.getId() < RIGHTSIDE_MIN;

        if (context.getIteration() == context.getNumIterations() - 1) {
            // Last iteration
            if (isLeft) {
                if (vertex.getId() < leftWeightMatrix.getNumRows()) {
                    for(int compId=0; compId < numComputations; compId++) {
                        leftScoreMatrix.setValue(vertex.getId(), compId, leftScoreMatrix.getValue(vertex.getId(), compId) * vertex.numEdges());
                    }
                }
            }
            return;
        }

        for(int compId=0; compId < numComputations; compId++) {
            if (isLeft) {
                if (vertex.getId() >= leftWeightMatrix.getNumRows()) {
                    continue;
                }
                /* Left side */
                float sumRight = 0.0f;
                float myWeight = leftWeightMatrix.getValue(vertex.getId(), compId);

                if (myWeight > 0.0f) {
                    if (context.getIteration() > 0) {
                    for(int e=0; e < vertex.numEdges(); e++) {
                        sumRight += rightScoreMatrix.getValue(vertex.edge(e).getVertexId() - RIGHTSIDE_MIN, compId);
                    }
                    } else {
                        sumRight = 1.0f;
                    }
                    if (sumRight > 0)
                        sumRight /= vertex.numEdges();
                    sumRight *= myWeight;
                }
                leftScoreMatrix.setValue(vertex.getId(), compId, sumRight);
                if (vertex.getId() == 242 || vertex.getId() == 18273 || vertex.getId() == 834621) {
                    System.out.println(vertex.getId() + " --> " + sumRight + ", " + vertex.numEdges());
                }
            }  else {
                if (vertex.getId() - RIGHTSIDE_MIN >= rightScoreMatrix.getNumRows()) {
                    continue;
                }
                float sumLeft = 0.0f;
                int maxLeft = (int) leftWeightMatrix.getNumRows();
                float normalizer = 0f;
                for(int e=0; e < vertex.numEdges(); e++) {
                    int nbId = vertex.edge(e).getVertexId();
                    if (nbId < maxLeft) {
                        float weight = leftWeightMatrix.getValue(nbId, compId);
                        sumLeft += leftScoreMatrix.getValue(nbId, compId);
                        normalizer += weight;
                    }

                }
                if (normalizer > 0) sumLeft /= normalizer;
                rightScoreMatrix.setValue(vertex.getId() - RIGHTSIDE_MIN, compId, sumLeft);

                if (vertex.getId() % 1000 == 1 && sumLeft > 0) {
                    System.out.println(vertex.getId() - RIGHTSIDE_MIN + " ***> " + sumLeft + " norm: " + normalizer);
                }
            }
        }
    }



    @Override
    public void endIteration(GraphChiContext ctx) {
        // No normalizing
        System.out.println("SALSA: ended iteration " + ctx.getIteration());
    }


    public static void main(String[] args) throws Exception {
        SimpleMetricsReporter rep = SimpleMetricsReporter.enable(2, TimeUnit.MINUTES);

        int k=0;
        String graph = args[k++];
        int nshards = Integer.parseInt(args[k++]);
        int niters = Integer.parseInt(args[k++]);
        String inputFile = args[k++];
        float cutOff = Float.parseFloat(args[k++]);

        /* Initialize computations */
        List<ComputationInfo> computations = ComputationInfo.loadComputations(inputFile);

        /* Find the maximum vertex id on left-side by looking at the first non-zero degree */
        int leftMax = findApproxMaximumLeftVertex(graph);

        GraphChiEngine<Float, Float> engine = new GraphChiEngine<Float, Float>(graph, nshards);
        SALSA salsa = initializeApp(cutOff, computations, leftMax, engine);

        engine.setOnlyAdjacency(true);
        engine.setAutoLoadNext(true);
        engine.setModifiesInedges(false);
        engine.setModifiesOutedges(false);
        engine.setEnableDeterministicExecution(false);
        engine.setEdataConverter(null);
        engine.setVertexDataConverter(null);
        engine.run(salsa, niters);


        outputResults(salsa, cutOff, computations, "salsa");

        /* Report metrics */
        Metrics.shutdown();
        rep.run();
    }


    protected static SALSA initializeApp(float cutOff, List<ComputationInfo> computations, int leftMax, GraphChiEngine engine) throws IOException {
        return new SALSA(computations,
                leftMax, engine.numVertices(), cutOff);
    }

}
