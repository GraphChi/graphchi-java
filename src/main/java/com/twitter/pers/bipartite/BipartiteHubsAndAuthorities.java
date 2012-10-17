package com.twitter.pers.bipartite;

import com.twitter.pers.multicomp.ComputationInfo;
import com.twitter.pers.multicomp.WeightUtil;
import com.yammer.metrics.Metrics;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.engine.auxdata.DegreeData;
import edu.cmu.graphchi.metrics.SimpleMetricsReporter;
import edu.cmu.graphchi.util.HugeFloatMatrix;
import edu.cmu.graphchi.util.IdFloat;
import edu.cmu.graphchi.util.Toplist;

import java.io.*;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class BipartiteHubsAndAuthorities implements GraphChiProgram<Float, Float> {

    public final static int RIGHTSIDE_MIN = 2000000000;

    private HugeFloatMatrix leftWeightMatrix;
    private HugeFloatMatrix leftScoreMatrix;
    private HugeFloatMatrix rightScoreMatrix;
    int numComputations;

    private BipartiteHubsAndAuthorities(List<ComputationInfo> computations, int maxLeftVertex, int maxRightVertex, float  cutOff)
            throws IOException {
        numComputations = computations.size();

        leftWeightMatrix = new HugeFloatMatrix(maxLeftVertex + 1, numComputations);
        leftScoreMatrix = new HugeFloatMatrix(maxLeftVertex + 1, numComputations, 1.0f);
        rightScoreMatrix = new HugeFloatMatrix(maxRightVertex - RIGHTSIDE_MIN + 1, numComputations, 1.0f);


        for(ComputationInfo compInfo : computations) {
            System.out.println("Loading weights: " + compInfo);
            WeightUtil.loadWeights(compInfo, leftWeightMatrix, cutOff);
        }
    }

    @Override
    public void update(ChiVertex<Float, Float> vertex, GraphChiContext context) {

        boolean isLeft = vertex.getId() < RIGHTSIDE_MIN;

        for(int compId=0; compId < numComputations; compId++) {
            if (isLeft) {
                if (vertex.getId() >= leftWeightMatrix.getNumRows()) {
                    continue;
                }
                /* Left side */
                float sumRight = 0.0f;
                float myWeight = leftWeightMatrix.getValue(vertex.getId(), compId);

                if (myWeight > 0.0f) {
                    for(int e=0; e < vertex.numEdges(); e++) {
                        sumRight += rightScoreMatrix.getValue(vertex.edge(e).getVertexId() - RIGHTSIDE_MIN, compId);
                    }
                    sumRight *= myWeight;
                }
                leftScoreMatrix.setValue(vertex.getId(), compId, sumRight);
            }  else {
                if (vertex.getId() - RIGHTSIDE_MIN >= rightScoreMatrix.getNumRows()) {
                    continue;
                }
                float sumLeft = 0.0f;
                for(int e=0; e < vertex.numEdges(); e++) {
                    int nbId = vertex.edge(e).getVertexId();
                    sumLeft += leftWeightMatrix.getValue(nbId, compId) * leftScoreMatrix.getValue(nbId, compId);
                }
                rightScoreMatrix.setValue(vertex.getId() - RIGHTSIDE_MIN, compId, sumLeft);
            }
        }
    }

    @Override
    public void beginIteration(GraphChiContext ctx) {
    }

    @Override
    public void endIteration(GraphChiContext ctx) {
        System.out.println("Normalizing...");
        // Normalize
        for(int c=0; c < numComputations; c++) {
            leftScoreMatrix.normalizeSquared(c);
            rightScoreMatrix.normalizeSquared(c);
        }
    }

    @Override
    public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void endInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
    }

    @Override
    public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
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

        GraphChiEngine engine = new GraphChiEngine<Float, Float>(graph, nshards);
        BipartiteHubsAndAuthorities bhaa = new BipartiteHubsAndAuthorities(computations,
                leftMax, engine.numVertices(), cutOff);

        engine.setOnlyAdjacency(true);
        engine.setAutoLoadNext(true);
        engine.setModifiesInedges(false);
        engine.setModifiesOutedges(false);
        engine.setEnableDeterministicExecution(false);
        engine.setEdataConverter(null);
        engine.setVertexDataConverter(null);
        engine.run(bhaa, niters);


        /* Output top-lists */
        int ntop = 10000;
        for(int icomp=0; icomp < computations.size(); icomp++) {
            TreeSet<IdFloat> topList = Toplist.topList(bhaa.leftScoreMatrix, icomp, ntop);
            String outputfile = "toplist.weightedHubsAuth." + computations.get(icomp).getName() + "_cutoff_" + cutOff + ".tsv";

            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputfile)));

            for(IdFloat item : topList) {
                writer.write(item.getVertexId() + "\t" + computations.get(icomp).getName() + "\t" + item.getValue() +"\n");                }


            writer.close();
        };

        /* Report metrics */
        Metrics.shutdown();
        rep.run();
    }

    private static int findApproxMaximumLeftVertex(String graph) throws IOException {

        /* Check if a file exists that states the number */
        File overrideFile = new File(graph + ".maxleft");
        if (overrideFile.exists()) {
            BufferedReader rd = new BufferedReader(new FileReader(overrideFile));
            String ln = rd.readLine();
            System.out.println("---> Override: " + ln);
            return Integer.parseInt(ln);
        }


        DegreeData degData = new DegreeData(graph);

        int vertexSt = 0;
        int step = 2000000;
        int maxId = step;
        boolean  found = false;

        while(maxId < RIGHTSIDE_MIN) {

            degData.load(vertexSt, maxId);

            boolean nonzero = false;
            for(int i=vertexSt; i <= maxId; i++) {
                if (degData.getDegree(i).inDegree > 0 || degData.getDegree(i).outDegree > 0) {
                    nonzero = true;
                    break;
                }
            }
            if (!nonzero) {
                return vertexSt;
            }

            vertexSt = maxId;
            maxId += step;
        }
        throw new RuntimeException("No non-zero degree vertices??!");
    }


}
