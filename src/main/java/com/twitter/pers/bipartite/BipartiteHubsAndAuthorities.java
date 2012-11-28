package com.twitter.pers.bipartite;

import com.twitter.pers.Experiment;
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
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of Kleinberg's HITS algorithm
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class BipartiteHubsAndAuthorities implements GraphChiProgram<Float, Float> {

    protected static int RIGHTSIDE_MIN = -1;

    protected HugeFloatMatrix leftWeightMatrix;
    protected HugeFloatMatrix leftScoreMatrix;
    protected HugeFloatMatrix rightScoreMatrix;
    protected int numComputations;
    protected boolean initWeights;
    protected List<ComputationInfo> computations;

    /**
     * HITS algorithms
     * @param computations list of parallel computations to run
     * @param maxLeftVertex
     * @param maxRightVertex
     * @param cutOff minimum value for a weight to be included in the graph
     * @param weighted weighted HITS?
     * @param initWeights whether to use the initial weights
     * @throws IOException
     */
    protected BipartiteHubsAndAuthorities(List<ComputationInfo> computations, int maxLeftVertex, int maxRightVertex, float cutOff,
                                          boolean weighted, boolean initWeights)
            throws IOException {
        numComputations = computations.size();
        this.computations = computations;
        this.initWeights = initWeights;
        leftWeightMatrix = new HugeFloatMatrix(maxLeftVertex + 1, numComputations);
        rightScoreMatrix = new HugeFloatMatrix(maxRightVertex - RIGHTSIDE_MIN + 1, numComputations, 1.0f);
        leftScoreMatrix = new HugeFloatMatrix(maxLeftVertex + 1, numComputations, 1.0f);


        for(ComputationInfo compInfo : computations) {
            System.out.println("Loading weights: " + compInfo);
            WeightUtil.loadWeights(compInfo, leftWeightMatrix, cutOff, weighted);
            if (initWeights) {
                System.out.println("Loading initial weights: " + compInfo);
                WeightUtil.loadWeights(compInfo, leftScoreMatrix, cutOff, true);
            }
        }

        if (RIGHTSIDE_MIN  < 0) throw new IllegalArgumentException("list-id-offset not set!");
    }

    @Override
    public void update(ChiVertex<Float, Float> vertex, GraphChiContext context) {
        boolean isLeft = vertex.getId() < RIGHTSIDE_MIN;
        if (isLeft && initWeights && context.getIteration() == 0) {
            // If the left side has initial weights, then we need
            // to run right side (the hubs) first.
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
                int maxLeft = (int) leftWeightMatrix.getNumRows();
                for(int e=0; e < vertex.numEdges(); e++) {
                    int nbId = vertex.edge(e).getVertexId();
                    if (nbId < maxLeft) {
                        sumLeft += leftWeightMatrix.getValue(nbId, compId) * leftScoreMatrix.getValue(nbId, compId);
                    }
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

        String experimentDefinition = args[0];
        Experiment experiment = new Experiment(experimentDefinition);

        /* Initialize computations */
        String graph = experiment.getGraph();
        int nshards = experiment.getNumShards();
        float cutOff = Float.parseFloat(experiment.getProperty("cutoff"));
        int niters = experiment.getNumIterations();
        boolean weighted = Integer.parseInt(experiment.getProperty("weighted")) == 1;
        boolean initWeights = Integer.parseInt(experiment.getProperty("weighted")) == 2;
        BipartiteHubsAndAuthorities.RIGHTSIDE_MIN = Integer.parseInt(experiment.getProperty("list_id_offset"));

        List<ComputationInfo> computations = ComputationInfo.loadComputations(experiment.getFilenameProperty("inputlist"));

        /* Find the maximum vertex id on left-side by looking at the first non-zero degree */
        int leftMax = findApproxMaximumLeftVertex(graph);

        GraphChiEngine<Float, Float> engine = new GraphChiEngine<Float, Float>(graph, nshards);
        BipartiteHubsAndAuthorities bhaa = initializeApp(cutOff, computations, leftMax, engine, weighted, initWeights);

        engine.setOnlyAdjacency(true);
        engine.setAutoLoadNext(true);
        engine.setModifiesInedges(false);
        engine.setModifiesOutedges(false);
        engine.setEnableDeterministicExecution(false);
        engine.setEdataConverter(null);
        engine.setVertexDataConverter(null);
        engine.run(bhaa, niters);

        outputResults(experiment, bhaa, cutOff, computations, "hubsauth" + (weighted ? "_weighted" : "_unweighted"));

        /* Run debug output */
        debugRun(experiment, bhaa, computations, graph, nshards, "hubsauth" + (weighted ? "_weighted" : "_unweighted"));

        /* Report metrics */
        rep.run();
    }

    /**
     * Special step to output special information of the neighbors of the top 50
     * users for each computatoin.
     * @param app
     * @param computations
     */
    private static void debugRun(final Experiment experiment,
                                 final BipartiteHubsAndAuthorities app, final  List<ComputationInfo> computations,
                                 final String graph, final  int nshards, final String appName) throws IOException {

        System.out.println("=================== DEBUG RUN ==================");

        final ArrayList<FileWriter> debugWriters = new ArrayList<FileWriter>();
        final ArrayList<FileWriter> debugEdgeWriters = new ArrayList<FileWriter>();

        for(ComputationInfo computationInfo : computations) {
            HashMap<String, String> placeholders = new HashMap<String, String>();
            placeholders.put("topic", computationInfo.getName());
            placeholders.put("algo", appName);

            String outputfile = experiment.getOutputName(placeholders);

            FileWriter wr = new FileWriter(outputfile + ".debug_" + computationInfo.getName() + ".txt");
            wr.write("Started...\n");
            System.out.println("Opened: " + outputfile + ".debug_" + computationInfo.getName() + ".txt");
            debugWriters.add(wr);
            debugEdgeWriters.add(new FileWriter(outputfile + ".debugedge_" + computationInfo.getName() + ".txt"));
        }

        // Set of Lists to be added to debug (this set is populated by the top-users on first sweep)
        final Set<IntPair> toDebuglists = Collections.synchronizedSet(new HashSet<IntPair>());

        GraphChiEngine<Float, Float> engine = new GraphChiEngine<Float, Float>(graph, nshards);
        engine.setEnableScheduler(true);
        engine.setOnlyAdjacency(true);
        engine.setModifiesInedges(false);
        engine.setModifiesOutedges(false);
        engine.run(new GraphChiProgram<Float, Float>() {


            private void debugMessage(int compId, int vertexId, String message) {
                System.out.println("Debug message: " + message + "; compId=" + compId);
                try {
                    FileWriter wr = debugWriters.get(compId);
                    String str = vertexId + "\t" + message + "\n";

                    System.out.println("Debug write: " + wr + "; " + str);
                    synchronized (wr) {
                        wr.write(str);
                    }
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    throw new RuntimeException(ioe);
                }
            }

            private void debugEdge(int compId, int userVertex, int toListVertex, float weight, boolean authorityTop) {
                try {
                    FileWriter wr = debugEdgeWriters.get(compId);
                    synchronized (wr) {
                        wr.write(userVertex + "\t" + toListVertex + "\t" + weight + "\t" + (authorityTop ? 1 : 0) + "\n");
                    }
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    throw new RuntimeException(ioe);
                }
            }

            @Override
            public void update(ChiVertex<Float, Float> vertex, GraphChiContext context) {
                try {
                    if (vertex.getId() < RIGHTSIDE_MIN) {
                        // Authorities in top
                        for(ComputationInfo computationInfo : computations) {
                            if (computationInfo.getTopList().contains(vertex.getId())) {
                                int compId = computationInfo.getId();
                                System.out.println("Vertex " + vertex.getId()+ " in toplist " + computationInfo.getName() + ";" + compId);

                                /* Take top 20 of lists */
                                ArrayList<IdFloat> scores = new ArrayList<IdFloat>(vertex.numEdges());
                                float sumScore = 0;
                                float maxScore = 0;
                                for(int e=0; e < vertex.numEdges(); e++) {
                                    float hubScore = app.rightScoreMatrix.getValue(vertex.edge(e).getVertexId() - RIGHTSIDE_MIN, compId);
                                    scores.add(new IdFloat(vertex.edge(e).getVertexId(), hubScore));
                                    sumScore += hubScore;
                                    maxScore = Math.max(hubScore, maxScore);
                                }
                                debugMessage(compId, vertex.getId(), "Neighbor sum: " + sumScore);
                                debugMessage(compId, vertex.getId(), "Num neighbors: " + vertex.numEdges());
                                debugMessage(compId, vertex.getId(), "Max neighbor: " + maxScore);

                                Collections.sort(scores, new IdFloat.Comparator());
                                for(int i=0; i < 20; i++) {
                                    System.out.println(i + " / " + scores.size());
                                    if (i < scores.size()) {
                                        context.getScheduler().addTask(scores.get(i).getVertexId());
                                        System.out.println(scores.get(i).getVertexId() + "; " + scores.get(i).getValue());
                                        debugEdge(compId, vertex.getId(),
                                                scores.get(i).getVertexId() - RIGHTSIDE_MIN,
                                                scores.get(i).getValue(), true);
                                        toDebuglists.add(new IntPair(scores.get(i).getVertexId(), compId));
                                    }
                                }
                            }
                        }
                    } else {
                        // Lists in top
                        for(ComputationInfo computationInfo : computations) {
                            int compId = computationInfo.getId();
                            if (toDebuglists.contains(new IntPair(vertex.getId(), compId))) {
                                System.out.println("List " + vertex.getId()+ " in toplist debug " + computationInfo.getName());

                                /* Take top 20 of vertices */
                                ArrayList<IdFloat> scores = new ArrayList<IdFloat>(vertex.numEdges());
                                float sumScore = 0;
                                float maxScore = 0;
                                for(int e=0; e < vertex.numEdges(); e++) {
                                    float hubScore = app.leftScoreMatrix.getValue(vertex.edge(e).getVertexId(), compId);
                                    scores.add(new IdFloat(vertex.edge(e).getVertexId(), hubScore));
                                    sumScore += hubScore;
                                    maxScore = Math.max(hubScore, maxScore);
                                }

                                // Lot of code repetition!
                                debugMessage(compId, vertex.getId(), "Neighbor sum: " + sumScore);
                                debugMessage(compId, vertex.getId(), "Num neighbors: " + vertex.numEdges());
                                debugMessage(compId, vertex.getId(), "Max neighbor: " + maxScore);

                                Collections.sort(scores, new IdFloat.Comparator());
                                for(int i=0; i < 20; i++) {
                                    if (i < scores.size()) {
                                        debugEdge(compId, scores.get(i).getVertexId(),
                                                vertex.getId() - RIGHTSIDE_MIN,
                                                scores.get(i).getValue(), false);
                                        toDebuglists.add(new IntPair(scores.get(i).getVertexId(), compId));
                                    }
                                }
                            }
                        }

                    }
                } catch (Exception err) {
                    err.printStackTrace();
                }
            }

            @Override
            public void beginIteration(GraphChiContext ctx) {
                // Schedule the top vertices
                ctx.getScheduler().removeAllTasks();
                for(ComputationInfo computationInfo : computations) {
                    for (Integer vertexId : computationInfo.getTopList())  {
                        ctx.getScheduler().addTask(vertexId);
                    }
                }
            }

            @Override
            public void endIteration(GraphChiContext ctx) {
                try {
                    for(int i=0; i<debugEdgeWriters.size(); i++) {
                        debugEdgeWriters.get(i).close();
                        debugWriters.get(i).close();
                    }
                } catch (IOException ioe) { ioe.printStackTrace(); }
            }


            @Override
            public void beginInterval(GraphChiContext ctx, VertexInterval interval) {
            }

            @Override
            public void endInterval(GraphChiContext ctx, VertexInterval interval) {
                try {
                    for(int i=0; i<debugEdgeWriters.size(); i++) {
                        debugEdgeWriters.get(i).flush();
                        debugWriters.get(i).flush();
                    }
                } catch (Exception err) {
                    err.printStackTrace();
                }

                System.out.println(toDebuglists);
            }

            @Override
            public void beginSubInterval(GraphChiContext ctx, VertexInterval interval) {
            }

            @Override
            public void endSubInterval(GraphChiContext ctx, VertexInterval interval) {
            }
        }, 1);

    }

    protected static void outputResults(Experiment exp, BipartiteHubsAndAuthorities app, float cutOff,
                                        List<ComputationInfo> computations, String appName) throws IOException {
        /* Output top-lists */
        int ntop = 10000;
        for(int icomp=0; icomp < computations.size(); icomp++) {
            TreeSet<IdFloat> topList = Toplist.topList(app.leftScoreMatrix, icomp, ntop);
            HashMap<String, String> placeholders = new HashMap<String, String>();
            placeholders.put("topic", computations.get(icomp).getName());
            placeholders.put("algo", appName);

            String outputfile = exp.getOutputName(placeholders);
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputfile)));

            int i = 0;
            for(IdFloat item : topList) {
                writer.write(item.getVertexId() + "\t" + computations.get(icomp).getName() + "\t" + item.getValue() +"\n");
                if (i < 50) computations.get(icomp).getTopList().add(item.getVertexId());
                i++;
            }
            writer.close();
        }

    }

    protected static BipartiteHubsAndAuthorities initializeApp(float cutOff, List<ComputationInfo> computations, int leftMax,
                                                               GraphChiEngine engine, boolean weighted, boolean initWeights) throws IOException {
        return new BipartiteHubsAndAuthorities(computations,
                leftMax, engine.numVertices(), cutOff, weighted, initWeights);
    }

    protected static int findApproxMaximumLeftVertex(String graph) throws IOException {

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

    static class IntPair {
        int a;
        int b;

        IntPair(int a, int b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IntPair intPair = (IntPair) o;

            if (a != intPair.a) return false;
            if (b != intPair.b) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = a;
            result = 31 * result + b;
            return result;
        }
    }


}
