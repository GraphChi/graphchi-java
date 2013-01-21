package edu.cmu.graphchi.util;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.vertexdata.ForeachCallback;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * From float vertex data, prints a top K listing. For example, used
 * to get top K highest ranked vertices in PageRank.
 */
public class Toplist {

    /**
     * Returns a sorted list of top topN vertices having float values.
     * @param baseFilename input-graph
     * @param numVertices number of vertices in the graph (use engine.numVertices())
     * @param topN how many top results to include
     * @return
     * @throws IOException
     */
    public static TreeSet<IdFloat> topListFloat(String baseFilename, int numVertices, final int topN) throws IOException{
        final TreeSet<IdFloat> topList = new TreeSet<IdFloat>(new IdFloat.Comparator());
        VertexAggregator.foreach(numVertices, baseFilename, new FloatConverter(), new ForeachCallback<Float>()  {
            IdFloat least;
            public void callback(int vertexId, Float vertexValue) {
                if (topList.size() < topN) {
                    topList.add(new IdFloat(vertexId, vertexValue));
                    least = topList.last();
                } else {
                    if (vertexValue > least.value) {
                        topList.remove(least);
                        topList.add(new IdFloat(vertexId, vertexValue));
                        least = topList.last();
                    }
                }
            }
        }

        );
        return topList;
    }

    /**
     * Returns a sorted list of top topN vertices having integer values.
     * @param baseFilename input-graph
     * @param numVertices number of vertices in the graph (use engine.numVertices())
     * @param topN how many top results to include
     * @return
     * @throws IOException
     */
    public static TreeSet<IdInt> topListInt(String baseFilename, int numVertices, final int topN) throws IOException{
        final TreeSet<IdInt> topList = new TreeSet<IdInt>(new Comparator<IdInt>() {
            public int compare(IdInt a, IdInt b) {
                if (a.vertexId == b.vertexId) return 0;
                return  (a.value > b.value ? -1 : (a.value == b.value ? (a.vertexId < b.vertexId ? -1 : 1) : 1)); // Descending order
            }
        });
        VertexAggregator.foreach(numVertices, baseFilename, new IntConverter(), new ForeachCallback<Integer>()  {
            IdInt least;
            public void callback(int vertexId, Integer vertexValue) {
                if (topList.size() < topN) {
                    topList.add(new IdInt(vertexId, vertexValue));
                    least = topList.last();
                } else {
                    if (vertexValue > least.value) {
                        topList.remove(least);
                        topList.add(new IdInt(vertexId, vertexValue));
                        least = topList.last();
                    }
                }
            }
        }

        );
        return topList;
    }
    /**
     * Returns a sorted list of top topN vertices having float values stored
     * in-memory in the HugeFloatMatrix.
     * @param floatMatrix vertex-values
     * @param column column to use
     * @param topN how many top results to include
     * @return
     * @throws IOException
     */
    public static TreeSet<IdFloat> topList(final HugeFloatMatrix floatMatrix, final int column, final int topN) {
        final TreeSet<IdFloat> topList = new TreeSet<IdFloat>(new Comparator<IdFloat>() {
            public int compare(IdFloat idFloat, IdFloat idFloat1) {
                if (idFloat.vertexId == idFloat1.vertexId) return 0;
                int comp =  -Float.compare(idFloat.value, idFloat1.value); // Descending order
                return (comp != 0 ? comp : (idFloat.vertexId < idFloat1.vertexId ? 1 : -1));
            }
        });
        IdFloat least = null;
        int n = (int) floatMatrix.getNumRows();
        for(int vertexId=0; vertexId < n; vertexId++) {
            float vertexValue = floatMatrix.getValue(vertexId, column);
            if (topList.size() < topN) {
                topList.add(new IdFloat(vertexId, vertexValue));
                least = topList.last();
            } else {
                if (vertexValue > least.value) {
                    topList.remove(least);
                    topList.add(new IdFloat(vertexId, vertexValue));
                    least = topList.last();
                }
            }
        }

        return topList;
    }


    public static void main(String[] args) throws Exception {
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);

        ArrayList<VertexInterval> intervals = ChiFilenames.loadIntervals(baseFilename, nShards);
        TreeSet<IdFloat> top20 = Toplist.topListFloat(baseFilename, intervals.get(intervals.size() - 1).getLastVertex(), 20);
        VertexIdTranslate trans = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, nShards)));
        System.out.println("Result: " + top20);
        int i = 0;
        for(IdFloat vertexRank : top20) {
            System.out.println(++i + ": " + trans.backward(vertexRank.getVertexId()) + " = " + vertexRank.getValue());
        }
    }
}
