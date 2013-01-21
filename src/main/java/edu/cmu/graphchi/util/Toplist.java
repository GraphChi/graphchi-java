package edu.cmu.graphchi.util;

import edu.cmu.graphchi.aggregators.ForeachCallback;
import edu.cmu.graphchi.aggregators.VertexAggregator;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.datablocks.IntConverter;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * From float vertex data, prints a top K listing
 */
public class Toplist {

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
}
