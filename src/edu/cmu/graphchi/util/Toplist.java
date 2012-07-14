package edu.cmu.graphchi.util;

import edu.cmu.graphchi.aggregators.ForeachCallback;
import edu.cmu.graphchi.aggregators.VertexAggregator;
import edu.cmu.graphchi.datablocks.FloatConverter;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * From float vertex data, prints a top K listing
 */
public class Toplist {

    public static TreeSet<IdFloat> topListFloat(String baseFilename, final int topN) throws IOException{
        final TreeSet<IdFloat> topList = new TreeSet<IdFloat>(new Comparator<IdFloat>() {
            public int compare(IdFloat idFloat, IdFloat idFloat1) {
                return -Float.compare(idFloat.value, idFloat1.value); // Descending order
            }
        });
        VertexAggregator.foreach(baseFilename, new FloatConverter(), new ForeachCallback<Float>()  {
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


}
