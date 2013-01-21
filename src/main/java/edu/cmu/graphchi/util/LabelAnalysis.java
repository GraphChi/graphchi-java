package edu.cmu.graphchi.util;

import edu.cmu.graphchi.vertexdata.ForeachCallback;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import edu.cmu.graphchi.datablocks.IntConverter;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;

/**
 * Utility for counting the number of different labels in the
 * vertex-data. Vertices which have same id as their vertex-id are
 * not calculated. This is used for connected components and community
 * detection applications.
 * @author  Aapo Kyrola
 */
public class LabelAnalysis {

    /* TODO: faster, more memory efficient */

    /**
     * Analyzes the labels of the vertices and outputs a file baseFilename + ".components"
     * with label,count.  Singletons labels not listed.
     * @param baseFilename input graph fhile
     * @param numVertices number of vertices in graph
     * @param translate vertex-id translater (from internal to actual ids)
     * @return
     * @throws IOException
     */
    public static Collection<IdCount> computeLabels(String baseFilename, int numVertices,
                                                    VertexIdTranslate translate) throws IOException {
        final HashMap<Integer, IdCount> counts = new HashMap<Integer, IdCount>(1000000);
        VertexAggregator.foreach(numVertices, baseFilename, new IntConverter(), new ForeachCallback<Integer>() {
            public void callback(int vertexId, Integer vertexValue) {
                 if (vertexId != vertexValue) {
                    IdCount cnt = counts.get(vertexValue);
                     if (cnt == null) {
                         cnt = new IdCount(vertexValue, 1);
                         counts.put(vertexValue, cnt);
                     }
                     cnt.count++;
                 }
            }
        });

        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(baseFilename + ".components"));
        for(IdCount cnt : counts.values()) {
            String s = translate.backward(cnt.id) + "," + cnt.count + "\n";
            bos.write(s.getBytes());
        }
        bos.flush();
        bos.close();

        return counts.values();
    }

}
