package edu.cmu.graphchi.apps.util;

import edu.cmu.graphchi.*;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexProcessor;

import java.io.*;
import java.util.logging.Logger;

/**
 * Creates a cassovary adjacency
 * @author Aapo Kyrola
 */
public class CreateCassovary  implements GraphChiProgram<Integer, Integer> {
    private static Logger logger = ChiLogger.getLogger("cassovaryconv");

    private BufferedOutputStream bos;

    @Override
    public void update(ChiVertex<Integer, Integer> vertex, GraphChiContext context) {
        try {
            if (vertex.numOutEdges() > 0) {
                synchronized(this) {
                    bos.write((context.getVertexIdTranslate().backward(vertex.getId()) + " " + vertex.numOutEdges() + "\n").getBytes());
                    for(int i=0; i < vertex.numOutEdges(); i++) {
                        bos.write((context.getVertexIdTranslate().backward(vertex.outEdge(i).getVertexId()) + "\n").getBytes());
                    }
                }
            }
        } catch (IOException ioe) {}
    }

    @Override
    public void beginIteration(GraphChiContext ctx) {
    }

    @Override
    public void endIteration(GraphChiContext ctx) {
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


    protected static FastSharder createSharder(String graphName, int numShards) throws IOException {
        return new FastSharder<Integer, Integer>(graphName, numShards, null, null, null, null);
    }

    public static void main(String[] args) throws Exception {
        String baseFilename = args[0];
        int nShards = Integer.parseInt(args[1]);
        String fileType = (args.length >= 3 ? args[2] : null);


        /* Create shards */
        FastSharder sharder = createSharder(baseFilename, nShards);
        if (baseFilename.equals("pipein")) {     // Allow piping graph in
            sharder.shard(System.in, fileType);
        } else {
            if (!new File(ChiFilenames.getFilenameIntervals(baseFilename, nShards)).exists()) {
                sharder.shard(new FileInputStream(new File(baseFilename)), fileType);
            } else {
                logger.info("Found shards -- no need to preprocess");
            }
        }

        /* Run GraphChi */
        GraphChiEngine<Integer, Integer> engine = new GraphChiEngine<Integer, Integer>(baseFilename, nShards);
        engine.setEdataConverter(null);
        engine.setVertexDataConverter(null);
        engine.setModifiesInedges(false); // Important optimization
        engine.setDisableInedges(true);
        engine.setModifiesOutedges(false);
        engine.setOnlyAdjacency(true);

        CreateCassovary cassovaryConv = new CreateCassovary();
        cassovaryConv.bos = new BufferedOutputStream(new FileOutputStream(baseFilename + ".cassovary"));
        engine.run(cassovaryConv, 1);

        cassovaryConv.bos.flush();
        cassovaryConv.bos.close();
        logger.info("Ready.");
    }
}
