package edu.cmu.graphchi.shards;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.io.CompressedIO;
import nom.tam.util.BufferedDataInputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 * Copyright [2012] [Aapo Kyrola, Guy Blelloch, Carlos Guestrin / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Used only internally - do not modify. To understand Memory shards, see
 * http://code.google.com/p/graphchi/wiki/IntroductionToGraphChi
 * @param <EdgeDataType>
 */
public class MemoryShard <EdgeDataType> {

    private String edgeDataFilename;
    private String adjDataFilename;
    private int rangeStart;
    private int rangeEnd;

    private byte[] adjData;
    private int[] blockIds = new int[0];
    private int[] blockSizes = new int[0];;

    private int edataFilesize;
    private boolean loaded = false;
    private boolean onlyAdjacency = false;
    private boolean hasSetRangeOffset = false, hasSetOffset = false;

    private int rangeStartOffset, rangeStartEdgePtr, rangeContVid;
    private int adjDataLength;

    private DataBlockManager dataBlockManager;
    private BytesToValueConverter<EdgeDataType> converter;
    private int streamingOffset, streamingOffsetEdgePtr, streamingOffsetVid;
    private int blocksize = 0;

    private final Timer loadAdjTimer = Metrics.defaultRegistry().newTimer(MemoryShard.class, "load-adj", TimeUnit.SECONDS, TimeUnit.MINUTES);
    private final Timer loadVerticesTimers = Metrics.defaultRegistry().newTimer(MemoryShard.class, "load-vertices", TimeUnit.SECONDS, TimeUnit.MINUTES);

    private static final Logger logger = ChiLogger.getLogger("memoryshard");

    private ArrayList<ShardIndex.IndexEntry> index;


    private MemoryShard() {}

    public MemoryShard(String edgeDataFilename, String adjDataFilename, int rangeStart, int rangeEnd) {
        this.edgeDataFilename = edgeDataFilename;
        this.adjDataFilename = adjDataFilename;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;

    }

    public void commitAndRelease(boolean modifiesInedges, boolean modifiesOutedges) throws IOException {
        int nblocks = blockIds.length;

        if (!onlyAdjacency && loaded) {
            if (modifiesInedges) {
                if (blocksize == 0) {
                    blocksize = ChiFilenames.getBlocksize(converter.sizeOf());
                }
                int startStreamBlock = rangeStartEdgePtr / blocksize;
                for(int i=0; i < nblocks; i++) {
                    String blockFilename = ChiFilenames.getFilenameShardEdataBlock(edgeDataFilename, i, blocksize);
                    if (i >= startStreamBlock) {
                        // Synchronous write
                        CompressedIO.writeCompressed(new File(blockFilename),
                                dataBlockManager.getRawBlock(blockIds[i]),
                                blockSizes[i]);
                    } else {
                        // Asynchronous write (not implemented yet, so is same as synchronous)
                        CompressedIO.writeCompressed(new File(blockFilename),
                                dataBlockManager.getRawBlock(blockIds[i]),
                                blockSizes[i]);
                    }
                }

            } else if (modifiesOutedges) {
                int last = streamingOffsetEdgePtr;
                if (last == 0) {
                    last = edataFilesize;
                }
                int startblock = (int) (rangeStartEdgePtr / blocksize);
                int endblock = (int) (last / blocksize);
                for(int i=startblock; i <= endblock; i++) {
                    String blockFilename = ChiFilenames.getFilenameShardEdataBlock(edgeDataFilename, i, blocksize);
                    CompressedIO.writeCompressed(new File(blockFilename),
                            dataBlockManager.getRawBlock(blockIds[i]),
                            blockSizes[i]);
                }
            }
            /* Release all blocks */
            for(Integer blockId : blockIds) {
                dataBlockManager.release(blockId);
            }
        }
    }

    public void loadVertices(final int windowStart, final int windowEnd, final ChiVertex[] vertices, final boolean disableOutEdges, final ExecutorService parallelExecutor)
            throws IOException {
        DataInput compressedInput = null;
        if (adjData == null) {
            compressedInput = loadAdj();

            if (!onlyAdjacency) loadEdata();
        }

        TimerContext _timer = loadVerticesTimers.time();

        if (compressedInput != null) {
            // This means we are using compressed data and cannot read in parallel (or could we?)
            // A bit ugly.
            index = new ArrayList<ShardIndex.IndexEntry>();
            index.add(new ShardIndex.IndexEntry(0, 0, 0));
        }
        final int sizeOf = (converter == null ? 0 : converter.sizeOf());

        /* Load in parallel */
        if (compressedInput == null) {
            final AtomicInteger countDown = new AtomicInteger(index.size());
            final Object waitLock = new Object();
            for(int chunk=0; chunk<index.size(); chunk++) {
                final int _chunk = chunk;
                parallelExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            loadAdjChunk(windowStart, windowEnd, vertices, disableOutEdges, null, sizeOf, _chunk);
                        } catch (IOException ioe) {
                            ioe.printStackTrace();
                            throw new RuntimeException(ioe);
                        } finally {
                            countDown.decrementAndGet();
                            synchronized (waitLock) {
                                waitLock.notifyAll();
                            }

                        }
                    }
                } );
            }
            /* Wait for finishing */
            while (countDown.get() > 0) {
                synchronized (waitLock) {
                    try {
                        waitLock.wait(10000);
                    } catch (InterruptedException e) {}
                }
            }
        } else {
            loadAdjChunk(windowStart, windowEnd, vertices, disableOutEdges, compressedInput, sizeOf, 0);
        }

        _timer.stop();
    }

    private void loadAdjChunk(int windowStart, int windowEnd, ChiVertex[] vertices, boolean disableOutEdges, DataInput compressedInput, int sizeOf, int chunk) throws IOException {
        ShardIndex.IndexEntry indexEntry = index.get(chunk);

        int vid = indexEntry.vertex;
        int viden = (chunk < index.size() - 1 ?  index.get(chunk + 1).vertex : Integer.MAX_VALUE);
        int edataPtr = indexEntry.edgePointer * sizeOf;
        int adjOffset = indexEntry.fileOffset;
        int end = adjDataLength;
        if (chunk < index.size() - 1) {
            end = index.get(chunk + 1).fileOffset;
        }

        boolean containsRangeEnd = (vid < rangeEnd && viden > rangeEnd);
        boolean containsRangeSt = (vid <= rangeStart && viden > rangeStart);

        DataInput adjInput = (compressedInput != null ? compressedInput : new DataInputStream(new ByteArrayInputStream(adjData)));

        adjInput.skipBytes(adjOffset);

        try {
            while(adjOffset < end) {

                if (containsRangeEnd) {
                    if (!hasSetOffset && vid > rangeEnd) {
                        streamingOffset = adjOffset;
                        streamingOffsetEdgePtr = edataPtr;
                        streamingOffsetVid = vid;
                        hasSetOffset = true;
                    }
                }
                if (containsRangeSt)  {
                    if (!hasSetRangeOffset && vid >= rangeStart) {
                        rangeStartOffset = adjOffset;
                        rangeStartEdgePtr = edataPtr;
                        hasSetRangeOffset = true;
                    }
                }

                int n = 0;
                int ns = adjInput.readUnsignedByte();
                adjOffset += 1;
                assert(ns >= 0);
                if (ns == 0) {
                    // next value tells the number of vertices with zeros
                    vid++;
                    int nz = adjInput.readUnsignedByte();
                    adjOffset += 1;
                    vid += nz;
                    continue;
                }
                if (ns == 0xff) {   // If 255 is not enough, then stores a 32-bit integer after.
                    n = Integer.reverseBytes(adjInput.readInt());
                    adjOffset += 4;
                } else {
                    n = ns;
                }

                ChiVertex vertex = null;
                if (vid >= windowStart && vid <= windowEnd) {
                    vertex = vertices[vid - windowStart];
                }

                while (--n >= 0) {
                    int target = Integer.reverseBytes(adjInput.readInt());
                    adjOffset += 4;
                    if (!(target >= rangeStart && target <= rangeEnd))
                        throw new IllegalStateException("Target " + target + " not in range!");
                    if (vertex != null && !disableOutEdges) {
                        vertex.addOutEdge((onlyAdjacency ? -1 : blockIds[edataPtr / blocksize]), (onlyAdjacency ? -1 : edataPtr % blocksize), target);
                    }

                    if (target >= windowStart) {
                        if (target <= windowEnd) {
                            ChiVertex dstVertex = vertices[target - windowStart];
                            if (dstVertex != null) {
                                dstVertex.addInEdge((onlyAdjacency ? -1 : blockIds[edataPtr / blocksize]),
                                        (onlyAdjacency ? -1 : edataPtr % blocksize),
                                        vid);
                            }
                            if (vertex != null && dstVertex != null) {
                                dstVertex.parallelSafe = false;
                                vertex.parallelSafe = false;
                            }
                        }
                    }
                    edataPtr += sizeOf;

                    // TODO: skip
                }
                vid++;
            }
        } catch (EOFException eof) {
            return;
        }
        if (adjInput instanceof InputStream) {
            ((InputStream) adjInput).close();
        }
    }


    private DataInput loadAdj() throws FileNotFoundException, IOException {
        File compressedFile = new File(adjDataFilename + ".gz");
        InputStream adjStreamRaw;
        long fileSizeEstimate = 0;
        if (compressedFile.exists()) {
            logger.info("Note: using compressed: " + compressedFile.getAbsolutePath());
            adjStreamRaw = new GZIPInputStream(new FileInputStream(compressedFile));
            fileSizeEstimate = compressedFile.length() * 3 / 2;
        } else {
            adjStreamRaw = new FileInputStream(adjDataFilename);
            fileSizeEstimate = new File(adjDataFilename).length();
        }

        /* Load index */
        index = new ShardIndex(new File(adjDataFilename)).sparserIndex(1204 * 1024);
        BufferedInputStream adjStream =	new BufferedInputStream(adjStreamRaw, (int) fileSizeEstimate /
                4);

        // Hack for cases when the load is not divided into subwindows
        TimerContext _timer = loadAdjTimer.time();

        ByteArrayOutputStream adjDataStream = new ByteArrayOutputStream((int) fileSizeEstimate);
        try {
            byte[] buf = new byte[(int) fileSizeEstimate / 4];   // Read in 16 chunks
            while (true) {
                int read =  adjStream.read(buf);
                if (read > 0) {
                    adjDataStream.write(buf, 0, read);
                } else break;
            }
        } catch (EOFException err) {
            // Done
        }

        adjData = adjDataStream.toByteArray();
        adjDataLength = adjData.length;

        adjStream.close();
        adjDataStream.close();

        _timer.stop();
        return null;
    }

    private void loadEdata() throws FileNotFoundException, IOException {
        /* Load the edge data from file. Should be done asynchronously. */
        blocksize = ChiFilenames.getBlocksize(converter.sizeOf());

        if (!loaded) {
            edataFilesize = ChiFilenames.getShardEdataSize(edgeDataFilename);
            int nblocks = edataFilesize / blocksize + (edataFilesize % blocksize == 0 ? 0 : 1);
            blockIds = new int[nblocks];
            blockSizes = new int[nblocks];
            for(int fileBlockId=0; fileBlockId < nblocks; fileBlockId++) {
                int fsize = Math.min(edataFilesize - blocksize * fileBlockId, blocksize);
                blockIds[fileBlockId] = dataBlockManager.allocateBlock(fsize);
                blockSizes[fileBlockId] = fsize;
                String blockfilename = ChiFilenames.getFilenameShardEdataBlock(edgeDataFilename, fileBlockId, blocksize);
                CompressedIO.readCompressed(new File(blockfilename), dataBlockManager.getRawBlock(blockIds[fileBlockId]), fsize);
            }

            loaded = true;
        }
    }

    public DataBlockManager getDataBlockManager() {
        return dataBlockManager;
    }

    public void setDataBlockManager(DataBlockManager dataBlockManager) {
        this.dataBlockManager = dataBlockManager;
    }

    public void setConverter(BytesToValueConverter<EdgeDataType> converter) {
        this.converter = converter;
    }

    public int getStreamingOffset() {
        return streamingOffset;
    }

    public int getStreamingOffsetEdgePtr() {
        return streamingOffsetEdgePtr;
    }

    public int getStreamingOffsetVid() {
        return streamingOffsetVid;
    }

    public void setOnlyAdjacency(boolean onlyAdjacency) {
        this.onlyAdjacency = onlyAdjacency;
    }
}
