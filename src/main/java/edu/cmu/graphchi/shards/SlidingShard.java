package edu.cmu.graphchi.shards;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.io.CompressedIO;
import nom.tam.util.BufferedDataInputStream;

import java.io.*;
import java.util.ArrayList;
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
 * Used only internally - do not modify. To understand Sliding shards, see
 * http://code.google.com/p/graphchi/wiki/IntroductionToGraphChi
 * @param <EdgeDataType>
 */
public class SlidingShard <EdgeDataType> {

    private String edgeDataFilename;
    private String adjDataFilename;
    private int rangeStart;
    private int rangeEnd;

    private DataBlockManager blockManager;

    private ArrayList<Block> activeBlocks;

    public long edataFilesize, adjFilesize;
    private Block curBlock = null;
    private int edataOffset = 0;
    private int blockSize = -1;
    private int sizeOf = -1;
    private int adjOffset = 0;
    private int curvid = 0;
    private boolean onlyAdjacency = false;
    private boolean asyncEdataLoading = true;

    private BytesToValueConverter<EdgeDataType> converter;
    private BufferedDataInputStream adjFile;
    private boolean modifiesOutedges = true;
    
    private static final Logger logger = ChiLogger.getLogger("slidingshard");


    public SlidingShard(String edgeDataFilename, String adjDataFilename,
                        int rangeStart, int rangeEnd) throws IOException {
        this.edgeDataFilename = edgeDataFilename;
        this.adjDataFilename = adjDataFilename;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;

        adjFilesize = new File(adjDataFilename).length();
        if (edgeDataFilename != null) {
            edataFilesize = ChiFilenames.getShardEdataSize(edgeDataFilename);
            activeBlocks = new ArrayList<Block>();
        } else {
            onlyAdjacency = true;
        }
    }

    public void finalize() {
        for (Block b : activeBlocks) b.release();
    }

    private void checkCurblock(int toread) {
        if (curBlock == null || curBlock.end < edataOffset + toread) {
            if (curBlock != null) {
                if  (!curBlock.active) {
                    curBlock.release();
                }
            }
            // Load next
            int start = (edataOffset / blockSize) * blockSize; // align
            int fileBlockId = edataOffset / blockSize;
            curBlock = new Block(edgeDataFilename, start,
                    (int) Math.min(start + blockSize, edataFilesize), fileBlockId, blockSize);
            curBlock.ptr = edataOffset - start; // Correction due to alignment.
            activeBlocks.add(curBlock);

        }
    }

    private ChiPointer readEdgePtr() {
        assert(sizeOf >= 0);
        if (onlyAdjacency) return null;
        checkCurblock(sizeOf);
        ChiPointer ptr = new ChiPointer(curBlock.blockId, curBlock.ptr);
        curBlock.ptr += sizeOf;
        edataOffset += sizeOf;
        return ptr;
    }


    public void skip(int n) throws IOException {
        int tot = n * 4;
        adjOffset += tot;
        adjFile.skipBytes(tot);
        edataOffset += sizeOf * n;
        if (curBlock != null) {
            curBlock.ptr += sizeOf * n;
        }
    }

    public void readNextVertices(ChiVertex[] vertices, int start, boolean disableWrites) throws IOException {
        int nvecs = vertices.length;
        curBlock = null;
        releasePriorToOffset(false, disableWrites);
        assert(activeBlocks.size() <= 1);

        /* Read next */
        if (!onlyAdjacency && !activeBlocks.isEmpty()) {
            curBlock = activeBlocks.get(0);
        }

        if (adjFile == null) {

            File compressedFile = new File(adjDataFilename + ".gz");
            if (compressedFile.exists()) {
                logger.info("Note: using compressed: " + compressedFile.getName());
                adjFile = new BufferedDataInputStream(new GZIPInputStream(new FileInputStream(compressedFile)), 1024 * 1024);

            } else {
                adjFile = new BufferedDataInputStream(new FileInputStream(adjDataFilename), 1024 * 1024);
            }
            adjFile.skipBytes(adjOffset);
        }


        try {
            for(int i=(curvid - start); i < nvecs; i++) {
                int n;
                int ns = adjFile.readUnsignedByte();


                assert(ns >= 0);
                adjOffset++;

                if (ns == 0) {
                    curvid++;
                    int nz = adjFile.readUnsignedByte();

                    adjOffset++;
                    assert(nz >= 0);
                    curvid += nz;
                    i += nz;
                    continue;
                }

                if (ns == 0xff) {
                    n = adjFile.readIntReversed();
                    adjOffset += 4;
                } else {
                    n = ns;
                }

                if (i < 0) {
                    skip(n);
                } else {
                    ChiVertex vertex = vertices[i];
                    assert(vertex == null || vertex.getId() == curvid);

                    if (vertex != null) {
                        while (--n >= 0) {
                            int target = adjFile.readIntReversed();
                            adjOffset += 4;
                            ChiPointer eptr = readEdgePtr();

                            if (!onlyAdjacency) {
                                if (!curBlock.active) {
                                    if (asyncEdataLoading) {
                                        curBlock.readAsync();
                                    } else {
                                        curBlock.readNow();
                                    }
                                }
                                curBlock.active = true;
                            }
                            vertex.addOutEdge(eptr == null ? -1 : eptr.blockId, eptr == null ? -1 : eptr.offset, target);

                            if (!(target >= rangeStart && target <= rangeEnd)) {
                                throw new IllegalStateException("Target " + target + " not in range!");
                            }
                        }
                    } else {
                        skip(n);
                    }
                }
                curvid++;
            }
        } catch (EOFException e) {}    // kosher
    }

    public void flush() throws IOException {
        releasePriorToOffset(true, false);
    }

    public void setOffset(int newoff, int _curvid, int edgeptr) {
        try {
           if (adjFile != null) adjFile.close();
        } catch (IOException ioe) {}
        adjFile = null;
        adjOffset = newoff;
        curvid = _curvid;
        edataOffset = edgeptr;
    }

    public void releasePriorToOffset(boolean all, boolean disableWrites)
            throws IOException {
        if (onlyAdjacency) return;
        for(int i=activeBlocks.size() - 1; i >= 0; i--) {
            Block b = activeBlocks.get(i);
            if (b.end <= edataOffset || all) {
                commit(b, all, disableWrites);
                activeBlocks.remove(i);
            }
        }
    }

    public long getEdataFilesize() {
        return edataFilesize;
    }

    public long getAdjFilesize() {
        return adjFilesize;
    }

    public DataBlockManager getDataBlockManager() {
        return blockManager;
    }

    public void setDataBlockManager(DataBlockManager dataBlockManager) {
        this.blockManager = dataBlockManager;
    }

    public BytesToValueConverter<EdgeDataType> getConverter() {
        return converter;
    }

    public void setConverter(BytesToValueConverter<EdgeDataType> converter) {
        this.converter = converter;
        if (converter == null) {
            sizeOf = 0;
            return;
        }
        sizeOf = converter.sizeOf();
        blockSize = ChiFilenames.getBlocksize(sizeOf);
    }


    void commit(Block b, boolean synchronously, boolean disableWrites) throws IOException {
        disableWrites = disableWrites || !modifiesOutedges;
        if (synchronously) {
            if (!disableWrites) b.commitNow();
            b.release();
        } else {
            if (!disableWrites) b.commitAsync();
            else b.release();
        }
    }

    public void setModifiesOutedges(boolean modifiesOutedges) {
        this.modifiesOutedges = modifiesOutedges;
    }

    public void setOnlyAdjacency(boolean onlyAdjacency) {
        this.onlyAdjacency = onlyAdjacency;
    }

    public long getNumEdges() {
        if (converter == null) return edataFilesize / 4; // TODO: fix.
        return edataFilesize / converter.sizeOf();
    }

    class Block {
        String blockFileName;
        int offset;
        int end;
        int blockId;
        int ptr;
        boolean active = false;

        Block(String edataFileName, int offset, int end, int fileBlockId, int blockSize) {
            this.end = end;
            this.offset = offset;
            blockId = blockManager.allocateBlock(end - offset);
            ptr = 0;
            blockFileName = ChiFilenames.getFilenameShardEdataBlock(edataFileName, fileBlockId, blockSize);
        }


        void readAsync() throws IOException {
            // TODO: actually async
            readNow();
        }

        void readNow() throws IOException {
            byte[] data = blockManager.getRawBlock(blockId);
            CompressedIO.readCompressed(new File(blockFileName), data, end - offset);

        }

        void commitNow() throws IOException {
            byte[] data = blockManager.getRawBlock(blockId);
            CompressedIO.writeCompressed(new File(blockFileName), data, end - offset);
        }

        void commitAsync() throws IOException {
            commitNow();
            // TODO asynchronous implementation
            release();
        }

        void release() {
            blockManager.release(blockId);
        }
    }
}
