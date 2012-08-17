package edu.cmu.graphchi.shards;

import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import ucar.unidata.io.RandomAccessFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

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
    private int blockSize = 1024 * 1024;
    private int sizeOf = -1;
    private int adjOffset = 0;
    private int curvid = 0;
    private boolean onlyAdjacency = false;
    private boolean asyncEdataLoading = true;

    private BytesToValueConverter<EdgeDataType> converter;
    private RandomAccessFile edataFile;
    private RandomAccessFile adjFile;
    private boolean modifiesOutedges = true;


    public SlidingShard(String edgeDataFilename, String adjDataFilename,
                        int rangeStart, int rangeEnd) throws IOException {
        this.edgeDataFilename = edgeDataFilename;
        this.adjDataFilename = adjDataFilename;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;

        adjFilesize = new File(adjDataFilename).length();
        edataFilesize = new File(edgeDataFilename).length();
        activeBlocks = new ArrayList<Block>();
        edataFile = new RandomAccessFile(this.edgeDataFilename, "rwd");
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
            curBlock = new Block(edataFile, edataOffset,
                    (int) Math.min(edataOffset + blockSize, edataFilesize));
            activeBlocks.add(curBlock);

        }
    }

    private ChiPointer readEdgePtr() {
        assert(sizeOf >= 0);
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

        System.out.println("Load sliding: " + rangeStart + " -- " + rangeEnd);
        System.out.println(adjDataFilename);
        /* Read next */
        if (!activeBlocks.isEmpty() && !onlyAdjacency) {
            curBlock = activeBlocks.get(0);
        }

        if (adjFile == null) {
            adjFile = new RandomAccessFile(adjDataFilename, "r");
        }

        adjFile.seek(adjOffset);

        for(int i=(curvid - start); i < nvecs; i++) {
            if (adjOffset >= adjFilesize) break;

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
                n = adjFile.readInt();
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
                        int target = adjFile.readInt();
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
                        vertex.addOutEdge(eptr.blockId, eptr.offset, target);

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
    }

    public void flush() throws IOException {
        releasePriorToOffset(true, false);
    }

    public void setOffset(int newoff, int _curvid, int edgeptr) {
        adjOffset = newoff;
        curvid = _curvid;
        edataOffset = edgeptr;
    }

    public void releasePriorToOffset(boolean all, boolean disableWrites)
            throws IOException {
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
        sizeOf = converter.sizeOf();
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

    class Block {
        RandomAccessFile rfile;
        int offset;
        int end;
        int blockId;
        int ptr;
        boolean active = false;

        Block(RandomAccessFile rfile, int offset, int end) {
            this.end = end;
            this.rfile = rfile;
            this.offset = offset;
            blockId = blockManager.allocateBlock(end - offset);
            ptr = 0;
        }


        void readAsync() throws IOException {
            // TODO: actually asynch
            readNow();
        }

        void readNow() throws IOException {
            synchronized (rfile) {
                rfile.seek(offset);
                byte[] data = blockManager.getRawBlock(blockId);
                rfile.readFully(data);
            }
        }

        void commitNow() throws IOException {
            synchronized (rfile) {
                rfile.seek(offset);
                byte[] data = blockManager.getRawBlock(blockId);
                rfile.write(data);
            }
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
