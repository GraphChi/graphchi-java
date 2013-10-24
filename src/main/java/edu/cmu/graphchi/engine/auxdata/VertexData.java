package edu.cmu.graphchi.engine.auxdata;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import edu.cmu.graphchi.datablocks.IntConverter;
import nom.tam.util.BufferedDataInputStream;
import ucar.unidata.io.RandomAccessFile;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Logger;

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
public class VertexData <VertexDataType> {

    private byte[] vertexData;
    private long vertexSt, vertexEn;
    private String baseFilename;
    private RandomAccessFile vertexDataFile;
    private BytesToValueConverter <VertexDataType> converter;
    private DataBlockManager blockManager;
    private boolean sparse;
    private long[] index;
    private long lastOffset = 0;
    private long lastStart = 0;

    private final static Logger logger = ChiLogger.getLogger("vertex-data");

    public VertexData(long nvertices, String baseFilename,
                      BytesToValueConverter<VertexDataType> converter) throws IOException {
        this(nvertices, baseFilename, converter, true);
    }

    public VertexData(long nvertices, String baseFilename,
                      BytesToValueConverter<VertexDataType> converter, boolean _sparse) throws IOException {
        this.baseFilename = baseFilename;
        this.converter = converter;
        this.sparse = _sparse;

        File sparseDegreeFile = new File(ChiFilenames.getFilenameOfDegreeData(baseFilename, true));
        if (sparse && !sparseDegreeFile.exists()) {
            sparse = false;
            logger.info("Sparse vertex data was allowed but sparse degree file did not exist  using dense");
        }
        File vertexfile = new File(ChiFilenames.getFilenameOfVertexData(baseFilename, converter, sparse));

        if (!sparse) {
            long expectedSize = (long) converter.sizeOf() * (long) nvertices;

            // Check size and create if does not exists
            logger.info("Vertex file [" + vertexfile.getAbsolutePath() + "] length: " + vertexfile.length() + ", nvertices=" + nvertices
                    + ", expected size: " + expectedSize);
            if (!vertexfile.exists() || vertexfile.length() < expectedSize) {
                if (!vertexfile.exists()) {
                    vertexfile.createNewFile();
                }
                logger.warning("Vertex data file did not exists, creating it. Vertices: " + nvertices);
                FileOutputStream fos = new FileOutputStream(vertexfile);
                byte[] tmp = new byte[32678];
                long written = 0;
                while(written < expectedSize) {
                    long n = Math.min(expectedSize - written, tmp.length);
                    fos.write(tmp, 0, (int)n);
                    written += n;
                }
                fos.close();
            }
        } else {
            /* Initializes sparse vertex data file given sparse degree file */
            if (!vertexfile.exists()) {
                BufferedDataInputStream dis = new BufferedDataInputStream(new FileInputStream(sparseDegreeFile));
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(vertexfile)));

                byte[] empty = new byte[converter.sizeOf()];
                try {
                    while(true) {
                        long vertexId = Long.reverseBytes(dis.readLong());
                        dis.skipBytes(8);
                        dos.writeLong(Long.reverseBytes(vertexId));
                        dos.write(empty);
                    }
                } catch (EOFException err) {}
                dos.close();
                dis.close();;
            }
        }

        vertexDataFile = new RandomAccessFile(vertexfile.getAbsolutePath(), "rwd");
        vertexEn = vertexSt = 0;
    }

    public void releaseAndCommit(long firstVertex, int blockId) throws IOException {
        assert(blockId >= 0);
        byte[] data = blockManager.getRawBlock(blockId);

        if (!sparse) {
            long dataStart = (long) firstVertex * (long) converter.sizeOf();

            synchronized (vertexDataFile) {
                vertexDataFile.seek(dataStart);
                vertexDataFile.write(data);

                blockManager.release(blockId);

                vertexDataFile.flush();
            }
            logger.info("Vertex data write: " + dataStart + " -- " + (dataStart + data.length));

        } else {
            synchronized (vertexDataFile) {
                vertexDataFile.seek(lastOffset);
                int sizeOf = converter.sizeOf();
                for(int i=0; i < index.length; i++) {
                    vertexDataFile.writeLong(Long.reverseBytes(index[i]));  // Note: when writing, the random access file does not take byte order into account!
                    vertexDataFile.write(data, i * sizeOf, sizeOf);
                }
                blockManager.release(blockId);
                vertexDataFile.flush();

            }
        }
    }

    /**
     * Load vertices' data
     * @param _vertexSt
     * @param _vertexEn inclusive
     * @return
     * @throws IOException
     */
    public int load(long _vertexSt, long _vertexEn) throws IOException {

        vertexSt = _vertexSt;
        vertexEn = _vertexEn;
        synchronized (vertexDataFile) {

            if (!sparse) {
                long dataSize =  (vertexEn - vertexSt + 1) *  (long)  converter.sizeOf();
                long dataStart =   vertexSt *  (long) converter.sizeOf();

                int blockId =  blockManager.allocateBlock((int) dataSize);
                vertexData = blockManager.getRawBlock(blockId);
                vertexDataFile.seek(dataStart);
                vertexDataFile.readFully(vertexData);
                return blockId;
            } else {

                // Have to read in two passes
                if (lastStart > _vertexSt) {
                    vertexDataFile.seek(0);
                }
                lastStart = _vertexSt;

                int sizeOf = converter.sizeOf();
                long startPos = vertexDataFile.getFilePointer();
                int n = 0;
                boolean foundStart = false;
                try {
                    while(true) {
                        long vertexId = vertexDataFile.readLong();
                        if (!foundStart && vertexId >= _vertexSt) {
                            startPos = vertexDataFile.getFilePointer() - 8;
                            foundStart = true;
                        }
                        if (vertexId >= _vertexSt && vertexId <= _vertexEn) {
                            n++;
                        } else if (vertexId > vertexEn) {
                            break;
                        }

                        vertexDataFile.skipBytes(sizeOf);
                    }
                } catch (EOFException eof) {}

                index = new long[n];
                vertexDataFile.seek(startPos);
                int blockId =  blockManager.allocateBlock(n * sizeOf);
                vertexData = blockManager.getRawBlock(blockId);

                int i = 0;
                try {
                    while(i < n) {
                        long vertexId = vertexDataFile.readLong();
                        if (vertexId >= _vertexSt && vertexId <= _vertexEn) {
                            index[i] = vertexId;
                            vertexDataFile.read(vertexData, i * sizeOf, sizeOf);
                            i++;
                        } else {
                            vertexDataFile.skipBytes(sizeOf);
                        }
                    }
                } catch (EOFException eof) {}
                if (i != n) throw new IllegalStateException("Mismatch when reading sparse vertex data:" + i + " != " + n);
                lastOffset = (int) startPos;
                return blockId;
            }
        }
    }

    public ChiPointer getVertexValuePtr(long vertexId, int blockId) {
        assert(vertexId >= vertexSt && vertexId <= vertexEn);
        if (!sparse) {
            return new ChiPointer(blockId, (int) ((vertexId - vertexSt) * converter.sizeOf()));
        } else {
            int idx = Arrays.binarySearch(index, vertexId);
            if (idx < 0) {
                return null;
            }
            return new ChiPointer(blockId, idx * converter.sizeOf());
        }
    }

    public void setBlockManager(DataBlockManager blockManager) {
        this.blockManager = blockManager;
    }

    // This is a bit funny... Is there a better way to create a memory efficient
    // int array in scala?
    public static int[] createIntArray(int n) {
        return new int[n];
    }

    public Iterator<Long> currentIterator() {
        if (!sparse) {
            return new Iterator<Long>() {
                long j = vertexSt;
                @Override
                public boolean hasNext() {
                    return (j <= vertexEn);
                }

                @Override
                public Long next() {
                    return j++;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } else {
            return new Iterator<Long>() {
                int j = 0;
                @Override
                public boolean hasNext() {
                    return (j < index.length);
                }

                @Override
                public Long next() {
                    return index[j++];
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };

        }
    }

    public void close() {
        try {
            vertexDataFile.flush();
            vertexDataFile.getFD().sync();
            vertexDataFile.close();
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }
}
