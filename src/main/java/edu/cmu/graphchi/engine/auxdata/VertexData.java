package edu.cmu.graphchi.engine.auxdata;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.LoggingInitializer;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import nom.tam.util.BufferedDataInputStream;
import ucar.unidata.io.RandomAccessFile;

import java.io.*;
import java.util.Arrays;
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
    private int vertexSt, vertexEn;
    private String baseFilename;
    private RandomAccessFile vertexDataFile;
    private BytesToValueConverter <VertexDataType> converter;
    private DataBlockManager blockManager;
    private boolean sparse;
    private int[] index;
    private int lastOffset = 0;
    private int lastStart = 0;

    private final static Logger logger = LoggingInitializer.getLogger("vertex-data");

    public VertexData(int nvertices, String baseFilename,
                      BytesToValueConverter<VertexDataType> converter, boolean sparse) throws IOException {
        this.baseFilename = baseFilename;
        this.converter = converter;
        this.sparse = sparse;

        File vertexfile = new File(ChiFilenames.getFilenameOfVertexData(baseFilename, converter, sparse));
        File sparseDegreeFile = new File(ChiFilenames.getFilenameOfDegreeData(baseFilename, true));
        if (sparse && !sparseDegreeFile.exists()) {
            sparse = false;
            logger.warning("Sparse vertex data was allowed but degree data was not false - using dense");
        }

        if (!sparse) {
            long expectedSize = (long) converter.sizeOf() * (long) nvertices;

            // Check size and create if does not exists
            System.out.println("Vertex file length: " + vertexfile.length() + ", nvertices=" + nvertices
                    + ", expected size: " + expectedSize);
            if (!vertexfile.exists() || vertexfile.length() < expectedSize) {
                if (!vertexfile.exists()) {
                    vertexfile.createNewFile();
                }
                System.out.println("Vertex data file did not exists, creating it. Vertices: " + nvertices);
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
            if (!vertexfile.exists()) {
                BufferedDataInputStream dis = new BufferedDataInputStream(new FileInputStream(sparseDegreeFile));
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(vertexfile)));

                byte[] empty = new byte[converter.sizeOf()];
                try {
                    while(true) {
                        int vertexId = Integer.reverseBytes(dis.readInt());
                        dis.skipBytes(8);
                        dos.writeInt(Integer.reverseBytes(vertexId));
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

    public void releaseAndCommit(int firstVertex, int blockId) throws IOException {
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
        } else {
            vertexDataFile.seek(lastOffset);
            int sizeOf = converter.sizeOf();
            for(int i=0; i < index.length; i++) {
                vertexDataFile.writeInt(index[i]);
                vertexDataFile.write(data, i * sizeOf, sizeOf);
            }
        }
    }

    /**
     * Load vertices
     * @param _vertexSt
     * @param _vertexEn inclusive
     * @return
     * @throws IOException
     */
    public int load(int _vertexSt, int _vertexEn) throws IOException {

        vertexSt = _vertexSt;
        vertexEn = _vertexEn;

        if (!sparse) {
            long dataSize = (long) (vertexEn - vertexSt + 1) *  (long)  converter.sizeOf();
            long dataStart =  (long) vertexSt *  (long) converter.sizeOf();

            int blockId =  blockManager.allocateBlock((int) dataSize);
            synchronized (vertexDataFile) {
                vertexData = blockManager.getRawBlock(blockId);
                vertexDataFile.seek(dataStart);
                vertexDataFile.readFully(vertexData);
            }
            return blockId;
        } else {
            // Have to read in two passes
            if (lastStart > _vertexSt) {
                vertexDataFile.seek(0);
            }

            int sizeOf = converter.sizeOf();
            long startPos = vertexDataFile.getFilePointer();
            int n = 0;
            boolean foundStart = false;
            try {
                while(true) {
                   int vertexId = vertexDataFile.readInt();
                   if (!foundStart && vertexId >= _vertexSt) {
                       startPos = vertexDataFile.getFilePointer() - 4;
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

            index = new int[n];
            vertexDataFile.seek(startPos);
            int blockId =  blockManager.allocateBlock(n * sizeOf);
            vertexData = blockManager.getRawBlock(blockId);

            int i = 0;
            try {
                while(true) {
                    int vertexId = vertexDataFile.readInt();

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

    public ChiPointer getVertexValuePtr(int vertexId, int blockId) {
        assert(vertexId >= vertexSt && vertexId <= vertexEn);
        if (!sparse) {
            return new ChiPointer(blockId, (vertexId - vertexSt) * converter.sizeOf());
        } else {
            int idx = Arrays.binarySearch(index, vertexId);
            if (idx < 0) return null;
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
}
