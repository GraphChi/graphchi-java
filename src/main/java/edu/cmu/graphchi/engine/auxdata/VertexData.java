package edu.cmu.graphchi.engine.auxdata;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import ucar.unidata.io.RandomAccessFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

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

    public VertexData(int nvertices, String baseFilename, BytesToValueConverter<VertexDataType> converter) throws IOException {
        this.baseFilename = baseFilename;
        this.converter = converter;

        long expectedSize = (long) converter.sizeOf() * (long) nvertices;

        // Check size and create if does not exists
        File vertexfile = new File(ChiFilenames.getFilenameOfVertexData(baseFilename, converter));
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


        vertexDataFile = new RandomAccessFile(ChiFilenames.getFilenameOfVertexData(baseFilename, converter), "rwd");
        vertexEn = vertexSt = 0;


    }

    public void releaseAndCommit(int firstVertex, int blockId) throws IOException {
        assert(blockId >= 0);
        byte[] data = blockManager.getRawBlock(blockId);
        long dataStart = (long) firstVertex * (long) converter.sizeOf();

        synchronized (vertexDataFile) {
            vertexDataFile.seek(dataStart);
            vertexDataFile.write(data);

            blockManager.release(blockId);

            vertexDataFile.flush();
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

        long dataSize = (long) (vertexEn - vertexSt + 1) *  (long)  converter.sizeOf();
        long dataStart =  (long) vertexSt *  (long) converter.sizeOf();

        int blockId =  blockManager.allocateBlock((int) dataSize);
        synchronized (vertexDataFile) {
            vertexData = blockManager.getRawBlock(blockId);
            vertexDataFile.seek(dataStart);
            vertexDataFile.readFully(vertexData);
        }
        return blockId;
    }

    public ChiPointer getVertexValuePtr(int vertexId, int blockId) {
        assert(vertexId >= vertexSt && vertexId <= vertexEn);
        return new ChiPointer(blockId, (vertexId - vertexSt) * converter.sizeOf());
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
