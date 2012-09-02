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
    private int currentBlockId = (-1);
    private DataBlockManager blockManager;

    public VertexData(int nvertices, String baseFilename, BytesToValueConverter<VertexDataType> converter) throws IOException {
        this.baseFilename = baseFilename;
        this.converter = converter;

        // Check size and create if does not exists
        File vertexfile = new File(ChiFilenames.getFilenameOfVertexData(baseFilename, converter));
        if (!vertexfile.exists() || vertexfile.length() < nvertices * converter.sizeOf()) {
            if (!vertexfile.exists()) {
                vertexfile.createNewFile();
            }
            System.out.println("Vertex data file did not exists, creating it.");
            FileOutputStream fos = new FileOutputStream(vertexfile);
            long newSize = converter.sizeOf() * nvertices;
            byte[] tmp = new byte[32678];
            long written = 0;
            while(written < newSize) {
                long n = Math.min(newSize - written, tmp.length);
                fos.write(tmp, 0, (int)n);
                written += n;
            }
            fos.close();
        }


        vertexDataFile = new RandomAccessFile(ChiFilenames.getFilenameOfVertexData(baseFilename, converter), "rwd");
        vertexEn = vertexSt = 0;


    }

    public void releaseAndCommit() throws IOException {
        assert(currentBlockId >= 0);
        byte[] data = blockManager.getRawBlock(currentBlockId);
        int dataStart = vertexSt * converter.sizeOf();

        vertexDataFile.seek(dataStart);
        vertexDataFile.write(data);

        blockManager.release(currentBlockId);
        
        vertexDataFile.flush();
    }

    public void load(int _vertexSt, int _vertexEn) throws IOException {

        vertexSt = _vertexSt;
        vertexEn = _vertexEn;

        int dataSize = (vertexEn - vertexSt + 1) * converter.sizeOf();
        int dataStart = vertexSt * converter.sizeOf();

        currentBlockId =  blockManager.allocateBlock(dataSize);

        vertexData = blockManager.getRawBlock(currentBlockId);
        vertexDataFile.seek(dataStart);
        vertexDataFile.readFully(vertexData);
    }

    public ChiPointer getVertexValuePtr(int vertexId) {
        assert(vertexId >= vertexSt && vertexId <= vertexEn);
        return new ChiPointer(currentBlockId, (vertexId - vertexSt) * converter.sizeOf());
    }

    public void setBlockManager(DataBlockManager blockManager) {
        this.blockManager = blockManager;
    }
}
