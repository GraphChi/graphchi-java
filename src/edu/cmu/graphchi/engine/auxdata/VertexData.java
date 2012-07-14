package edu.cmu.graphchi.engine.auxdata;

import edu.cmu.graphchi.ChiFilenames;

import java.io.File;
import java.io.IOException;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.ChiPointer;
import edu.cmu.graphchi.datablocks.DataBlockManager;
import ucar.unidata.io.RandomAccessFile;
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

    public VertexData(String baseFilename, BytesToValueConverter<VertexDataType> converter) throws IOException {
        this.baseFilename = baseFilename;
        this.converter = converter;
        vertexDataFile = new RandomAccessFile(ChiFilenames.getFilenameOfVertexData(baseFilename, converter), "rwd");
        vertexEn = vertexSt = 0;
    }

    public void releaseAndCommit() throws IOException {
        assert(currentBlockId >= 0);
        byte[] data = blockManager.getRawBlock(currentBlockId);
        int dataStart = vertexSt * converter.sizeOf();

        vertexDataFile.seek(dataStart);
        vertexDataFile.write(data);
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
