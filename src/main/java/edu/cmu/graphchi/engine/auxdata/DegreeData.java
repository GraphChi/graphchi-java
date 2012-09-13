package edu.cmu.graphchi.engine.auxdata;

import edu.cmu.graphchi.ChiFilenames;
import ucar.unidata.io.RandomAccessFile;

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
public class DegreeData {

    private String baseFilename;
    private RandomAccessFile degreeFile;

    private byte[] degreeData;
    private int vertexSt, vertexEn;

    public DegreeData(String baseFilename) throws IOException {
        this.baseFilename = baseFilename;
        degreeFile = new RandomAccessFile(ChiFilenames.getFilenameOfDegreeData(baseFilename), "r");
        vertexEn = vertexSt = 0;
    }

    public void load(int _vertexSt, int _vertexEn) throws IOException {
        vertexSt = _vertexSt;
        vertexEn = _vertexEn;

        long dataSize =  (long)  (vertexEn - vertexSt + 1) * 4 * 2;
        long dataStart =  (long)  vertexSt * 4l * 2l;

        degreeData = new byte[(int) dataSize];

        degreeFile.seek(dataStart);
        degreeFile.readFully(degreeData);
    }

    public VertexDegree getDegree(int vertexId) {
        assert(vertexId >= vertexSt && vertexId <= vertexEn);

        byte[] tmp = new byte[4];
        int idx = vertexId - vertexSt;
        System.arraycopy(degreeData, idx * 8, tmp, 0, 4);

        int indeg = ((tmp[3]  & 0xff) << 24) + ((tmp[2] & 0xff) << 16) + ((tmp[1] & 0xff) << 8) + (tmp[0] & 0xff);

        System.arraycopy(degreeData, idx * 8 + 4, tmp, 0, 4);
        int outdeg = ((tmp[3]  & 0xff) << 24) + ((tmp[2] & 0xff) << 16) + ((tmp[1] & 0xff) << 8) + (tmp[0] & 0xff);

        return new VertexDegree(indeg, outdeg);
    }
}
