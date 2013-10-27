package edu.cmu.graphchi.engine.auxdata;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import ucar.unidata.io.RandomAccessFile;

import java.io.EOFException;
import java.io.File;
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


/**
 * GraphChi keeps track of the degree of each vertex (count of in- and out-edges). This class
 * allows accessing the vertex degrees efficiently by loading a window
 * of the edges a time. This supports both sparse and dense representation
 * of the degrees. This class should not be needed by application writers, and
 * is only used internally by GraphChi.
 * @author Aapo Kyrola
 */
public class DegreeData {

    private RandomAccessFile degreeFile;

    private byte[] degreeData;
    private int vertexSt, vertexEn;

    private boolean sparse = false;
    private int lastQuery = 0, lastId = -1;

    public DegreeData(String baseFilename) throws IOException {
        File sparseFile = new File(ChiFilenames.getFilenameOfDegreeData(baseFilename, true));
        File denseFile = new File(ChiFilenames.getFilenameOfDegreeData(baseFilename, false));

        if (sparseFile.exists()) {
            sparse = true;
            degreeFile = new RandomAccessFile(sparseFile.getAbsolutePath(), "r");
        } else {
            sparse = false;
            degreeFile = new RandomAccessFile(denseFile.getAbsolutePath(), "r");
        }
        vertexEn = vertexSt = 0;
    }

    /**
     * Load degrees for an interval of vertices
     * @param _vertexSt first vertex
     * @param _vertexEn last vertex (inclusive)
     * @throws IOException
     */
    public void load(int _vertexSt, int _vertexEn) throws IOException {

        int prevVertexEn = vertexEn;
        int prevVertexSt = vertexSt;

        vertexSt = _vertexSt;
        vertexEn = _vertexEn;

        long dataSize =  (long)  (vertexEn - vertexSt + 1) * 4 * 2;

        byte[] prevData = degreeData;
        degreeData = new byte[(int)dataSize];
        int len = 0;

        // Little bit cmoplicated book keeping to avoid redundant reads
        if (prevVertexEn > _vertexSt && prevVertexSt <= _vertexSt) {
            // Copy previous
            len = (int) ((prevVertexEn - vertexSt + 1) * 8);
            System.arraycopy(prevData, prevData.length - len, degreeData, 0, len);
        }


        if (!sparse) {
            int adjLen = (int) (dataSize - len);

            if (adjLen == 0) return;
            long dataStart =  (long)  vertexSt * 4l * 2l + len;

            try {
                degreeFile.seek(dataStart);
                degreeFile.readFully(degreeData, (int)(dataSize - adjLen), adjLen);
            } catch (EOFException eof) {
            	ChiLogger.getLogger("engine").info("Error: Tried to read past file: " + dataStart + " --- " + (dataStart + dataSize));
                // But continue
            }
        } else {
            if (lastQuery > _vertexSt) {
                lastId = -1;
                degreeFile.seek(0);
            }

            try {
                while(true) {
                    int vertexId = (lastId < 0 ? degreeFile.readInt() : lastId);
                    if (vertexId >= _vertexSt && vertexId <= _vertexEn) {
                        degreeFile.readFully(degreeData, (vertexId - vertexSt) * 8, 8);
                        VertexDegree deg = getDegree(vertexId);
                        lastId = -1;
                    } else if (vertexId > vertexEn){
                        lastId = vertexId; // Remember last one read
                        break;
                    } else {
                        degreeFile.skipBytes(8);
                    }
                }
            } catch (EOFException eof) {
                degreeFile.seek(0);
            }
            lastQuery = _vertexEn;
        }
    }

    /**
     * Returns degree of a vertex. The vertex must be in the previous
     * interval loaded using load().
     * @param vertexId id of the vertex
     * @return  VertexDegree object
     */
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
