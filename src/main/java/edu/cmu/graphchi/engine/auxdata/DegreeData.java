package edu.cmu.graphchi.engine.auxdata;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import ucar.unidata.io.RandomAccessFile;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
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
    private long vertexSt, vertexEn;

    private boolean sparse = false;
    private long lastQuery = 0, lastId = -1, lastStart = 0;
    private boolean intervalContainsAny = true;
    private boolean hitEnd;
    private final static Logger logger = ChiLogger.getLogger("degree-data");

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
        hitEnd = false;
    }

    public boolean wasLast() {
         return (hitEnd);
    }

    /**
     * Load degrees for an interval of vertices
     * @param _vertexSt first vertex
     * @param _vertexEn last vertex (inclusive)
     * @throws IOException
     */
    public void load(long _vertexSt, long _vertexEn) throws IOException {
    /*    if (sparse && !intervalContainsAny && _vertexSt < lastId && _vertexEn < lastId && _vertexSt >= lastQuery) {
             return; // Nothing to do for sure
        }*/
        hitEnd = false;

        long prevVertexEn = vertexEn;
        long prevVertexSt = vertexSt;

        vertexSt = _vertexSt;
        vertexEn = _vertexEn;

        long dataSize =  (long)  (vertexEn - vertexSt + 1) * 4 * 2;

        byte[] prevData = degreeData;
        degreeData = new byte[(int)dataSize];
        int len = 0;

        logger.info("Load degree: " + vertexSt + " -- " + _vertexEn);

        // Little bit cmoplicated book keeping to avoid redundant reads
        if (prevVertexEn > _vertexSt && prevVertexSt <= _vertexSt) {
            // Copy previous
            len = (int) ((prevVertexEn - vertexSt + 1) * 8);
            System.arraycopy(prevData, prevData.length - len, degreeData, 0, len);
            logger.info("Copied previous data: " + len + " , prevVertexEn:" + prevVertexEn);

        }


        if (!sparse) {
            int adjLen = (int) (dataSize - len);

            if (adjLen == 0){
                return;
            }
            intervalContainsAny = false;

            long dataStart =  (long)  vertexSt * 4l * 2l + len;

            try {
                degreeFile.seek(dataStart);
                degreeFile.readFully(degreeData, (int)(dataSize - adjLen), adjLen);

            } catch (EOFException eof) {
            	ChiLogger.getLogger("engine").info("Error: Tried to read past file: " + dataStart + " --- " + (dataStart + dataSize));
                // But continue
            }
            /* Check if any edges */
            for(long vid=vertexSt; vid<=vertexEn; vid++) {
                if (getDegree(vid).getDegree() > 0) {
                    intervalContainsAny = true;
                    break;
                }
            }
        } else {
            if (lastQuery > _vertexSt) {
                lastId = -1;
                degreeFile.seek(lastStart);
                logger.info("Rewind because lastQuery: " + lastQuery + " > " + _vertexSt + ", lastId:" + lastId + " -->" + lastStart);
            }

            lastStart = 0;
            try {
                intervalContainsAny = false;

                while(true) {
                    long vertexId = (lastId < 0 ? degreeFile.readLong() : lastId);
                    if (vertexId >= _vertexSt && vertexId <= _vertexEn) {
                        if (lastStart == 0 && !intervalContainsAny) {
                            lastStart = degreeFile.getFilePointer() - 8;
                        }
                        degreeFile.readFully(degreeData, (int) (vertexId - vertexSt) * 8, 8);
                        lastId = -1;


                        intervalContainsAny = true;
                    } else if (vertexId > vertexEn){
                        lastId = vertexId; // Remember last one read
                        logger.info("Finished scan, next one will be: " + lastId);
                        break;
                    } else {
                        degreeFile.skipBytes(8);
                    }
                }
            } catch (EOFException eof) {
                hitEnd = true;
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
    public VertexDegree getDegree(long vertexId) {
        assert(vertexId >= vertexSt && vertexId <= vertexEn);

        byte[] tmp = new byte[4];
        int idx = (int) (vertexId - vertexSt);
        System.arraycopy(degreeData, idx * 8, tmp, 0, 4);

        int indeg = ((tmp[3]  & 0xff) << 24) + ((tmp[2] & 0xff) << 16) + ((tmp[1] & 0xff) << 8) + (tmp[0] & 0xff);

        System.arraycopy(degreeData, idx * 8 + 4, tmp, 0, 4);
        int outdeg = ((tmp[3]  & 0xff) << 24) + ((tmp[2] & 0xff) << 16) + ((tmp[1] & 0xff) << 8) + (tmp[0] & 0xff);

        return new VertexDegree(indeg, outdeg);
    }

    public boolean doesIntervalContainAnyEdges() {
        return intervalContainsAny;
    }

    public long next() {
        if (!sparse) throw new IllegalStateException("Can be only called when sparse data!");
        return lastId;
    }

    public boolean sparse() {
        return sparse;
    }
}
