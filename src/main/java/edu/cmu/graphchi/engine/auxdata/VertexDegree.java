package edu.cmu.graphchi.engine.auxdata;

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
 * Represents a vertex in- and out-degree, i.e the number of in-edges
 * and out-edges.
 * @author Aapo Kyrola
 */
public class VertexDegree {
    public int inDegree;
    public int outDegree;

    public VertexDegree(int inDegree, int outDegree) {
        this.inDegree = inDegree;
        this.outDegree = outDegree;
    }

    public String toString() {
        return inDegree + "," + outDegree;
    }
}
