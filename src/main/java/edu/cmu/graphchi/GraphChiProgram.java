package edu.cmu.graphchi;

import edu.cmu.graphchi.engine.VertexInterval;

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
 * All GraphChi applications must extend this class.
 * @param <VertexDataType>
 * @param <EdgeDataType>
 */
public interface GraphChiProgram <VertexDataType, EdgeDataType> {

    /**
     * Called for every (scheduled) vertex
     * @param vertex the current vertex
     * @param context context representing the current state of the computation
     */
    public void update(ChiVertex<VertexDataType, EdgeDataType> vertex, GraphChiContext context);

    /**
     * Called when a new iteration starts
     * @param ctx
     */
    public void beginIteration(GraphChiContext ctx);


    /**
     * Called when an iteration ends
     * @param ctx
     */
    public void endIteration(GraphChiContext ctx);


    /**
     * Called when a new interval of vertices (corresponding to shards, see
     * http://code.google.com/p/graphchi/wiki/IntroductionToGraphChi) is
     * being processed.
     * @param ctx
     * @param interval
     */
    public void beginInterval(GraphChiContext ctx, VertexInterval interval);

    public void endInterval(GraphChiContext ctx, VertexInterval interval);

    /**
     * Called when a new sub-interval of vertices (see
     * http://code.google.com/p/graphchi/wiki/IntroductionToGraphChi) is
     * being processed.
     * @param ctx
     * @param interval
     */
    public void beginSubInterval(GraphChiContext ctx, VertexInterval interval);

    public void endSubInterval(GraphChiContext ctx, VertexInterval interval);

}
