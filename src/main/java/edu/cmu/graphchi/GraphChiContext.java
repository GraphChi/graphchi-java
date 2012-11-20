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
public class GraphChiContext {

    private Scheduler scheduler;
    private VertexInterval curInterval;

    public GraphChiContext() {}

    public VertexInterval getCurInterval() {
        return curInterval;
    }

    public void setCurInterval(VertexInterval curInterval) {
        this.curInterval = curInterval;
    }

    public int getIteration() {
        return iteration;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public int getNumIterations() {
        return numIterations;
    }

    public void setNumIterations(int numIterations) {
        this.numIterations = numIterations;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public long getNumEdges() {
        return numEdges;
    }

    public void setNumEdges(long numEdges) {
        this.numEdges = numEdges;
    }

    public long getNumVertices() {
        return numVertices;
    }

    public void setNumVertices(long numVertices) {
        this.numVertices = numVertices;
    }

    private int iteration;
    private int numIterations;
    private long numEdges;
    private long numVertices;



}
