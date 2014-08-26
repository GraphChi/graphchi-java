package edu.cmu.graphchi.walks;

import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

/**
 * @author Aapo Kyrola
 */
public interface DrunkardContext {

    /**
     * Returns whether vertex id is a source of random walks.
     * @return
     */
    boolean isSource();

    /**
     * Returns the index-number of a source vertex.
     * @return source index or -1 if vertex is not a source
     */
    int sourceIndex();


    int getIteration();

    /**
     * Object for translating from internal to original vertex ids
     * @return
     */
    VertexIdTranslate getVertexIdTranslate();
}
