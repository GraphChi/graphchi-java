package edu.cmu.graphchi.util;


/**
 * Container class carrying a vertex id and a float value.
 * @author Aapo Kyrola
 */
public class IdFloat {
    int vertexId;
    float value;

    public IdFloat(int vertexId, float value) {
        this.vertexId = vertexId;
        this.value = value;
    }

    public int getVertexId() {
        return vertexId;
    }

    public float getValue() {
        return value;
    }

    public static class Comparator implements java.util.Comparator<IdFloat> {
        public int compare(IdFloat idFloat, IdFloat idFloat1) {
            if (idFloat.vertexId == idFloat1.vertexId) return 0;
            int comp = -Float.compare(idFloat.value, idFloat1.value); // Descending order
            return (comp != 0 ? comp : (idFloat.vertexId < idFloat1.vertexId ? -1 : 1));
        }

    }

}
