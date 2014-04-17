package edu.cmu.graphchi.datablocks;

/**
 * Represents a 2-tuple of floats.
 * Access the tuple elements by pair.first, pair.second.
 *
 * @author Aapo Kyrola
 */
public class FloatPair {
    public final float first;
    public final float second;

    public FloatPair(float first, float second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FloatPair floatPair = (FloatPair) o;
        return(floatPair.first == first && floatPair.second == second);
    }

    @Override
    public int hashCode() {
        int result = (first != +0.0f ? Float.floatToIntBits(first) : 0);
        result = 31 * result + (second != +0.0f ? Float.floatToIntBits(second) : 0);
        return result;
    }
}
