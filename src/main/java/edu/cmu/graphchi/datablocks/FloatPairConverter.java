package edu.cmu.graphchi.datablocks;

/**
 * @author Aapo Kyrola, akyrola@twitter.com, akyrola@cs.cmu.edu
 */

public class FloatPairConverter implements  BytesToValueConverter<FloatPair> {
    public int sizeOf() {
        return 8;
    }

    public FloatPair getValue(byte[] array) {
        int x = ((array[0]  & 0xff) << 24) + ((array[1] & 0xff) << 16) + ((array[2] & 0xff) << 8) + (array[3] & 0xff);
        int y = ((array[4]  & 0xff) << 24) + ((array[5] & 0xff) << 16) + ((array[6] & 0xff) << 8) + (array[7] & 0xff);
        return new FloatPair(Float.intBitsToFloat(x), Float.intBitsToFloat(y));
    }

    public void setValue(byte[] array, FloatPair val) {
        int x = Float.floatToIntBits(val.first);
        array[0] = (byte) ((x >>> 24) & 0xff);
        array[1] = (byte) ((x >>> 16) & 0xff);
        array[2] = (byte) ((x >>> 8) & 0xff);
        array[3] = (byte) ((x >>> 0) & 0xff);
        int y = Float.floatToIntBits(val.second);
        array[4] = (byte) ((y >>> 24) & 0xff);
        array[5] = (byte) ((y >>> 16) & 0xff);
        array[6] = (byte) ((y >>> 8) & 0xff);
        array[7] = (byte) ((y >>> 0) & 0xff);
    }
}