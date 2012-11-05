package edu.cmu.graphchi.util;

/**
 * A huge dense matrix, which internally splits to many subblocks
 * Row-directed storage, so scanning row by row is efficient.
 * @author akyrola
 *
 */
public class HugeFloatMatrix {

    private int BLOCKSIZE = 1024 * 1024 * 16; // 16M * 4 = 64 megabytes
    private long nrows, ncols;
    private float[][] data;

    public HugeFloatMatrix(long nrows, long ncols, float initialValue) {
        this.nrows = (long)nrows;
        this.ncols = (long)ncols;

        while(BLOCKSIZE % ncols != 0) BLOCKSIZE++;

        long elements = nrows * ncols;
        int nblocks = (int) (elements / (long)BLOCKSIZE + (elements % BLOCKSIZE == 0 ? 0 : 1));
        data = new float[nblocks][];

        System.out.println("Creating " + nblocks + " blocks");
        for(int i=0; i<nblocks; i++) {
            data[i] = new float[BLOCKSIZE];

            if (initialValue != 0.0f) {
                float[] mat = data[i];
                for(int j=0; j<BLOCKSIZE; j++) {
                    mat[j] = initialValue;
                }
            }
        }
    }

    public HugeFloatMatrix(long nrows, long ncols) {
        this(nrows, ncols, 0.0f);
    }

    public long size() {
        return nrows * ncols;
    }

    public long getNumRows() {
        return nrows;
    }

    public float getValue(int row, int col) {
        long idx = (long)row * ncols + (long)col;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        return data[block][blockidx];
    }

    public void setValue(int row, int col, float val) {
        long idx = (long)row * ncols + (long)col;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        data[block][blockidx] = val;
    }

    // Premature optimization
    public float[] getRowBlock(int row) {
        long idx = (long)row * ncols;
        int block = (int) (idx / BLOCKSIZE);
        return data[block];
    }

    public int getBlockIdx(int row) {
        long idx = (long)row * ncols;
        int blockidx = (int) (idx % BLOCKSIZE);
        return blockidx;
    }

    public float[] getEmptyRow() {
        float[] arr = new float[(int)ncols];
        return arr;
    }

    public void multiplyRow(int row, float mul) {
        for(int i=0; i<ncols; i++) {
            setValue(row, i, getValue(row, i) * mul); // TODO make faster
        }
    }

    public void getRow(int row, float[] arr) {
        long idx = (long)row * ncols;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        System.arraycopy(data[block], blockidx, arr, 0, (int)ncols);
    }


    /** Divides each item by the sum of squares */
    public void normalizeSquared(int col) {
        double sqr = 0.0f;
        for(int j=0; j < nrows; j++) {
            double x = (double) getValue(j, col);
            sqr += x * x;
        }
        System.out.println("Normalize-squared: " + col + " sqr: " + sqr);

        float div = (float) Math.sqrt(sqr);
        System.out.println("Div : " + div);

        if (Float.isInfinite(div) || Float.isNaN(div)) throw new RuntimeException("Illegal normalizer: " + div);

        if (sqr == 0.0f) throw new IllegalArgumentException("Column was all-zeros!");
        for(int j=0; j < nrows; j++) {
            float x = getValue(j, col);
            setValue(j, col, x / div);
        }
    }

    /**
     * Sets every value less than a cutoff value to zero.
     * @param cutOff
     */
    public void zeroLessThan(float cutOff) {
        for(int i=0; i < data.length; i++) {
            float[] block = data[i];
            for(int j=0; j < block.length; j++) {
                if (block[j] != 0.0f && block[j] < cutOff) block[j] = 0.0f;
            }
        }
    }

    /**
     * Sets all values less than cutOff to zero, everyone else to value
     * @param cutOff
     * @param value
     */
    public void binaryFilter(float cutOff, float value) {
        for(int i=0; i < data.length; i++) {
            float[] block = data[i];
            for(int j=0; j < block.length; j++) {
                block[j] = (block[j] >= cutOff ? 1.0f : 0.0f) * value;
            }
        }
    }

}