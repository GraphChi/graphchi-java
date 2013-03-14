package edu.cmu.graphchi.util;

import java.util.Random;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;

/**
 * A huge dense matrix, which internally splits to many sub-blocks
 * Row-directed storage, so scanning row by row is efficient.  This is useful
 * in keeping all vertex-values in memory efficiently.
 * @author akyrola
 */
public class HugeDoubleMatrix implements Cloneable {

    private int BLOCKSIZE = 1024 * 1024 * 16; // 16M * 4 = 64 megabytes
    private long nrows, ncols;
    private double[][] data;


    public HugeDoubleMatrix(long nrows, long ncols, double initialValue) {
        this.nrows = (long)nrows;
        this.ncols = (long)ncols;

        while(BLOCKSIZE % ncols != 0) BLOCKSIZE++;

        long elements = nrows * ncols;
        int nblocks = (int) (elements / (long)BLOCKSIZE + (elements % BLOCKSIZE == 0 ? 0 : 1));
        data = new double[nblocks][];

        System.out.println("Creating " + nblocks + " blocks");
        for(int i=0; i<nblocks; i++) {
            data[i] = new double[BLOCKSIZE];

            if (initialValue != 0.0f) {
                double[] mat = data[i];
                for(int j=0; j<BLOCKSIZE; j++) {
                    mat[j] = initialValue;
                }
            }
        }
    }

    public HugeDoubleMatrix(long nrows, long ncols) {
        this(nrows, ncols, 0.0);
    }

    public long size() {
        return nrows * ncols;
    }

    public long getNumRows() {
        return nrows;
    }

    public double getValue(int row, int col) {
        long idx = (long)row * ncols + (long)col;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        return data[block][blockidx];
    }

    public void setValue(int row, int col, double val) {
        long idx = (long)row * ncols + (long)col;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        data[block][blockidx] = val;
    }

    public void add(int row, int col, double delta) {
        long idx = (long)row * ncols + (long)col;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        data[block][blockidx] += delta;
    }

    // Premature optimization
    public double[] getRowBlock(int row) {
        long idx = (long)row * ncols;
        int block = (int) (idx / BLOCKSIZE);
        return data[block];
    }

    public int getBlockIdx(int row) {
        long idx = (long)row * ncols;
        int blockidx = (int) (idx % BLOCKSIZE);
        return blockidx;
    }

    public double[] getEmptyRow() {
        double[] arr = new double[(int)ncols];
        return arr;
    }

    public void multiplyRow(int row, float mul) {
        for(int i=0; i<ncols; i++) {
            setValue(row, i, getValue(row, i) * mul); // TODO make faster
        }
    }

    public void getRow(int row, double[] arr) {
        long idx = (long)row * ncols;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        System.arraycopy(data[block], blockidx, arr, 0, (int)ncols);
    }
    
    public RealVector getRowAsVector(int row) {
    	double [] arr = new double[(int) ncols];
        long idx = (long)row * ncols;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        System.arraycopy(data[block], blockidx, arr, 0, (int)ncols);
        return new ArrayRealVector(arr);
    }
    

    public void setRow(int row, double[] arr) {
        long idx = (long)row * ncols;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        System.arraycopy(arr, 0, data[block], blockidx, (int)ncols);
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
            double x = getValue(j, col);
            setValue(j, col, x / div);
        }
    }


    public void setColumn(int col, double val) {
        for(int j=0; j < nrows; j++) {
            this.setValue(j, col, val);
        }
    }

    /**
     * Sets every value less than a cutoff value to zero.
     * @param cutOff
     */
    public void zeroLessThan(float cutOff) {
        for(int i=0; i < data.length; i++) {
            double[] block = data[i];
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
    public void binaryFilter(double cutOff, double value) {
        for(int i=0; i < data.length; i++) {
            double[] block = data[i];
            for(int j=0; j < block.length; j++) {
                block[j] = (block[j] >= cutOff ? 1.0 : 0.0) * value;
            }
        }
    }

    /**
     * Randomize the content with numbers between from and to
     * @param from min value
     * @param to max value
     */
    public void randomize(double from, double to) {
        Random r = new Random();
        for(int i=0; i < data.length; i++) {
            double[] block = data[i];
            for(int j=0; j < block.length; j++) {
                block[j] = from + (to - from) * r.nextDouble();
            }
        }
    }
}