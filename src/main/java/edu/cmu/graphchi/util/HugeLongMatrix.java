package edu.cmu.graphchi.util;


/**
 * A huge dense matrix, which internally splits to many sub-blocks
 * Row-directed storage, so scanning row by row is efficient.  This is useful
 * in keeping all vertex-values in memory efficiently.
 * @author akyrola
 */
public class HugeLongMatrix {

    private int BLOCKSIZE = 1024 * 1024 * 16; // 16M * 4 = 64 megabytes
    private long nrows, ncols;
    private long[][] data;

    public HugeLongMatrix(long nrows, long ncols, long initialValue) {
        this.nrows = (long)nrows;
        this.ncols = (long)ncols;

        while(BLOCKSIZE % ncols != 0) BLOCKSIZE++;

        long elements = nrows * ncols;
        int nblocks = (int) (elements / (long)BLOCKSIZE + (elements % BLOCKSIZE == 0 ? 0 : 1));
        data = new long[nblocks][];

        System.out.println("Creating " + nblocks + " blocks");
        for(int i=0; i<nblocks; i++) {
            data[i] = new long[BLOCKSIZE];

            if (initialValue != 0.0f) {
                long[] mat = data[i];
                for(int j=0; j<BLOCKSIZE; j++) {
                    mat[j] = initialValue;
                }
            }
        }
    }

    public HugeLongMatrix(long nrows, long ncols) {
        this(nrows, ncols, 0l);
    }

    public long size() {
        return nrows * ncols;
    }

    public long getNumRows() {
        return nrows;
    }

    public long getValue(int row, int col) {
        long idx = (long)row * ncols + (long)col;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        return data[block][blockidx];
    }

    public void setValue(int row, int col, long val) {
        long idx = (long)row * ncols + (long)col;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        data[block][blockidx] = val;
    }

    // Premature optimization
    public long[] getRowBlock(int row) {
        long idx = (long)row * ncols;
        int block = (int) (idx / BLOCKSIZE);
        return data[block];
    }

    public int getBlockIdx(int row) {
        long idx = (long)row * ncols;
        int blockidx = (int) (idx % BLOCKSIZE);
        return blockidx;
    }

    public long[] getEmptyRow() {
        long[] arr = new long[(int)ncols];
        return arr;
    }



    public void getRow(int row, long[] arr) {
        long idx = (long)row * ncols;
        int block = (int) (idx / BLOCKSIZE);
        int blockidx = (int) (idx % BLOCKSIZE);
        System.arraycopy(data[block], blockidx, arr, 0, (int)ncols);
    }


}
