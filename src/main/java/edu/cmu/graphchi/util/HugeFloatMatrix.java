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
    
    public HugeFloatMatrix(long nrows, long ncols) {
    	this.nrows = (long)nrows;
    	this.ncols = (long)ncols;
    	
    	while(BLOCKSIZE % ncols != 0) BLOCKSIZE++;
    	
    	long elements = nrows * ncols;
    	int nblocks = (int) (elements / (long)BLOCKSIZE + (elements % BLOCKSIZE == 0 ? 0 : 1));
    	data = new float[nblocks][];
    	
    	System.out.println("Creating " + nblocks + " blocks");
    	for(int i=0; i<nblocks; i++) {
    		data[i] = new float[BLOCKSIZE];
    	}
    }
    
    public long size() {
    	return nrows * ncols;
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
    
    public void getRow(int row, float[] arr) {
    	long idx = (long)row * ncols;
    	int block = (int) (idx / BLOCKSIZE);
    	int blockidx = (int) (idx % BLOCKSIZE);
    	System.arraycopy(data[block], blockidx, arr, 0, (int)ncols);
    }
	
}
