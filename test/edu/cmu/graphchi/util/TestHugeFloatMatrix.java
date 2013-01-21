package edu.cmu.graphchi.util;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHugeFloatMatrix {

	private HugeFloatMatrix matrix;
	
	@Before
	public void setUp() throws Exception {
		matrix = new HugeFloatMatrix(2000, 800);
	}

	@Test
	public void test() {
		for(int i=0; i<2000; i++) {
			for(int j=i%5; j<800; j+= 100) {
				float val = i * 3.9f + j/8.1f;
				matrix.setValue(i, j, val);
			}
		}
		for(int i=0; i<2000; i++) {
			for(int j=i%5; j<800; j+= 100) {
				float val = i * 3.9f + j/8.1f;
				assertEquals(matrix.getValue(i, j), val, 1e-10);
			}
		}
	}

}
