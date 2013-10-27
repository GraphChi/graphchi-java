package edu.cmu.graphchi.engine.auxdata;

import edu.cmu.graphchi.ChiFilenames;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;

/**
 * @author Aapo Kyrola
 */
public class TestDegreeData {

    public static void main(String[] args) throws Exception {
        String baseFilename = args[0];
        DegreeData d = new DegreeData(baseFilename);

        File f = new File(ChiFilenames.getFilenameOfDegreeData(baseFilename, true));
        DataInputStream dis = new DataInputStream(new FileInputStream(f));

        try {
            while(true) {
                long vertexId  = Long.reverseBytes(dis.readLong());
                int inDeg = Integer.reverseBytes(dis.readInt());
                int outDeg = Integer.reverseBytes(dis.readInt());
                System.out.println(vertexId + " " + inDeg + " " + outDeg);
            }
        } catch (EOFException e) {
            System.out.println("Finished");
        }
    }
}
