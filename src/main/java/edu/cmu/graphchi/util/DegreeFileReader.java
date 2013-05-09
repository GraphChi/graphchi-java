package edu.cmu.graphchi.util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;

/**
 * @author Aapo Kyrola
 */
public class DegreeFileReader {

    public static void main(String[] args) throws Exception {
        String degreeFile = args[0];
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(degreeFile)));
        int j = 0;
        long totIn = 0;
        long totOut = 0;
        try {
            while(true) {
                int inDeg = Integer.reverseBytes(dis.readInt());
                int outDeg = Integer.reverseBytes(dis.readInt());
                j++;
                totIn += inDeg;
                totOut += outDeg;

                if (j >= 100000  && j < 200000) {
                    System.out.println(j + ", " + inDeg + ", " + outDeg);
                }
            }
        }catch (EOFException e) {}

        System.out.println(j + " in=" + totIn + " out=" + totOut);
    }
}
