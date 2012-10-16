package com.twitter.pers.multicomp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains information about a computation. Used when running
 * many computations in parallel.
 * @author Aapo Kyrola, akyrola@cs.cmu.edu, akyrola@twitter.com
 */
public class ComputationInfo {

    private int id;
    private File inputFile;
    private String name;


    public ComputationInfo(int id, File inputFile, String name) {
        this.id = id;
        this.inputFile = inputFile;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public File getInputFile() {
        return inputFile;
    }

    public String getName() {
        return name;
    }

    public static List<ComputationInfo> loadComputations(String file) throws IOException {
        BufferedReader rd = new BufferedReader(new FileReader(file));
        File dir = new File(file).getParentFile();
        String ln;
        ArrayList<ComputationInfo> infos = new ArrayList<ComputationInfo>();
        while((ln = rd.readLine()) != null) {
            String[] toks = ln.split("\t");
            if (toks.length == 2) {
                ComputationInfo ci = new ComputationInfo(infos.size(), new File(dir, toks[1]), toks[0]);
                infos.add(ci);
                System.out.println("Initialized: " + ci);
            }
        }

        rd.close();
        return infos;
    }

    public String toString() {
        return "Computation " + id + ", name=" + name + ", inputfile=" + inputFile.getAbsolutePath();
    }
}
