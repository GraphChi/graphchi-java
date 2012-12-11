package edu.cmu.graphchi.preprocessing;

import java.io.*;

public class ShardEdgeListFromPipe {

    public static void main(String[] args) throws IOException {
        FastSharder sharder = new FastSharder("pipein", 80, 4);

        InputStream inputStream = (args.length == 0 ? System.in : new FileInputStream(args[0]));

        BufferedReader ins = new BufferedReader(new InputStreamReader(inputStream));
        String ln;
        long lineNum = 0;
        while ((ln = ins.readLine()) != null) {
            if (ln.length() > 2 && !ln.startsWith("#")) {
                lineNum++;
                if (lineNum % 2000000 == 0) System.out.println(lineNum);
                String[] tok = ln.split("\t");
                if (tok.length == 2) {
                    sharder.addEdge(Integer.parseInt(tok[0]), Integer.parseInt(tok[1]));
                }
            }
        }
        sharder.process();

    }

}
