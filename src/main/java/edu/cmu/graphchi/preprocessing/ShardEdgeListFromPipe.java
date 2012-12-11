package edu.cmu.graphchi.preprocessing;

import java.io.*;

public class ShardEdgeListFromPipe {

    public static void main(String[] args) throws IOException {
        FastSharder sharder = new FastSharder("pipein", 5, 4);

        InputStream inputStream = (args.length == 0 ? System.in : new FileInputStream(args[0]));

        sharder.shard(inputStream);

    }

}
