package com.twitter.pers.graphchi.walks.distributions;

import com.twitter.pers.graphchi.walks.RMIHack;
import com.twitter.pers.util.RemoteVertexNameService;
import edu.cmu.graphchi.util.IdCount;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.TreeSet;

/**
 * Client for querying
 */
public class DrunkardClient {

    public static void main(String[] args) throws Exception {
        String hostAddress = args[0];
        String namingAddress = args[1];
        if (hostAddress.contains("localhost")) {
            RMIHack.setupLocalHostTunneling();
        }
        RemoteDrunkardCompanion companion = (RemoteDrunkardCompanion) Naming.lookup(hostAddress);
        RemoteVertexNameService vns = (RemoteVertexNameService) Naming.lookup(namingAddress);

        BufferedReader cmd = new BufferedReader(new InputStreamReader(System.in));
        String ln;
        while ((ln = cmd.readLine()) != null) {
            if (ln.startsWith("q")) return;
            int vertexId = Integer.parseInt(ln);
            try {
                IdCount[] top = companion.getTop(vertexId);

                ArrayList<Integer> ids = new ArrayList<Integer>();
                ids.add(vertexId);
                for(IdCount ic : top) ids.add(ic.id);
                ArrayList<String> names = vns.namify(ids);

                int j = 1;
                System.out.println("Result for " + names.get(0));
                for(IdCount ic : top) {
                    System.out.println(names.get(j++) + ":" + ic.id + ": " + ic.count);
                }
            } catch (Exception err) {
                err.printStackTrace();
            }
        }

        cmd.close();
    }

}
