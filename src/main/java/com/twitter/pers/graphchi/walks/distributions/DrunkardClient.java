package com.twitter.pers.graphchi.walks.distributions;

import com.twitter.pers.graphchi.walks.RMIHack;
import edu.cmu.graphchi.util.IdCount;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.Naming;
import java.util.TreeSet;

/**
 * Client for querying
 */
public class DrunkardClient {

    public static void main(String[] args) throws Exception {
        String hostAddress = args[0];
        if (hostAddress.contains("localhost")) {
            RMIHack.setupLocalHostTunneling();
        }
        RemoteDrunkardCompanion companion = (RemoteDrunkardCompanion) Naming.lookup(hostAddress);
        BufferedReader cmd = new BufferedReader(new InputStreamReader(System.in));
        String ln;
        while ((ln = cmd.readLine()) != null) {
            if (ln.startsWith("q")) return;
            int vertexId = Integer.parseInt(ln);
            try {
                IdCount[] top = companion.getTop(vertexId);

                for(IdCount ic : top) {
                    System.out.println(ic.id + ": " + ic.count);
                }
            } catch (Exception err) {
                err.printStackTrace();
            }
        }

        cmd.close();
    }

}
