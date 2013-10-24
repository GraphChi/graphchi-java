package edu.cmu.graphchi.walks.distributions;

import edu.cmu.graphchi.util.IdCount;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.rmi.Naming;

/**
 * Client for querying the DrunkardCompanion.
 *
 */
public class DrunkardClient {

    public static void main(String[] args) throws Exception {
        String hostAddress = args[0];
        String namingAddress = args[1];

        RemoteDrunkardCompanion companion = (RemoteDrunkardCompanion) Naming.lookup(hostAddress);

        BufferedReader cmd = new BufferedReader(new InputStreamReader(System.in));
        String ln;
        while ((ln = cmd.readLine()) != null) {
            if (ln.startsWith("q")) return;
            int vertexId = Integer.parseInt(ln);
            try {
                IdCount[] top = companion.getTop(vertexId, 10);

                System.out.println("Result:");
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
