package com.twitter.pers.graphchi.walks.distributions;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author  Aapo Kyrola, akyrola@twitter.com
 */
public interface RemoteDrunkardCompanion extends Remote {

    /**
     * Set a list of ids that should not be counted for vertexId's
     * distribution.
     * @param sourceIdx
     * @param avoidList list (unsorted) of ids to omit
     */
    void setAvoidList(int sourceIdx, int[] avoidList) throws RemoteException;

    void setSources(int[] sources) throws RemoteException;

    void processWalks(int[] walks, int[] atVertices) throws RemoteException;

    void outputDistributions(String outputFile) throws RemoteException;
}
