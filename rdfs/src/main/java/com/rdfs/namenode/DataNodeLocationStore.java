package com.rdfs.namenode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import com.rdfs.NodeLocation;
import com.rdfs.Constants;


// should map all file names to datanodes containing blocks
// should be able to query for all available datanodes

public class DataNodeLocationStore {
    private static DataNodeLocationStore store = null;
    private ArrayList<NodeLocation> dataNodes;
    private HashMap<String, ArrayList<ArrayList<NodeLocation>>> fileNameBlockLocationMap; 
    // filename -> list of blocks
    // blocks -> list of locations

    private DataNodeLocationStore() {
        dataNodes = new ArrayList<NodeLocation>();
        fileNameBlockLocationMap = new HashMap<>();
    }

    public static DataNodeLocationStore getDataNodeLocationStore() {
        if (store == null) {
            store = new DataNodeLocationStore();
        }
        return store;
    }

    public ArrayList<NodeLocation> getDataNodeLocations(String fileName) {
        var blockLocations = fileNameBlockLocationMap.get(fileName);
        int numberOfBlocks = blockLocations.size();
        var nodeLocations = new ArrayList<NodeLocation>();
        for (var replicaNodeLocations: blockLocations) {
            var firstLocation = replicaNodeLocations.get(0);
            nodeLocations.add(firstLocation);
        }
        return nodeLocations;
    }

    public ArrayList<NodeLocation> addBlockNodeLocations(String fileName) {
        var newDataNodeLocations = selectNewBlockLocations(fileName);
        var blockLocations = fileNameBlockLocationMap.get(fileName);
        blockLocations.add(newDataNodeLocations);
        return newDataNodeLocations;
    }

    // select random datanode locations for a file (specifically for a block)
    private ArrayList<NodeLocation> selectNewBlockLocations(String fileName) {
        //TODO get this from option
        int replicationFactor = Constants.DEFAULT_REPLICATION_FACTOR;
        var randomLocations = new ArrayList<NodeLocation>(replicationFactor);
        int numberOfDataNodes = dataNodes.size();
        Random random = new Random();
        for (int i = 0; i < replicationFactor; ++i) {
            int randomIndex = random.nextInt(numberOfDataNodes);
            NodeLocation randomNodeLocation = dataNodes.get(randomIndex);
            randomLocations.add(randomNodeLocation);
        }
        return randomLocations;
    }

    public void deleteFileMetaData(String fileName) {
        fileNameBlockLocationMap.remove(fileName);
    }

    public ArrayList<ArrayList<NodeLocation>> getBlockLocations(String fileName) {
        return fileNameBlockLocationMap.get(fileName);
    }
}
