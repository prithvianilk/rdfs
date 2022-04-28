package com.rdfs.namenode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Arrays;

import org.redisson.api.RedissonClient;
import org.redisson.api.RMap;
import org.redisson.api.RList;

import com.rdfs.NodeLocation;
import com.rdfs.BlockReplicasLocation;
import com.rdfs.Constants;

public class DataNodeLocationStore {
    private static DataNodeLocationStore store = null;
    private RList<NodeLocation> dataNodes;
    private RMap<String, ArrayList<NodeLocation[]>> fileNameBlockLocationMap; 

    private DataNodeLocationStore() {
        RedissonClient redisClient = RedisSingleton.getRedis().getClient();
        dataNodes = redisClient.getList("datanode-list");
        fileNameBlockLocationMap = redisClient.getMap("file-location-map");
    }

    public static DataNodeLocationStore getDataNodeLocationStore() {
        if (store == null) {
            store = new DataNodeLocationStore();
        }
        return store;
    }

    public void addDataNode(NodeLocation nodeLocation) {
        for (NodeLocation currentNodeLocation: dataNodes) {
            if (nodeLocation.equals(currentNodeLocation)) {
                return ;
            }
        }
        dataNodes.add(nodeLocation);
    }

    public NodeLocation[] getDataNodeLocations(String fileName) {
        ArrayList<NodeLocation[]> blockLocations = fileNameBlockLocationMap.get(fileName);
        int numberOfBlocks = blockLocations.size();
        NodeLocation[] nodeLocations = new NodeLocation[numberOfBlocks];
        for (int i = 0; i < numberOfBlocks; ++i) {
            NodeLocation[] replicaNodeLocations = blockLocations.get(i);
            var firstLocation = replicaNodeLocations[0];
            nodeLocations[i] = firstLocation;
        }
        return nodeLocations;
    }

    public NodeLocation[] addBlockNodeLocations(String fileName) {
        var newDataNodeLocations = selectNewBlockLocations();
        var blockLocations = fileNameBlockLocationMap.get(fileName);
        if (blockLocations == null) {
            blockLocations = new ArrayList<NodeLocation[]>();
        }
        blockLocations.add(newDataNodeLocations);
        fileNameBlockLocationMap.put(fileName, blockLocations);
        return newDataNodeLocations;
    }

    private NodeLocation[] selectNewBlockLocations() {
        //TODO get this from option
        int replicationFactor = Constants.DEFAULT_REPLICATION_FACTOR;
        var randomLocations = new NodeLocation[replicationFactor];
        ArrayList<NodeLocation> dataNodesCopy = new ArrayList<>();
        for (NodeLocation nodeLocation: dataNodes) {
            dataNodesCopy.add(nodeLocation.clone());
        }
        Random random = new Random();
        for (int i = 0; i < replicationFactor; ++i) {
            int numberOfDataNodes = dataNodesCopy.size();
            int randomIndex = random.nextInt(numberOfDataNodes);
            var randomNodeLocation = dataNodesCopy.get(randomIndex);
            randomLocations[i] = randomNodeLocation.clone();
            dataNodesCopy.remove(randomNodeLocation);
            System.out.println("Choice: " + randomNodeLocation.toString());
        }
        return randomLocations;
    }

    public void deleteFileMetaData(String fileName) {
        fileNameBlockLocationMap.remove(fileName);
    }

    public BlockReplicasLocation[] getBlockLocations(String fileName) {
        ArrayList<NodeLocation[]> blockLocationsArrayList = fileNameBlockLocationMap.get(fileName);
        int numberOfBlocks = blockLocationsArrayList.size();
        BlockReplicasLocation[] blockReplicasLocations = new BlockReplicasLocation[numberOfBlocks];
        for (int i = 0; i < numberOfBlocks; ++i) {
            NodeLocation[] dataNodeLocations = blockLocationsArrayList.get(i);
            BlockReplicasLocation blockReplicasLocation = new BlockReplicasLocation();
            blockReplicasLocation.dataNodeLocations = dataNodeLocations;
            blockReplicasLocations[i] = blockReplicasLocation;
        }
        return blockReplicasLocations;
    }
}
