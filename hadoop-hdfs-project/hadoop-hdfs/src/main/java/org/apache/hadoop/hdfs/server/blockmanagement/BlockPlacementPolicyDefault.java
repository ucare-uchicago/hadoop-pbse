/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import com.google.common.annotations.VisibleForTesting;

/**
 * The class is responsible for choosing the desired number of targets
 * for placing block replicas.
 * The replica placement strategy is that if the writer is on a datanode,
 * the 1st replica is placed on the local machine, 
 * otherwise a random datanode. The 2nd replica is placed on a datanode
 * that is on a different rack. The 3rd replica is placed on a datanode
 * which is on a different node of the rack as the second replica.
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyDefault extends BlockPlacementPolicy {

  private static final String enableDebugLogging =
    "For more information, please enable DEBUG log level on "
    + BlockPlacementPolicy.class.getName();

  private static final ThreadLocal<StringBuilder> debugLoggingBuilder
      = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  protected boolean considerLoad; 
  //private boolean preferLocalNode = true;
  //huanke not choose local node in function chooseLocalStorage()
  private boolean preferLocalNode = true;
  protected NetworkTopology clusterMap;
  protected Host2NodesMap host2datanodeMap;
  private FSClusterStats stats;
  protected long heartbeatInterval;   // interval for DataNode heartbeats
  private long staleInterval;   // interval used to identify stale DataNodes

  
  /**
   * A miss of that many heartbeats is tolerated for replica deletion policy.
   */
  protected int tolerateHeartbeatMultiplier;

  //huanke
  private Collection<String> slowDataNodes;
  private List<String> IgnoreInfo=new ArrayList<>();

  protected BlockPlacementPolicyDefault() {
  }
    
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap, 
                         Host2NodesMap host2datanodeMap) {
    this.considerLoad = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY, true);
    this.stats = stats;
    this.clusterMap = clusterMap;
    this.host2datanodeMap = host2datanodeMap;
    this.heartbeatInterval = conf.getLong(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT) * 1000;
    this.tolerateHeartbeatMultiplier = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY,
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT);
    this.staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    //huanke
    this.slowDataNodes = conf.getStringCollection(
        DFSConfigKeys.HACK_COMMON_WRITE_PIPELINE_DATANODE);
    //huanke-----------------------------------
//    setslowDN(slowDataNodes);
    //-------------------------------------
//    this.fixReplica=conf.getInt("dfs.replication",2);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenNodes,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    final BlockStoragePolicy storagePolicy) {
    LOG.info("@huanke Don't tell me you come out from here");
    //huanke create a empty boolean list
    List<Boolean> OutputBoolean=new ArrayList<Boolean>();
    OutputBoolean.add(false);
    OutputBoolean.add(false);
    return chooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes,
        excludedNodes, blocksize, storagePolicy, OutputBoolean);
  }


  //huanke
  public DatanodeStorageInfo[] chooseTargetHK(String srcPath,
                                            int numOfReplicas,
                                            Node writer,
                                            List<DatanodeStorageInfo> chosenNodes,
                                            boolean returnChosenNodes,
                                            Set<Node> excludedNodes,
                                            long blocksize,
                                            final BlockStoragePolicy storagePolicy,
                                              List<String> IgnoreInfo,List<Boolean> OutputBoolean) {
    LOG.info("@huanke IgnoreInfo chooseTargetHK "+IgnoreInfo+OutputBoolean);
    if(this.IgnoreInfo == null && IgnoreInfo != null && IgnoreInfo.size() > 0){
    	this.IgnoreInfo = new ArrayList<>();
    	this.IgnoreInfo.addAll(IgnoreInfo);
    }
    else if(this.IgnoreInfo != null && IgnoreInfo != null && IgnoreInfo.size() > 0){
    	this.IgnoreInfo.clear();
    	this.IgnoreInfo.addAll(IgnoreInfo);
    }
    LOG.info("@huanke IgnoreInfo classVariable "+ this.IgnoreInfo + OutputBoolean);
    LOG.info("@huanke numOfReplicas1: "+numOfReplicas+" writer: "+writer+" chosenNodes: "+chosenNodes+ " excludedNodes: "+excludedNodes );
    DatanodeStorageInfo[] targets = chooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes,
            excludedNodes, blocksize, storagePolicy, OutputBoolean);
    return targets;
  }

  //huanke
  public List<String> getslowDN(){
    return new ArrayList<>(this.slowDataNodes);
  }
//  public void setslowDN(Collection<String> slowDataNodes){
//    clusterMap.setslowDN(slowDataNodes);
//  }
  public List<String> getIgnoreInfo() {return this.IgnoreInfo;}


  //huanke just change chooseTarget into chooseTargetHK
  @Override
  DatanodeStorageInfo[] chooseTarget(String src,
      int numOfReplicas,
      Node writer,
      Set<Node> excludedNodes,
      long blocksize,
      List<DatanodeDescriptor> favoredNodes,
      BlockStoragePolicy storagePolicy) {
    try {
        LOG.info("@huanke getslowDN: "+getslowDN());
       LOG.info("@huanke favoredNodes: "+favoredNodes+" favoredNodes.size()");

      //huanke favoredNodes: [10.1.1.7:50010] favoredNodes.size()1

      if (favoredNodes == null || favoredNodes.size() == 0) {
        LOG.info("@huanke favoredNodes=null: ");
        // Favored nodes not specified, fall back to regular block placement.
        return chooseTarget(src, numOfReplicas, writer,
            new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
            excludedNodes, blocksize, storagePolicy);
      }

      Set<Node> favoriteAndExcludedNodes = excludedNodes == null ?
          new HashSet<Node>() : new HashSet<Node>(excludedNodes);
      final List<StorageType> requiredStorageTypes = storagePolicy
          .chooseStorageTypes((short)numOfReplicas);
      final EnumMap<StorageType, Integer> storageTypes =
          getRequiredStorageTypes(requiredStorageTypes);

      // Choose favored nodes
      List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>();
      boolean avoidStaleNodes = stats != null
          && stats.isAvoidingStaleDataNodesForWrite();

      int maxNodesAndReplicas[] = getMaxNodesPerRack(0, numOfReplicas);
      numOfReplicas = maxNodesAndReplicas[0];
      int maxNodesPerRack = maxNodesAndReplicas[1];

      //huanke if favoredNodes.size()1, then it execute from here, no the final line, that we can not get the output
      for (int i = 0; i < favoredNodes.size() && results.size() < numOfReplicas; i++) {
        DatanodeDescriptor favoredNode = favoredNodes.get(i);
        // Choose a single node which is local to favoredNode.
        // 'results' is updated within chooseLocalNode
        final DatanodeStorageInfo target = chooseLocalStorage(favoredNode,
            favoriteAndExcludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes, false);
        if (target == null) {
          LOG.warn("Could not find a target for file " + src
              + " with favored node " + favoredNode); 
          continue;
        }
        favoriteAndExcludedNodes.add(target.getDatanodeDescriptor());
      }

      if (results.size() < numOfReplicas) {
        // Not enough favored nodes, choose other nodes.
        numOfReplicas -= results.size();
        DatanodeStorageInfo[] remainingTargets = 
            chooseTarget(src, numOfReplicas, writer, results,
                false, favoriteAndExcludedNodes, blocksize, storagePolicy);
        for (int i = 0; i < remainingTargets.length; i++) {
          results.add(remainingTargets[i]);
        }
      }
      return getPipeline(writer,
          results.toArray(new DatanodeStorageInfo[results.size()]));
    } catch (NotEnoughReplicasException nr) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose with favored nodes (=" + favoredNodes
            + "), disregard favored nodes hint and retry.", nr);
      }
      // Fall back to regular block placement disregarding favored nodes hint
      return chooseTarget(src, numOfReplicas, writer, 
          new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
          excludedNodes, blocksize, storagePolicy);
    }
  }

  /** This is the implementation. */
  private DatanodeStorageInfo[] chooseTarget(int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenStorage,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    final BlockStoragePolicy storagePolicy, List<Boolean> OutputBoolean) {
    DFSClient.LOG.debug("@huanke----step9---BlockPlacementPolicyDefault.chooseTarget()------"+chosenStorage+" ex1: "+excludedNodes+" numRep: "+numOfReplicas + "OutputBoolean: "+OutputBoolean);
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {//I hope all the replica is equal to 2
      return DatanodeStorageInfo.EMPTY_ARRAY;
    }
    LOG.info("@huanke----------"+chosenStorage+" ex2: "+excludedNodes+" numRep: "+numOfReplicas + "OutputBoolean: "+OutputBoolean);
      
    if (excludedNodes == null) {
      excludedNodes = new HashSet<Node>();
    }
     
    int[] result = getMaxNodesPerRack(chosenStorage.size(), numOfReplicas);
    numOfReplicas = result[0];
    int maxNodesPerRack = result[1];
      
    final List<DatanodeStorageInfo> results = new ArrayList<DatanodeStorageInfo>(chosenStorage);
    LOG.debug("@huanke chosenStorage1 : "+chosenStorage);
    for (DatanodeStorageInfo storage : chosenStorage) {
      // add localMachine and related nodes to excludedNodes
      addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    }
    LOG.debug("@huanke excludedNodes1 : "+chosenStorage);

    boolean avoidStaleNodes = (stats != null
        && stats.isAvoidingStaleDataNodesForWrite());
    LOG.debug("@huanke chooseTarget implementation: "+" numOfReplicas: "+numOfReplicas+ " writer: "+writer+" chosenStorage: "+chosenStorage+" excludedNodes: "+excludedNodes); //130 times
    final Node localNode = chooseTarget(numOfReplicas, writer, excludedNodes,
        blocksize, maxNodesPerRack, results, avoidStaleNodes, storagePolicy,
        EnumSet.noneOf(StorageType.class), results.isEmpty(), OutputBoolean);
    if (!returnChosenNodes) {  
      results.removeAll(chosenStorage);
    }

    LOG.debug("@huanke writer: "+writer+" results: "+results); //284times
    //Node type: IP:port
      
    // sorting nodes to form a pipeline
    return getPipeline(
        (writer != null && writer instanceof DatanodeDescriptor) ? writer
            : localNode,
        results.toArray(new DatanodeStorageInfo[results.size()]));
  }

  /**
   * Calculate the maximum number of replicas to allocate per rack. It also
   * limits the total number of replicas to the total number of nodes in the
   * cluster. Caller should adjust the replica count to the return value.
   *
   * @param numOfChosen The number of already chosen nodes.
   * @param numOfReplicas The number of additional nodes to allocate.
   * @return integer array. Index 0: The number of nodes allowed to allocate
   *         in addition to already chosen nodes.
   *         Index 1: The maximum allowed number of nodes per rack. This
   *         is independent of the number of chosen nodes, as it is calculated
   *         using the target number of replicas.
   */
  private int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    LOG.info("@huanke getMaxNodesPerRack: numOfChosen: "+numOfChosen + " numOfReplicas: "+numOfReplicas+ " clusterSize: "+clusterSize);
    if (totalNumOfReplicas > clusterSize) {
      //huanke there are two outliers that I don't know why, later on it is possible that the numeber of replicas is 6
//      huanke getMaxNodesPerRack: numOfChosen: 0 numOfReplicas: 10 clusterSize: 6
//      uanke getMaxNodesPerRack: numOfChosen: 2 numOfReplicas: 8 clusterSize: 6

        numOfReplicas -= (totalNumOfReplicas-clusterSize);
        totalNumOfReplicas = clusterSize;
    }
    // No calculation needed when there is only one rack or picking one node.
    int numOfRacks = clusterMap.getNumOfRacks();
    if (numOfRacks == 1 || totalNumOfReplicas <= 1) {
      return new int[] {numOfReplicas, totalNumOfReplicas};
    }

    int maxNodesPerRack = (totalNumOfReplicas-1)/numOfRacks + 2;
    // At this point, there are more than one racks and more than one replicas
    // to store. Avoid all replicas being in the same rack.
    //
    // maxNodesPerRack has the following properties at this stage.
    //   1) maxNodesPerRack >= 2
    //   2) (maxNodesPerRack-1) * numOfRacks > totalNumOfReplicas
    //          when numOfRacks > 1
    //
    // Thus, the following adjustment will still result in a value that forces
    // multi-rack allocation and gives enough number of total nodes.
    if (maxNodesPerRack == totalNumOfReplicas) {
      maxNodesPerRack--;
    }
    return new int[] {numOfReplicas, maxNodesPerRack};
  }

  private EnumMap<StorageType, Integer> getRequiredStorageTypes(
      List<StorageType> types) {
    EnumMap<StorageType, Integer> map = new EnumMap<StorageType,
        Integer>(StorageType.class);
    for (StorageType type : types) {
      if (!map.containsKey(type)) {
        map.put(type, 1);
      } else {
        int num = map.get(type);
        map.put(type, num + 1);
      }
    }
    return map;
  }

  /**
   * choose <i>numOfReplicas</i> from all data nodes
   * @param numOfReplicas additional number of replicas wanted
   * @param writer the writer's machine, could be a non-DatanodeDescriptor node
   * @param excludedNodes datanodes that should not be considered as targets
   * @param blocksize size of the data to be written
   * @param maxNodesPerRack max nodes allowed per rack
   * @param results the target nodes already chosen
   * @param avoidStaleNodes avoid stale nodes in replica choosing
   * @return local node of writer (not chosen node)
   */
  private Node chooseTarget(int numOfReplicas,
                            Node writer,
                            final Set<Node> excludedNodes,
                            final long blocksize,
                            final int maxNodesPerRack,
                            final List<DatanodeStorageInfo> results,
                            final boolean avoidStaleNodes,
                            final BlockStoragePolicy storagePolicy,
                            final EnumSet<StorageType> unavailableStorages,
                            final boolean newBlock,
                            List<Boolean> OutputBoolean) {
    LOG.debug("@huanke clusterMap1: numofReplicas: "+numOfReplicas);  //130 times
    if (numOfReplicas == 0 || clusterMap.getNumOfLeaves()==0) {
      return (writer instanceof DatanodeDescriptor) ? writer : null;
    }
    final int numOfResults = results.size();
    final int totalReplicasExpected = numOfReplicas + numOfResults;
    LOG.debug("@huanke-----numOfReplicas: "+numOfReplicas+" numOfResults: "+numOfResults+" totalReplicasExpected: "+totalReplicasExpected); //13 times, then above if remove 117 times


    if ((writer == null || !(writer instanceof DatanodeDescriptor)) && !newBlock) {
      writer = results.get(0).getDatanodeDescriptor();
    }

    // Keep a copy of original excludedNodes
    final Set<Node> oldExcludedNodes = new HashSet<Node>(excludedNodes);

    // choose storage types; use fallbacks for unavailable storages
    final List<StorageType> requiredStorageTypes = storagePolicy
        .chooseStorageTypes((short) totalReplicasExpected,
            DatanodeStorageInfo.toStorageTypes(results),
            unavailableStorages, newBlock);
    final EnumMap<StorageType, Integer> storageTypes =
        getRequiredStorageTypes(requiredStorageTypes);
    if (LOG.isTraceEnabled()) {
      LOG.trace("storageTypes=" + storageTypes);
    }

    try {
      if ((numOfReplicas = requiredStorageTypes.size()) == 0) {
        throw new NotEnoughReplicasException(
            "All required storage types are unavailable: "
            + " unavailableStorages=" + unavailableStorages
            + ", storagePolicy=" + storagePolicy);
      }

      if (numOfResults == 0) {
        LOG.debug("@huanke testWriter: "+ writer);
        if(OutputBoolean.get(0)){
          writer = chooseLocalStorageHK(writer, excludedNodes, blocksize,
                  maxNodesPerRack, results, avoidStaleNodes, storageTypes, true, OutputBoolean)
                  .getDatanodeDescriptor();
        }
        else if(OutputBoolean.get(1)){
          writer = chooseLocalStorageHB(writer, excludedNodes, blocksize,
                  maxNodesPerRack, results, avoidStaleNodes, storageTypes, true, OutputBoolean)
                  .getDatanodeDescriptor();
        }
        else{
        	writer = chooseLocalStorage(writer, excludedNodes, blocksize,
        								maxNodesPerRack, results, avoidStaleNodes, storageTypes, true)
        								.getDatanodeDescriptor();
        }
        LOG.debug("@huanke numOfResults == 0--> writer: "+writer+" results: "+results+" numOfReplicas: "+numOfReplicas);
        //numOfResults == 0--> writer: 10.1.1.2:50010 results: [[DISK]DS-f74f341c-3a29-4c19-bdaa-2e02aa41b4a2:NORMAL:10.1.1.2:50010] numOfReplicas: 2
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      final DatanodeDescriptor dn0 = results.get(0).getDatanodeDescriptor();
      LOG.debug("@huanke dn0 -> "+dn0);
      if (numOfResults <= 1) {
        if(OutputBoolean.get(0)){
          chooseRemoteRackHK(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
                  results, avoidStaleNodes, storageTypes, OutputBoolean);
        }
        else if(OutputBoolean.get(1)){
          chooseRemoteRackHB(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
                  results, avoidStaleNodes, storageTypes, OutputBoolean);
        }
        else{
        	chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
        			results, avoidStaleNodes, storageTypes);
        }

        LOG.debug("@huanke numOfResults  <= 1 --> writer: "+writer+" results: "+results+" numOfReplicas: "+numOfReplicas);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }
      if (numOfResults <= 2) {
        final DatanodeDescriptor dn1 = results.get(1).getDatanodeDescriptor();
        LOG.debug("@huanke dn1 -> "+dn1);
        if (clusterMap.isOnSameRack(dn0, dn1)) {
          LOG.info("@huanke---case1----");
          //huanke----That's it!
          if(OutputBoolean.get(1)){
            chooseRemoteRackHB(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
                    results, avoidStaleNodes, storageTypes, OutputBoolean);
          }
          else{
        	  chooseRemoteRack(1, dn0, excludedNodes, blocksize, maxNodesPerRack,
        		  		   	results, avoidStaleNodes, storageTypes);
          }
        } else if (newBlock){
          LOG.debug("@huanke---case2----");
          chooseLocalRack(dn1, excludedNodes, blocksize, maxNodesPerRack,
        		  		  results, avoidStaleNodes, storageTypes);
        } else {
          LOG.debug("@huanke---case3----");
          chooseLocalRack(writer, excludedNodes, blocksize, maxNodesPerRack,
        		  		  results, avoidStaleNodes, storageTypes);
        }
        LOG.debug("@huanke numOfResults  <= 2 --> writer: "+writer+" results: "+results+" numOfReplicas: "+numOfReplicas);
        if (--numOfReplicas == 0) {
          return writer;
        }
      }

      LOG.debug("@huanke case4 and remaining number of replicas"+numOfReplicas+NodeBase.ROOT+excludedNodes+results);
      chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      final String message = "Failed to place enough replicas, still in need of "
          + (totalReplicasExpected - results.size()) + " to reach "
          + totalReplicasExpected
          + " (unavailableStorages=" + unavailableStorages
          + ", storagePolicy=" + storagePolicy
          + ", newBlock=" + newBlock + ")";

      if (LOG.isTraceEnabled()) {
        LOG.trace(message, e);
      } else {
        LOG.warn(message + " " + e.getMessage());
      }

      if (avoidStaleNodes) {
        // Retry chooseTarget again, this time not avoiding stale nodes.

        // excludedNodes contains the initial excludedNodes and nodes that were
        // not chosen because they were stale, decommissioned, etc.
        // We need to additionally exclude the nodes that were added to the 
        // result list in the successful calls to choose*() above.
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(), oldExcludedNodes);
        }
        // Set numOfReplicas, since it can get out of sync with the result list
        // if the NotEnoughReplicasException was thrown in chooseRandom().
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false, storagePolicy, unavailableStorages,
            newBlock, OutputBoolean);
      }

      boolean retry = false;
      // simply add all the remaining types into unavailableStorages and give
      // another try. No best effort is guaranteed here.
      for (StorageType type : storageTypes.keySet()) {
        if (!unavailableStorages.contains(type)) {
          unavailableStorages.add(type);
          retry = true;
        }
      }
      if (retry) {
        for (DatanodeStorageInfo resultStorage : results) {
          addToExcludedNodes(resultStorage.getDatanodeDescriptor(),
              oldExcludedNodes);
        }
        numOfReplicas = totalReplicasExpected - results.size();
        return chooseTarget(numOfReplicas, writer, oldExcludedNodes, blocksize,
            maxNodesPerRack, results, false, storagePolicy, unavailableStorages,
            newBlock, OutputBoolean);
      }
    }
    LOG.info("@huanke IgnoreInfo from DFSOutputStream "+IgnoreInfo);
    return writer;
  }
    
  /**
   * Choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available,
   * choose a node on the same rack
   * @return the chosen storage
   */
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes, boolean fallbackToLocalRack)
      throws NotEnoughReplicasException {
    // if no local machine, randomly choose one node
    if (localMachine == null) {
      LOG.info("@huanke test1");
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
      //default ---------------------choose local storage----------------------
    if (preferLocalNode && localMachine instanceof DatanodeDescriptor) {
      DatanodeDescriptor localDatanode = (DatanodeDescriptor) localMachine;
      // otherwise try local machine first
      if (excludedNodes.add(localMachine)) { // was not in the excluded list
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
            .entrySet().iterator(); iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          for (DatanodeStorageInfo localStorage : DFSUtil.shuffle(
              localDatanode.getStorageInfos())) {
            StorageType type = entry.getKey();
            if (addIfIsGoodTarget(localStorage, excludedNodes, blocksize,
                maxNodesPerRack, false, results, avoidStaleNodes, type) >= 0) {
              int num = entry.getValue();
              if (num == 1) {
                iter.remove();
              } else {
                entry.setValue(num - 1);
              }
              return localStorage;
            }
          }
        }
      }
    }

    if (!fallbackToLocalRack) {
      return null;
    }
    // try a node on local rack

    DatanodeStorageInfo LocalRackNode=chooseLocalRack(localMachine, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes);

    LOG.info("@huanke test4" +LocalRackNode.getDatanodeDescriptor().getIpAddr());
    return LocalRackNode;
  }




//huanke backup task don't choose ignore info
  protected DatanodeStorageInfo chooseLocalStorageHB(Node localMachine,
                                                     Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
                                                     List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
                                                     EnumMap<StorageType, Integer> storageTypes, boolean fallbackToLocalRack, List<Boolean> OutputBoolean)
          throws NotEnoughReplicasException {

    if (!fallbackToLocalRack) {
      return null;
    }
    // try a node on local rack
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    final String localRack = localMachine.getNetworkLocation();
    DatanodeStorageInfo LocalRackNode=chooseRandomHB(1, localRack, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes, OutputBoolean);

    LOG.info("@huanke tchooseLocalStorageHB : " +LocalRackNode.getDatanodeDescriptor().getIpAddr());
//    huanke tchooseLocalStorageHB : 10.1.1.4
    return LocalRackNode;
  }

  protected DatanodeStorageInfo chooseRandomHB(int numOfReplicas,
                                               String scope,
                                               Set<Node> excludedNodes,
                                               long blocksize,
                                               int maxNodesPerRack,
                                               List<DatanodeStorageInfo> results,
                                               boolean avoidStaleNodes,
                                               EnumMap<StorageType, Integer> storageTypes, List<Boolean> OutputBoolean)
          throws NotEnoughReplicasException {
    int numOfAvailableNodes = clusterMap.countNumOfAvailableNodes(
            scope, excludedNodes);
    LOG.info("@huanke chooseRandom()--> scope: "+scope+" excludedNodes: "+excludedNodes+" results:"+results+" numOfAvailableNodes:"+numOfAvailableNodes+ " BackupOutput :" + OutputBoolean.get(1));
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = debugLoggingBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    while(numOfReplicas > 0 && numOfAvailableNodes > 0) {
      LOG.info("------------------------------------");
      LOG.info("@huanke numOfReplicas: " + numOfReplicas + " || numOfAvailableNodes: " + numOfAvailableNodes + " || excludedNodesSize: " + excludedNodes.size());
      LOG.info("@huanke excluded nodes list: " + excludedNodes);
//      huanke excluded nodes list: [10.1.1.3:50010, 10.1.1.4:50010, 10.1.1.6:50010, 10.1.1.2:50010]
//      huanke chosenNodeHK: 10.1.1.5

      boolean flag=false;

      DatanodeDescriptor chosenNode =
              (DatanodeDescriptor)clusterMap.chooseRandom(scope);
      LOG.info("@huanke chosenNodeHK: " + chosenNode.getIpAddr());
      for(String slow: IgnoreInfo){
        LOG.info("@huanke continue slow1: "+slow);
        LOG.info("@huanke continue getHostname: "+chosenNode.getHostName());
        if(chosenNode.getHostName().contains(slow)){
          LOG.info("@huanke chooseLocalStorageHK: "+chosenNode.getHostName()+chosenNode.getIpAddr());
          flag=true;
          continue;
        }
      }

      if(flag){
        continue;
      }
      LOG.info("@huanke:  final chooseRandomHB chosenNode: " + chosenNode.getIpAddr());
      if (excludedNodes.add(chosenNode)) { //was not in the excluded list
        LOG.info("@huanke: inside excludedNodes if statement. excluded nodes size: " + excludedNodes.size());
        LOG.info("@huanke: inside excludedNodes if statement. excluded list " + excludedNodes);
//      if (true) { //was not in the excluded list
        if (LOG.isDebugEnabled()) {
          builder.append("\nNode ").append(NodeBase.getPath(chosenNode)).append(" [");
        }
        numOfAvailableNodes--;

        final DatanodeStorageInfo[] storages = DFSUtil.shuffle(
                chosenNode.getStorageInfos());
        int i = 0;
        boolean search = true;
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
                .entrySet().iterator(); search && iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          for (i = 0; i < storages.length; i++) {
            StorageType type = entry.getKey();
            final int newExcludedNodes = addIfIsGoodTarget(storages[i],
                    excludedNodes, blocksize, maxNodesPerRack, considerLoad, results,
                    avoidStaleNodes, type);
            if (newExcludedNodes >= 0) {
              numOfReplicas--;
              if (firstChosen == null) {
                firstChosen = storages[i];
              }
              numOfAvailableNodes -= newExcludedNodes;
              int num = entry.getValue();
              if (num == 1) {
                iter.remove();
              } else {
                entry.setValue(num - 1);
              }
              search = false;
              break;
            }
          }
        }
        if (LOG.isDebugEnabled()) {
          builder.append("\n]");
        }

        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (i == storages.length);
      }
      LOG.info("@huanke: outside excludedNodes if statement. excluded nodes size: " + excludedNodes.size());
      LOG.info("@huanke: outside excludedNodes if statement. excluded list " + excludedNodes);
    }

    if (numOfReplicas>0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled()) {
        if (badTarget && builder != null) {
          detail = builder.toString();
          builder.setLength(0);
        } else {
          detail = "";
        }
      }
      throw new NotEnoughReplicasException(detail);
    }
    LOG.info("@huanke: firstChosen:"+firstChosen.getDatanodeDescriptor());
    return firstChosen;

  }

  //huanke if OriginalOuput=true, then localStorage is focus on choosing D5
  /**
   * Choose <i>localMachine</i> as the target.
   * if <i>localMachine</i> is not available,
   * choose a node on the same rack
   * @return the chosen storage
   */
  protected DatanodeStorageInfo chooseLocalStorageHK(Node localMachine,
                                                   Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
                                                   List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
                                                   EnumMap<StorageType, Integer> storageTypes, boolean fallbackToLocalRack, List<Boolean> OutputBoolean)
          throws NotEnoughReplicasException {

    if (!fallbackToLocalRack) {
      return null;
    }
    // try a node on local rack
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    final String localRack = localMachine.getNetworkLocation();

    DatanodeStorageInfo LocalRackNode=chooseRandomHK(1, localRack, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, storageTypes, OutputBoolean);

    LOG.info("@huanke tchooseLocalStorageHK : " +LocalRackNode.getDatanodeDescriptor().getIpAddr());
//    huanke tchooseLocalStorageHK : 10.1.1.7
//    huanke tchooseLocalStorageHK : 10.1.1.7

    return LocalRackNode;
  }

  protected DatanodeStorageInfo chooseRandomHK(int numOfReplicas,
                                             String scope,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeStorageInfo> results,
                                             boolean avoidStaleNodes,
                                             EnumMap<StorageType, Integer> storageTypes, List<Boolean> OutputBoolean)
          throws NotEnoughReplicasException {
    int numOfAvailableNodes = clusterMap.countNumOfAvailableNodes(
            scope, excludedNodes);
    LOG.info("@huanke chooseRandom()--> scope: "+scope+" excludedNodes: "+excludedNodes+" results:"+results+" numOfAvailableNodes:"+numOfAvailableNodes);
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = debugLoggingBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    while(numOfReplicas > 0 && numOfAvailableNodes > 0) {
      LOG.info("------------------------------------");
      LOG.info("@huanke numOfReplicas: " + numOfReplicas + " || numOfAvailableNodes: " + numOfAvailableNodes + " || excludedNodesSize: " + excludedNodes.size());
      LOG.info("@huanke excluded nodes list: " + excludedNodes);
//      huanke excluded nodes list: [10.1.1.3:50010, 10.1.1.4:50010, 10.1.1.6:50010, 10.1.1.2:50010]
//      huanke chosenNodeHK: 10.1.1.5

      // boolean flag=false;

      DatanodeDescriptor chosenNode =
              (DatanodeDescriptor)clusterMap.chooseRandom(scope);
      // @Cesar: Commenting this out, original task can pick any node
      
      /*LOG.info("@huanke chosenNodeHK: " + chosenNode.getIpAddr());
        for(String slow: slowDataNodes){
          LOG.info("@huanke continue slow1: "+slow);
          LOG.info("@huanke continue getHostname: "+chosenNode.getHostName());
          if(!chosenNode.getHostName().contains(slow)){
            LOG.info("@huanke chooseLocalStorageHK: "+chosenNode.getHostName()+chosenNode.getIpAddr());
            flag=true;
            continue;
          }
      }*/

      /*if(flag){
        continue;
      }*/
      LOG.info("@huanke:  final chosenNode: " + chosenNode.getIpAddr());
      if (excludedNodes.add(chosenNode)) { //was not in the excluded list
        LOG.info("@huanke: inside excludedNodes if statement. excluded nodes size: " + excludedNodes.size());
        LOG.info("@huanke: inside excludedNodes if statement. excluded list " + excludedNodes);
//      if (true) { //was not in the excluded list
        if (LOG.isDebugEnabled()) {
          builder.append("\nNode ").append(NodeBase.getPath(chosenNode)).append(" [");
        }
        numOfAvailableNodes--;

        final DatanodeStorageInfo[] storages = DFSUtil.shuffle(
                chosenNode.getStorageInfos());
        int i = 0;
        boolean search = true;
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
                .entrySet().iterator(); search && iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          for (i = 0; i < storages.length; i++) {
            StorageType type = entry.getKey();
            final int newExcludedNodes = addIfIsGoodTarget(storages[i],
                    excludedNodes, blocksize, maxNodesPerRack, considerLoad, results,
                    avoidStaleNodes, type);
            if (newExcludedNodes >= 0) {
              numOfReplicas--;
              if (firstChosen == null) {
                firstChosen = storages[i];
              }
              numOfAvailableNodes -= newExcludedNodes;
              int num = entry.getValue();
              if (num == 1) {
                iter.remove();
              } else {
                entry.setValue(num - 1);
              }
              search = false;
              break;
            }
          }
        }
        if (LOG.isDebugEnabled()) {
          builder.append("\n]");
        }

        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (i == storages.length);
      }
      LOG.info("@huanke: outside excludedNodes if statement. excluded nodes size: " + excludedNodes.size());
      LOG.info("@huanke: outside excludedNodes if statement. excluded list " + excludedNodes);
    }

    if (numOfReplicas>0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled()) {
        if (badTarget && builder != null) {
          detail = builder.toString();
          builder.setLength(0);
        } else {
          detail = "";
        }
      }
      throw new NotEnoughReplicasException(detail);
    }
    LOG.info("@huanke: firstChosen:"+firstChosen.getDatanodeDescriptor());
    return firstChosen;

  }


//huanke not choose D5

  protected DatanodeStorageInfo chooseRandomHK1(int numOfReplicas,
                                               String scope,
                                               Set<Node> excludedNodes,
                                               long blocksize,
                                               int maxNodesPerRack,
                                               List<DatanodeStorageInfo> results,
                                               boolean avoidStaleNodes,
                                               EnumMap<StorageType, Integer> storageTypes, List<Boolean> OutputBoolean)
          throws NotEnoughReplicasException {
    int numOfAvailableNodes = clusterMap.countNumOfAvailableNodes(
            scope, excludedNodes);
    LOG.info("@huanke chooseRandom()--> scope: "+scope+" excludedNodes: "+excludedNodes+" results:"+results+" numOfAvailableNodes:"+numOfAvailableNodes);
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = debugLoggingBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    while(numOfReplicas > 0 && numOfAvailableNodes > 0) {
      LOG.info("------------------------------------");
      LOG.info("@huanke numOfReplicas: " + numOfReplicas + " || numOfAvailableNodes: " + numOfAvailableNodes + " || excludedNodesSize: " + excludedNodes.size());
      LOG.info("@huanke excluded nodes list: " + excludedNodes);
//      huanke excluded nodes list: [10.1.1.3:50010, 10.1.1.4:50010, 10.1.1.6:50010, 10.1.1.2:50010]
//      huanke chosenNodeHK: 10.1.1.5

      boolean flag=false;

      DatanodeDescriptor chosenNode =
              (DatanodeDescriptor)clusterMap.chooseRandom(scope);
      LOG.info("@huanke chosenNodeHK: " + chosenNode.getIpAddr());
      for(String slow: slowDataNodes){
        LOG.info("@huanke continue slow1: "+slow);
        LOG.info("@huanke continue getHostname: "+chosenNode.getHostName());
        if(chosenNode.getHostName().contains(slow)){
          LOG.info("@huanke chooseLocalStorageHK: "+chosenNode.getHostName()+chosenNode.getIpAddr());
          flag=true;
          continue;
        }
      }

      if(flag){
        continue;
      }
      LOG.info("@huanke:  final chooseRandomHK1 chosenNode: " + chosenNode.getIpAddr());
      if (excludedNodes.add(chosenNode)) { //was not in the excluded list
        LOG.info("@huanke: inside excludedNodes if statement. excluded nodes size: " + excludedNodes.size());
        LOG.info("@huanke: inside excludedNodes if statement. excluded list " + excludedNodes);
//      if (true) { //was not in the excluded list
        if (LOG.isDebugEnabled()) {
          builder.append("\nNode ").append(NodeBase.getPath(chosenNode)).append(" [");
        }
        numOfAvailableNodes--;

        final DatanodeStorageInfo[] storages = DFSUtil.shuffle(
                chosenNode.getStorageInfos());
        int i = 0;
        boolean search = true;
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
                .entrySet().iterator(); search && iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          for (i = 0; i < storages.length; i++) {
            StorageType type = entry.getKey();
            final int newExcludedNodes = addIfIsGoodTarget(storages[i],
                    excludedNodes, blocksize, maxNodesPerRack, considerLoad, results,
                    avoidStaleNodes, type);
            if (newExcludedNodes >= 0) {
              numOfReplicas--;
              if (firstChosen == null) {
                firstChosen = storages[i];
              }
              numOfAvailableNodes -= newExcludedNodes;
              int num = entry.getValue();
              if (num == 1) {
                iter.remove();
              } else {
                entry.setValue(num - 1);
              }
              search = false;
              break;
            }
          }
        }
        if (LOG.isDebugEnabled()) {
          builder.append("\n]");
        }

        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (i == storages.length);
      }
      LOG.info("@huanke: outside excludedNodes if statement. excluded nodes size: " + excludedNodes.size());
      LOG.info("@huanke: outside excludedNodes if statement. excluded list " + excludedNodes);
    }

    if (numOfReplicas>0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled()) {
        if (badTarget && builder != null) {
          detail = builder.toString();
          builder.setLength(0);
        } else {
          detail = "";
        }
      }
      throw new NotEnoughReplicasException(detail);
    }
    LOG.info("@huanke: firstChosen:"+firstChosen.getDatanodeDescriptor());
    return firstChosen;

  }


  /**
   * Add <i>localMachine</i> and related nodes to <i>excludedNodes</i>
   * for next replica choosing. In sub class, we can add more nodes within
   * the same failure domain of localMachine
   * @return number of new excluded nodes
   */
  protected int addToExcludedNodes(DatanodeDescriptor localMachine,
      Set<Node> excludedNodes) {
    return excludedNodes.add(localMachine) ? 1 : 0;
  }

  /**
   * Choose one node from the rack that <i>localMachine</i> is on.
   * if no such node is available, choose one node from the rack where
   * a second replica is on.
   * if still no such node is available, choose a random node 
   * in the cluster.
   * @return the chosen node
   */
  protected DatanodeStorageInfo chooseLocalRack(Node localMachine,
                                                Set<Node> excludedNodes,
                                                long blocksize,
                                                int maxNodesPerRack,
                                                List<DatanodeStorageInfo> results,
                                                boolean avoidStaleNodes,
                                                EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    // no local machine, so choose a random machine
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
    final String localRack = localMachine.getNetworkLocation();
    LOG.info("@huanke chooseLocalRack() --> "+" localMachine: "+localMachine+"excludedNodes: " +excludedNodes+" results: "+results+ "localRack: "+localRack);
    //huanke chooseLocalRack() -->  localMachine: 10.1.1.2:50010 excludedNodes: [] results: []localRack: /default-rack


    try {
      // choose one from the local rack
      return chooseRandom(localRack, excludedNodes,
          blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // find the next replica and retry with its rack
      for(DatanodeStorageInfo resultStorage : results) {
        DatanodeDescriptor nextNode = resultStorage.getDatanodeDescriptor();
        if (nextNode != localMachine) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Failed to choose from local rack (location = " + localRack
                + "), retry with the rack of the next replica (location = "
                + nextNode.getNetworkLocation() + ")", e);
          }
          return chooseFromNextRack(nextNode, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose from local rack (location = " + localRack
            + "); the second replica is not found, retry choosing ramdomly", e);
      }
      //the second replica is not found, randomly choose one from the network
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  private DatanodeStorageInfo chooseFromNextRack(Node next,
      Set<Node> excludedNodes,
      long blocksize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes) throws NotEnoughReplicasException {
    final String nextRack = next.getNetworkLocation();
    try {
      return chooseRandom(nextRack, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
    } catch(NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose from the next rack (location = " + nextRack
            + "), retry choosing ramdomly", e);
      }
      //otherwise randomly choose one from the network
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /** 
   * Choose <i>numOfReplicas</i> nodes from the racks 
   * that <i>localMachine</i> is NOT on.
   * if not enough nodes are available, choose the remaining ones 
   * from the local rack
   */
    
  protected void chooseRemoteRack(int numOfReplicas,
                                  DatanodeDescriptor localMachine,
                                  Set<Node> excludedNodes,
                                  long blocksize,
                                  int maxReplicasPerRack,
                                  List<DatanodeStorageInfo> results,
                                  boolean avoidStaleNodes,
                                  EnumMap<StorageType, Integer> storageTypes)
                                    throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandom(numOfReplicas, "~" + localMachine.getNetworkLocation(),
          excludedNodes, blocksize, maxReplicasPerRack, results,
          avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose remote rack (location = ~"
            + localMachine.getNetworkLocation() + "), fallback to local rack", e);
      }
      chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
                   localMachine.getNetworkLocation(), excludedNodes, blocksize, 
                   maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  //huanke OutputBoolean.get(0)=true, not choose D5
  protected void chooseRemoteRackHK(int numOfReplicas,
                                  DatanodeDescriptor localMachine,
                                  Set<Node> excludedNodes,
                                  long blocksize,
                                  int maxReplicasPerRack,
                                  List<DatanodeStorageInfo> results,
                                  boolean avoidStaleNodes,
                                  EnumMap<StorageType, Integer> storageTypes, List<Boolean> OutputBoolean)
          throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandomHK1(numOfReplicas, "~" + localMachine.getNetworkLocation(),
              excludedNodes, blocksize, maxReplicasPerRack, results,
              avoidStaleNodes, storageTypes, OutputBoolean);
    } catch (NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose remote rack (location = ~"
                + localMachine.getNetworkLocation() + "), fallback to local rack", e);
      }
      chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
              localMachine.getNetworkLocation(), excludedNodes, blocksize,
              maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }


  //huanke OutputBoolean.get(0)=true, not choose D5
  protected void chooseRemoteRackHB(int numOfReplicas,
                                    DatanodeDescriptor localMachine,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    int maxReplicasPerRack,
                                    List<DatanodeStorageInfo> results,
                                    boolean avoidStaleNodes,
                                    EnumMap<StorageType, Integer> storageTypes, List<Boolean> OutputBoolean)
          throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();
    // randomly choose one node from remote racks
    try {
      chooseRandomHB(numOfReplicas, "~" + localMachine.getNetworkLocation(),
              excludedNodes, blocksize, maxReplicasPerRack, results,
              avoidStaleNodes, storageTypes, OutputBoolean);
    } catch (NotEnoughReplicasException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to choose remote rack (location = ~"
                + localMachine.getNetworkLocation() + "), fallback to local rack", e);
      }
      chooseRandom(numOfReplicas-(results.size()-oldNumOfReplicas),
              localMachine.getNetworkLocation(), excludedNodes, blocksize,
              maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /**
   * Randomly choose one target from the given <i>scope</i>.
   * @return the chosen storage, if there is any.
   */
  protected DatanodeStorageInfo chooseRandom(String scope,
      Set<Node> excludedNodes,
      long blocksize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
          throws NotEnoughReplicasException {
    return chooseRandom(1, scope, excludedNodes, blocksize, maxNodesPerRack,
        results, avoidStaleNodes, storageTypes);
  }

  /**
   //   * Randomly choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
   //   * @return the first chosen node, if there is any.
   //   */
  protected DatanodeStorageInfo chooseRandom(int numOfReplicas,
                                             String scope,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             int maxNodesPerRack,
                                             List<DatanodeStorageInfo> results,
                                             boolean avoidStaleNodes,
                                             EnumMap<StorageType, Integer> storageTypes)
          throws NotEnoughReplicasException {
    int numOfAvailableNodes = clusterMap.countNumOfAvailableNodes(
            scope, excludedNodes);
    LOG.info("@huanke chooseRandom()--> scope: "+scope+" excludedNodes: "+excludedNodes+" results:"+results+" numOfAvailableNodes:"+numOfAvailableNodes);
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = debugLoggingBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    while(numOfReplicas > 0 && numOfAvailableNodes > 0) {
      LOG.info("------------------------------------");
      LOG.info("@huanke numOfReplicas: " + numOfReplicas + " || numOfAvailableNodes: " + numOfAvailableNodes + " || excludedNodesSize: " + excludedNodes.size());
      LOG.info("@huanke excluded nodes list: " + excludedNodes);

      DatanodeDescriptor chosenNode = (DatanodeDescriptor)clusterMap.chooseRandom(scope);
      
      // @ scope: default-rack--> index: a random value in (numOfDataNodes) --> according to /etc/hosts order to choose one DN
      LOG.info("@huanke:  final chosenNode: " + chosenNode.getIpAddr());
      if (excludedNodes.add(chosenNode)) { //was not in the excluded list
        LOG.info("@huanke: inside excludedNodes if statement. excluded nodes size: " + excludedNodes.size());
        LOG.info("@huanke: inside excludedNodes if statement. excluded list " + excludedNodes);
        if (LOG.isDebugEnabled()) {
          builder.append("\nNode ").append(NodeBase.getPath(chosenNode)).append(" [");
        }
        numOfAvailableNodes--;

        final DatanodeStorageInfo[] storages = DFSUtil.shuffle(
                chosenNode.getStorageInfos());
        int i = 0;
        boolean search = true;
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes
                .entrySet().iterator(); search && iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          for (i = 0; i < storages.length; i++) {
            StorageType type = entry.getKey();
            final int newExcludedNodes = addIfIsGoodTarget(storages[i],
                    excludedNodes, blocksize, maxNodesPerRack, considerLoad, results,
                    avoidStaleNodes, type);
            if (newExcludedNodes >= 0) {
              numOfReplicas--;
              if (firstChosen == null) {
                firstChosen = storages[i];
              }
              numOfAvailableNodes -= newExcludedNodes;
              int num = entry.getValue();
              if (num == 1) {
                iter.remove();
              } else {
                entry.setValue(num - 1);
              }
              search = false;
              break;
            }
          }
        }
        if (LOG.isDebugEnabled()) {
          builder.append("\n]");
        }

        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (i == storages.length);
      }
      LOG.info("@huanke: outside excludedNodes if statement. excluded nodes size: " + excludedNodes.size());
      LOG.info("@huanke: outside excludedNodes if statement. excluded list " + excludedNodes);
    }

    if (numOfReplicas>0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled()) {
        if (badTarget && builder != null) {
          detail = builder.toString();
          builder.setLength(0);
        } else {
          detail = "";
        }
      }
      throw new NotEnoughReplicasException(detail);
    }
    LOG.info("@huanke: firstChosen:"+firstChosen.getDatanodeDescriptor());
    return firstChosen;

  }

  /**
   * If the given storage is a good target, add it to the result list and
   * update the set of excluded nodes.
   * @return -1 if the given is not a good target;
   *         otherwise, return the number of nodes added to excludedNodes set.
   */
  int addIfIsGoodTarget(DatanodeStorageInfo storage,
      Set<Node> excludedNodes,
      long blockSize,
      int maxNodesPerRack,
      boolean considerLoad,
      List<DatanodeStorageInfo> results,                           
      boolean avoidStaleNodes,
      StorageType storageType) {
    if (isGoodTarget(storage, blockSize, maxNodesPerRack, considerLoad,
        results, avoidStaleNodes, storageType)) {
      results.add(storage);
      // add node and related nodes to excludedNode
      return addToExcludedNodes(storage.getDatanodeDescriptor(), excludedNodes);
    } else { 
      return -1;
    }
  }

  private static void logNodeIsNotChosen(DatanodeStorageInfo storage, String reason) {
    if (LOG.isDebugEnabled()) {
      // build the error message for later use.
      debugLoggingBuilder.get()
          .append("\n  Storage ").append(storage)
          .append(" is not chosen since ").append(reason).append(".");
    }
  }

  /**
   * Determine if a storage is a good target. 
   * 
   * @param storage The target storage
   * @param blockSize Size of block
   * @param maxTargetPerRack Maximum number of targets per rack. The value of 
   *                       this parameter depends on the number of racks in 
   *                       the cluster and total number of replicas for a block
   * @param considerLoad whether or not to consider load of the target node
   * @param results A list containing currently chosen nodes. Used to check if 
   *                too many nodes has been chosen in the target rack.
   * @param avoidStaleNodes Whether or not to avoid choosing stale nodes
   * @return Return true if <i>node</i> has enough space, 
   *         does not have too much load, 
   *         and the rack does not have too many nodes.
   */
  private boolean isGoodTarget(DatanodeStorageInfo storage,
                               long blockSize, int maxTargetPerRack,
                               boolean considerLoad,
                               List<DatanodeStorageInfo> results,
                               boolean avoidStaleNodes,
                               StorageType requiredStorageType) {
    if (storage.getStorageType() != requiredStorageType) {
      logNodeIsNotChosen(storage, "storage types do not match,"
          + " where the required storage type is " + requiredStorageType);
      return false;
    }
    if (storage.getState() == State.READ_ONLY_SHARED) {
      logNodeIsNotChosen(storage, "storage is read-only");
      return false;
    }

    if (storage.getState() == State.FAILED) {
      logNodeIsNotChosen(storage, "storage has failed");
      return false;
    }

    DatanodeDescriptor node = storage.getDatanodeDescriptor();
    // check if the node is (being) decommissioned
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      logNodeIsNotChosen(storage, "the node is (being) decommissioned ");
      return false;
    }

    if (avoidStaleNodes) {
      if (node.isStale(this.staleInterval)) {
        logNodeIsNotChosen(storage, "the node is stale ");
        return false;
      }
    }
    
    final long requiredSize = blockSize * HdfsConstants.MIN_BLOCKS_FOR_WRITE;
    final long scheduledSize = blockSize * node.getBlocksScheduled(storage.getStorageType());
    final long remaining = node.getRemaining(storage.getStorageType());
    if (requiredSize > remaining - scheduledSize) {
      logNodeIsNotChosen(storage, "the node does not have enough "
          + storage.getStorageType() + " space"
          + " (required=" + requiredSize
          + ", scheduled=" + scheduledSize
          + ", remaining=" + remaining + ")");
      return false;
    }

    // check the communication traffic of the target machine
    if (considerLoad) {
      final double maxLoad = 2.0 * stats.getInServiceXceiverAverage();
      final int nodeLoad = node.getXceiverCount();
      if (nodeLoad > maxLoad) {
        logNodeIsNotChosen(storage, "the node is too busy (load: " + nodeLoad
            + " > " + maxLoad + ") ");
        return false;
      }
    }
      
    // check if the target rack has chosen too many nodes
    String rackname = node.getNetworkLocation();
    int counter=1;
    for(DatanodeStorageInfo resultStorage : results) {
      if (rackname.equals(
          resultStorage.getDatanodeDescriptor().getNetworkLocation())) {
        counter++;
      }
    }
    if (counter>maxTargetPerRack) {
      logNodeIsNotChosen(storage, "the rack has too many chosen nodes ");
      return false;
    }
    return true;
  }

  /**
   * Return a pipeline of nodes.
   * The pipeline is formed finding a shortest path that 
   * starts from the writer and traverses all <i>nodes</i>
   * This is basically a traveling salesman problem.
   */
  private DatanodeStorageInfo[] getPipeline(Node writer,
      DatanodeStorageInfo[] storages) {
    if (storages.length == 0) {
      return storages;
    }

    synchronized(clusterMap) {
      int index=0;
      if (writer == null || !clusterMap.contains(writer)) {
        writer = storages[0].getDatanodeDescriptor();
      }
      for(; index < storages.length; index++) {
        DatanodeStorageInfo shortestStorage = storages[index];
        int shortestDistance = clusterMap.getDistance(writer,
            shortestStorage.getDatanodeDescriptor());
        int shortestIndex = index;
        for(int i = index + 1; i < storages.length; i++) {
          int currentDistance = clusterMap.getDistance(writer,
              storages[i].getDatanodeDescriptor());
          if (shortestDistance>currentDistance) {
            shortestDistance = currentDistance;
            shortestStorage = storages[i];
            shortestIndex = i;
          }
        }
        //switch position index & shortestIndex
        if (index != shortestIndex) {
          storages[shortestIndex] = storages[index];
          storages[index] = shortestStorage;
        }
        writer = shortestStorage.getDatanodeDescriptor();
      }
    }
    return storages;
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(String srcPath,
      LocatedBlock lBlk, int numberOfReplicas) {
    DatanodeInfo[] locs = lBlk.getLocations();
    if (locs == null)
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    int numRacks = clusterMap.getNumOfRacks();
    if(numRacks <= 1) // only one rack
      return new BlockPlacementStatusDefault(
          Math.min(numRacks, numberOfReplicas), numRacks);
    int minRacks = Math.min(2, numberOfReplicas);
    // 1. Check that all locations are different.
    // 2. Count locations on different racks.
    Set<String> racks = new TreeSet<String>();
    for (DatanodeInfo dn : locs)
      racks.add(dn.getNetworkLocation());
    return new BlockPlacementStatusDefault(racks.size(), minRacks);
  }

  @Override
  public DatanodeStorageInfo chooseReplicaToDelete(BlockCollection bc,
      Block block, short replicationFactor,
      Collection<DatanodeStorageInfo> first,
      Collection<DatanodeStorageInfo> second,
      final List<StorageType> excessTypes) {
    long oldestHeartbeat =
      monotonicNow() - heartbeatInterval * tolerateHeartbeatMultiplier;
    DatanodeStorageInfo oldestHeartbeatStorage = null;
    long minSpace = Long.MAX_VALUE;
    DatanodeStorageInfo minSpaceStorage = null;

    // Pick the node with the oldest heartbeat or with the least free space,
    // if all hearbeats are within the tolerable heartbeat interval
    for(DatanodeStorageInfo storage : pickupReplicaSet(first, second)) {
      if (!excessTypes.contains(storage.getStorageType())) {
        continue;
      }

      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      long free = node.getRemaining();
      long lastHeartbeat = node.getLastUpdateMonotonic();
      if (lastHeartbeat < oldestHeartbeat) {
        oldestHeartbeat = lastHeartbeat;
        oldestHeartbeatStorage = storage;
      }
      if (minSpace > free) {
        minSpace = free;
        minSpaceStorage = storage;
      }
    }

    final DatanodeStorageInfo storage;
    if (oldestHeartbeatStorage != null) {
      storage = oldestHeartbeatStorage;
    } else if (minSpaceStorage != null) {
      storage = minSpaceStorage;
    } else {
      return null;
    }
    excessTypes.remove(storage.getStorageType());
    return storage;
  }

  /**
   * Pick up replica node set for deleting replica as over-replicated. 
   * First set contains replica nodes on rack with more than one
   * replica while second set contains remaining replica nodes.
   * So pick up first set if not empty. If first is empty, then pick second.
   */
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> first,
      Collection<DatanodeStorageInfo> second) {
    return first.isEmpty() ? second : first;
  }
  
  @VisibleForTesting
  void setPreferLocalNode(boolean prefer) {
    this.preferLocalNode = prefer;
  }
}

