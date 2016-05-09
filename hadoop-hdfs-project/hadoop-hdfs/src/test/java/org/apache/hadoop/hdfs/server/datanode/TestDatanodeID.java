package org.apache.hadoop.hdfs.server.datanode;

import static org.junit.Assert.*;

import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.junit.Assert;
import org.junit.Test;

public class TestDatanodeID {

  @Test
  public void test() {
    DatanodeID lastDatanodeID = new DatanodeID("0.0.0.0","localhost","null-uuid",0,0,0,0);
    System.out.println(lastDatanodeID.toString());
    System.out.println(lastDatanodeID.getDatanodeUuid());
    Assert.assertNotEquals("UUID is null", lastDatanodeID.getDatanodeUuid(), null);
  }

}
