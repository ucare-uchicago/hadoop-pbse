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
package org.apache.hadoop.hdfs.client;

import java.io.InputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import com.google.common.base.Preconditions;

/**
 * The Hdfs implementation of {@link FSDataInputStream}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HdfsDataInputStream extends FSDataInputStream {
  public HdfsDataInputStream(DFSInputStream in) throws IOException {
    super(in);
  }

  public HdfsDataInputStream(CryptoInputStream in) throws IOException {
    super(in);
    Preconditions.checkArgument(in.getWrappedStream() instanceof DFSInputStream,
        "CryptoInputStream should wrap a DFSInputStream");
  }

  private DFSInputStream getDFSInputStream() {
    if (in instanceof CryptoInputStream) {
      return (DFSInputStream) ((CryptoInputStream) in).getWrappedStream();
    }
    return (DFSInputStream) in;
  }

  /**
   * Get a reference to the wrapped output stream. We always want to return the
   * actual underlying InputStream, even when we're using a CryptoStream. e.g.
   * in the delegated methods below.
   *
   * @return the underlying output stream
   */
  public InputStream getWrappedStream() {
      return in;
  }

  /**
   * Get the datanode from which the stream is currently reading.
   */
  public DatanodeInfo getCurrentDatanode() {
    return getDFSInputStream().getCurrentDatanode();
  }

  /**
   * Get the block containing the target position.
   */
  public ExtendedBlock getCurrentBlock() {
    return getDFSInputStream().getCurrentBlock();
  }

  /**
   * Get the collection of blocks that has already been located.
   */
  public List<LocatedBlock> getAllBlocks() throws IOException {
    return getDFSInputStream().getAllBlocks();
  }

  /**
   * Get the visible length of the file. It will include the length of the last
   * block even if that is in UnderConstruction state.
   *
   * @return The visible length of the file.
   */
  public long getVisibleLength() throws IOException {
    return getDFSInputStream().getFileLength();
  }

  /**
   * Get statistics about the reads which this DFSInputStream has done.
   * Note that because HdfsDataInputStream is buffered, these stats may
   * be higher than you would expect just by adding up the number of
   * bytes read through HdfsDataInputStream.
   */
  public DFSInputStream.ReadStatistics getReadStatistics() {
    return getDFSInputStream().getReadStatistics();
  }

  public void clearReadStatistics() {
    getDFSInputStream().clearReadStatistics();
  }

  private DatanodeID ignoredDatanode;

  /**
   * Get ignored datanode by UCARE_SE0 algorithm. If no datanode is ignored before,
   * it will return null. If it is original task, it will return
   * {@link DatanodeID#nullDatanodeID}
   *
   * @return null if {@link HdfsDataInputStream#ignoredDatanode} never invoked,
   *         {@link DatanodeID#nullDatanodeID} if it is original task, or
   *         {@link DatanodeID} from lastest previous attempt.
   */
  public DatanodeID getIgnoredDatanode() {
    return ignoredDatanode;
  }

  /**
   * Ignore datanode by DatanodeID
   * @param lastDatanodeID
   */
  private void ignoreDatanode(DatanodeID lastDatanodeID) {
    this.ignoredDatanode = lastDatanodeID;
    getDFSInputStream().ignodeDatanode(lastDatanodeID);
  }

  /**
   * Ignore datanode by hostname
   * @param lastDatanodeID
   */
  private void ignoreDatanode(String hostname) {
    this.ignoredDatanode = getDFSInputStream().ignodeDatanode(hostname);
  }

  /**
   * Set datanode to ignore by UCARE_SE-Read-1 algorithm and switch datanode if current
   * datanode is equal {@code ignoredDatanode}
   *
   * @param ignoredDatanode
   *          datanode to ignore, by UCARE_SE-Read-1 algorithm.
   * @throws IOException
   */
  public void switchDatanode(DatanodeID ignoredDatanode) throws IOException{
    this.ignoreDatanode(ignoredDatanode);
    if (ignoredDatanode.equals(this.getCurrentDatanode())) {
      synchronized(in){
        this.seekToNewSource(this.getPos());
      }
    }
  }

  /**
   * Set datanode to ignore by UCARE_SE-Read-1 algorithm and switch datanode if current
   * datanode is equal {@code hostname}
   *
   * @param hostname
   *          hostname of datanode to ignore, by UCARE_SE-Read-1 algorithm.
   * @throws IOException
   */
  public void switchDatanode(String hostname) throws IOException{
    this.ignoreDatanode(hostname);
    if (this.ignoredDatanode != null) {
      if (this.ignoredDatanode.equals(this.getCurrentDatanode())) {
        synchronized (in) {
          this.seekToNewSource(this.getPos());
        }
      }
    }
  }
  
  public DatanodeID getCurrentOrChoosenDatanode() {
    return getDFSInputStream().getLastDatanodeID();
  }
}
