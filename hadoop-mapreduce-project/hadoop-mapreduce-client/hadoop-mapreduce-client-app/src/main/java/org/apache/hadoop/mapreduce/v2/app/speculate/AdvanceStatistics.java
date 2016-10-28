/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdvanceStatistics {
  private int count = 0;
  private double sum = 0;
  private double sumSquares = 0;
  private Map<Object, Double> data;

  public AdvanceStatistics() {
    this.data = new HashMap<Object, Double>();
  }

  public AdvanceStatistics(Object initKey, double initNum) {
    this.count = 1;
    this.sum = initNum;
    this.sumSquares = initNum * initNum;
    this.data = new HashMap<Object, Double>();
    this.data.put(initKey, initNum);
  }

  public synchronized void add(Object newKey, double newNum) {
    if (!this.data.containsKey(newKey)) {
      this.count++;
      this.sum += newNum;
      this.sumSquares += newNum * newNum;
      this.data.put(newKey, newNum);
    }
  }

  public synchronized void updateStatistics(Object key, double update) {
    Double old = this.data.get(key);
    if (old != null) {
      this.sum += update - old;
      this.sumSquares += (update * update) - (old * old);
      this.data.put(key, update);
    }
  }

  public synchronized void addOrUpdate(Object key, double update) {
    if (!this.data.containsKey(key)) {
      add(key, update);
    } else {
      updateStatistics(key, update);
    }
  }

  public synchronized double mean() {
    return count == 0 ? 0.0 : sum/count;
  }

  public synchronized double var() {
    // E(X^2) - E(X)^2
    if (count <= 1) {
      return 0.0;
    }
    double mean = mean();
    return Math.max((sumSquares/count) - mean * mean, 0.0d);
  }

  public synchronized double std() {
    return Math.sqrt(this.var());
  }

  public synchronized double outlier(float sigma) {
    if (count != 0.0) {
      return mean() + std() * sigma;
    }

    return 0.0;
  }

  public synchronized double count() {
    return count;
  }
  
  public synchronized double median() {
    if (data.size() <= 0)
      return 0.0;
    List<Double> values = new ArrayList<Double>(data.values());
    Collections.sort(values);
    return values.get(values.size() / 2);
  }

  public String toString() {
    return "DataStatistics: count is " + count + ", sum is " + sum +
    ", sumSquares is " + sumSquares + " mean is " + mean() + " std() is " + std() +
    ", median is " + median();
  }
}
