package org.apache.hadoop.mapreduce.lib.output;


import java.io.OutputStream;

/**
 * Created by huanke on 7/12/16.
 */
public interface OutputStreamOwner {
    public OutputStream  getOutputStream();
}
