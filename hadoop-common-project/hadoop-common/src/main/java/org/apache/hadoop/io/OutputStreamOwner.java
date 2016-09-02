package org.apache.hadoop.io;


import java.io.OutputStream;

/**
 * Created by huanke on 7/12/16.
 */
public interface OutputStreamOwner {
    //huanke
    public OutputStream  getOutputStream();
}
