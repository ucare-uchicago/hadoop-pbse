package org.apache.hadoop.io;


import java.io.OutputStream;

/**
 * Created by huanke on 7/12/16.
 */
public interface OutputStreamOwner {
    public OutputStream  getOutputStream();
}
