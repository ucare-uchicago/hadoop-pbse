package org.apache.hadoop.mapreduce.lib.input;

import java.io.InputStream;

public interface InputStreamOwner {
    
    public InputStream getInputStream();

}
