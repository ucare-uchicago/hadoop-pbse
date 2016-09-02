package org.apache.hadoop.io;

import java.io.InputStream;

public interface InputStreamOwner {
    
    public InputStream getInputStream();

}
