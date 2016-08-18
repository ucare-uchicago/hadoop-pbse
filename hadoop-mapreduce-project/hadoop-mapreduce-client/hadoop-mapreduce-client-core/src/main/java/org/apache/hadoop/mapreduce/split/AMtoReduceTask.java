package org.apache.hadoop.mapreduce.split;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringInterner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by huanke on 7/31/16.
 */

   public class AMtoReduceTask {

    private static final Log LOG = LogFactory.getLog(AMtoReduceTask.class);
//    private String monitorString = "";

    // huanke
    private String info = "";

    public AMtoReduceTask() {
        LOG.info("@huanke  create AMtaskInfo");
    }

//    public void monitor(){
//        (new Thread(new MonitorRunnable())).start();
//    }

    public static AMtoReduceTask createNullInfo() {
        return new AMtoReduceTask();
    }


    public void setAMtoTaskInfo(String info) {
        this.info = info;
    }

    public String getAMtaskInfo() {
        return this.info;
    }

//    public String getMonitorString(){return this.monitorString;}

//    public void statusUpdate() {
//        monitorString = info;
//    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, info);
        //out.writeInt(partition); the format to write int type
        //out.writeBoolean(b); the format to write boolean type
        //Text.wrteString(out, info); the format to write String type
        //AMtoReduceTask.write(out); the format to write a new Class type, create the write function on that class AMtoRedueceTask
    }

    public void readFields(DataInput in) throws IOException {
        info= StringInterner.weakIntern(Text.readString(in));
        //info= StringInterner.weakIntern(Text.readString(in)); the format to read DataInput into the String Type
        //skip =in.readBoolean(); the format to read DataInput into boolean Type
        //partition=in.readInt(); the format to read DataInput into int Type
        //AMtoReduceTask.readFields(in); the format to read DataInput to a new Class AMtoReduceTask
    }


//    class MonitorRunnable implements Runnable {
//
//        AMtoReduceTask tmp=new AMtoReduceTask();
//
//        public void run() {
//            while(true){
//                tmp.statusUpdate();
//                try {
//                    Thread.currentThread().sleep(10);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//
//        }
//
//    }



}
