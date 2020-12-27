package com.javaboy.mr.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class FlowBean implements Writable {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean(){
        super();
    }

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    /**
     * 序列化方法
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    /**
     * 反序列化方法
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow ;
    }

    public void set(long upFlow2, long downFlow2) {
        this.upFlow = upFlow2;
        this.downFlow = downFlow2;
        this.sumFlow = upFlow2 + downFlow2;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public FlowBean setUpFlow(long upFlow) {
        this.upFlow = upFlow;
        return this;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public FlowBean setDownFlow(long downFlow) {
        this.downFlow = downFlow;
        return this;
    }
}
