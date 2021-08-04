package com.bigdata.memo;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
  // 上传流量
  private long upFlow;
  // 下载流量
  private long downFlow;
  // 流量总和
  private long sumFlow;

  public FlowBean() {}

  public FlowBean(long upFlow, long downFlow) {
    this.upFlow = upFlow;
    this.downFlow = downFlow;
    this.sumFlow = upFlow + downFlow;
  }

  public void set(long upflow, long downFlow) {
    this.upFlow = upflow;
    this.downFlow = downFlow;
    this.sumFlow = upflow + downFlow;
  }

  public long getUpFlow() {
    return this.upFlow;
  }

  public long getDownFlow() {
    return this.downFlow;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeLong(upFlow);
    dataOutput.writeLong(downFlow);
    dataOutput.writeLong(sumFlow);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    this.upFlow = dataInput.readLong();
    this.downFlow = dataInput.readLong();
    this.sumFlow = dataInput.readLong();
  }

  @Override
  public String toString() {
    return upFlow + "\t" + downFlow + "\t" + sumFlow;
  }

}
