package org.atguigu.mr.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * OrderBean将作为Key，需要实现Writable接口
 * 由于OrderBean需要在网络中传输，因此需要实现Writable方法
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private int id;
    private int pid;
    private int amount;
    private String pname;

    public OrderBean() {

    }

    public OrderBean(int id, int pid, int amount, String pname) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.pname = pname;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    @Override
    public String toString() {
//        return "OrderBean{" +
//                "id=" + id +
//                ", pid=" + pid +
//                ", amount=" + amount +
//                ", pname='" + pname + '\'' +
//                '}';
//    }
        return id + " " + pid + " " + pname + " " + amount;
    }

    @Override
    public int compareTo(OrderBean o) {
        //先按照Pid进行排序，如果Pid相同再按照pname排序
        int cpid = this.pid - o.getPid();
        if (cpid == 0){
            return -this.pname.compareTo(o.getPname());
        }
        return cpid;
    }

    /**
     * 排序方法
     * @param obj the object to be compared.
     * @return
     */
//    @Override
//    public int compareTo(Object obj) {
//        OrderBean o = (OrderBean) obj;//向下转型
//        // 先按照pid排序，如果pid相同，再按照pname排序
//        int cpid = this.pid - o.getPid();
//        if(cpid == 0){
//            return this.pname.compareTo(o.getPname());
//        }
//        return cpid;
//    }

    /**
     * 序列化时调用的方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeInt(pid);
        dataOutput.writeInt(amount);
//        dataOutput.writeBytes(pname);
        dataOutput.writeUTF(pname);
    }

    /**
     * 反序列化调用的方法：要和序列化顺序保持一致
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        pid = dataInput.readInt();
        amount = dataInput.readInt();
        pname = dataInput.readUTF();
    }

}
