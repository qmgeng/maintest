package com.testjava.hbase;

import com.testjava.bean.Student;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by qmgeng on 2015/11/11.
 */
public class HBaseTest {
    // 自动加载hbase-site.xml
    static Configuration conf = HBaseConfiguration.create();

    static {
        // kerberos的配置文件的位置,windows下叫krb5.ini,linux下叫krb5.conf
        //System.setProperty("java.security.krb5.conf", "C:\\Windows\\krb5.ini");

        // win下的配置，防止出现winutil.exe的异常，hadoop.home.dir\bin目录下要有编译好的exe
        // 不配这个属性也没影响，就是会有个异常，不影响运行
        //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.2");

        // conf.set("com.testjava.hbase.zookeeper.quorum", "hbase0.photo.163.org,hbase1.photo.163.org,hbase2.photo.163.org");
        // conf.set("com.testjava.hbase.zookeeper.property.clientPort", "2181");

        // 使用keytab登陆
        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("weblog/dev@HADOOP.HZ.NETEASE.COM", "/home/weblog/weblog.keytab");
            // 定时调用更新kerberos（10小时过期），推荐用守护线程定期调用
            UserGroupInformation.getCurrentUser().reloginFromKeytab();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private ExecutorService pool = Executors.newFixedThreadPool(10);    // 这里创建了10个 Active RPC Calls

    /*
    * 根据rwokey查询
    *
    * @rowKey rowKey
    *
    * @tableName 表名
    */
    public static Result getResult(String tableName, String rowKey)
            throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// 获取表
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        for(Cell cell:result.rawCells()){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
        return result;
    }

    /*
    * 遍历查询hbase表
    *
    * @tableName 表名
    */
    public static void getResultScann(String tableName, String start_rowkey,
                                      String stop_rowkey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:"
                            + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /*
    * 查询表中的某一列
    *
    * @tableName 表名
    *
    * @rowKey rowKey
    */
    public static void getResultByColumn(String tableName, String rowKey,
                                         String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }

    /*
    * 查询某列数据的多个版本
    *
    * @tableName 表名
    *
    * @rowKey rowKey
    *
    * @familyName 列族名
    *
    * @columnName 列名
    */
    public static void getResultByVersion(String tableName, String rowKey,
                                          String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        /*
         * List<?> results = table.get(get).list(); Iterator<?> it =
         * results.iterator(); while (it.hasNext()) {
         * System.out.println(it.next().toString()); }
         */
    }

    public List<Student> getDatasFromHbase(final List<String> rowKeys,
                                           final List<String> filterColumn, boolean isContiansRowkeys,
                                           boolean isContainsList) {
        if (rowKeys == null || rowKeys.size() <= 0) {
            return new ArrayList<>();
        }
        final int maxRowKeySize = 1000;
        int loopSize = rowKeys.size() % maxRowKeySize == 0 ? rowKeys.size()
                / maxRowKeySize : rowKeys.size() / maxRowKeySize + 1;
        ArrayList<Future<List<Student>>> results = new ArrayList<Future<List<Student>>>();
        for (int loop = 0; loop < loopSize; loop++) {
            int end = (loop + 1) * maxRowKeySize > rowKeys.size() ? rowKeys
                    .size() : (loop + 1) * maxRowKeySize;
            List<String> partRowKeys = rowKeys.subList(loop * maxRowKeySize,
                    end);
            HbaseDataGetter hbaseDataGetter = new HbaseDataGetter(partRowKeys,
                    filterColumn, isContiansRowkeys, isContainsList);
            synchronized (pool) {
                Future<List<Student>> result = pool.submit(hbaseDataGetter);
                results.add(result);
            }
        }

        List<Student> students = new ArrayList<Student>();
        try {
            for (Future<List<Student>> result : results) {
                List<Student> rd = result.get();
                students.addAll(rd);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return students;
    }


}
