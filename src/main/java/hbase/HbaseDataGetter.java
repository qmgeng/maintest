//package hbase;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.Callable;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.HTableInterface;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.util.Bytes;
//
///**
// * Created by qmgeng on 2015/11/11.
// */
//public class HbaseDataGetter implements Callable<List<String>> {
//  private List<String> rowKeys;
//  private List<String> filterColumn;
//  private boolean isContiansRowkeys;
//  private boolean isContainsList;
//  private String tableName = "";
//
//  public HbaseDataGetter(List<String> rowKeys, List<String> filterColumn, boolean isContiansRowkeys,
//      boolean isContainsList) {
//    this.rowKeys = rowKeys;
//    this.filterColumn = filterColumn;
//    this.isContiansRowkeys = isContiansRowkeys;
//    this.isContainsList = isContainsList;
//  }
//
//  @Override
//  public List<String> call() throws Exception {
//    Object[] objects = getDatasFromHbase(rowKeys, filterColumn);
//    List<String> listData = new ArrayList<String>();
//    for (Object object : objects) {
//      Result r = (Result) object;
//      String data = assembleData(r, filterColumn, isContiansRowkeys, isContainsList);
//      listData.add(data);
//    }
//    return listData;
//  }
//
//  private Object[] getDatasFromHbase(List<String> rowKeys, List<String> filterColumn) {
//    createTable(tableName);
//    Object[] objects = null;
//    HTableInterface hTableInterface = createTable(tableName);
//    List<Get> listGets = new ArrayList<Get>();
//    for (String rk : rowKeys) {
//      Get get = new Get(Bytes.toBytes(rk));
//      if (filterColumn != null) {
//        for (String column : filterColumn) {
//          get.addColumn(columnFamilyName.getBytes(), column.getBytes());
//        }
//      }
//      listGets.add(get);
//    }
//    try {
//      objects = hTableInterface.get(listGets);
//    } catch (IOException e1) {
//      e1.printStackTrace();
//    } finally {
//      try {
//        listGets.clear();
//        hTableInterface.close();
//      } catch (IOException e) {
//        e.printStackTrace();
//      }
//    }
//    return objects;
//  }
//
//  private HTableInterface createTable(String tableName) {
//    HTable table = null;
//    Configuration conf = HBaseConfiguration.create();
//    try {
//      table = new HTable(conf, tableName);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//    return table;
//  }
//
//}
