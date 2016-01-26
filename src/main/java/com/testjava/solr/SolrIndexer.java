package com.testjava.solr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;

public class SolrIndexer {

    /**
     * @param args
     * @throws IOException
     * @throws SolrServerException
     */
    public static void main(String[] args) throws IOException, SolrServerException {
        HttpSolrServer solrServer = new HttpSolrServer("http://123.58.179.98:8983/com.testjava.solr/gettingstarted");

        Configuration conf = HBaseConfiguration.create();
        // 使用keytab登陆
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("weblog/dev@HADOOP.HZ.NETEASE.COM", "/home/weblog/weblog.keytab");

        // 定时调用更新kerberos（10小时过期），推荐用守护线程定期调用
        UserGroupInformation.getCurrentUser().reloginFromKeytab();


        HTable table = new HTable(conf, "datacube:onlinedchtuserremainiphone"); // 这里指定HBase表名称
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("value")); // 这里指定HBase表的列族
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        ResultScanner ss = table.getScanner(scan);

        System.out.println("start ...");
        int i = 0;
        try {
            for (Result r : ss) {
                SolrInputDocument solrDoc = new SolrInputDocument();
                solrDoc.addField("rowkey", new String(r.getRow()));
                for (KeyValue kv : r.raw()) {
                    String fieldName = new String(kv.getQualifier());
                    String fieldValue = new String(kv.getValue());
                    if (fieldName.equalsIgnoreCase("day") || fieldName.equalsIgnoreCase("2day")) {
                        solrDoc.addField(fieldName, fieldValue);
                    }
                }
                solrServer.add(solrDoc);
                solrServer.commit(true, true, true);
                i = i + 1;
                System.out.println("已经成功处理 " + i + " 条数据");
            }


            ss.close();
            table.close();
            System.out.println("done !");
        } catch (IOException e) {
        } finally {
            ss.close();
            table.close();

            solrServer.optimize();// 不要频繁的调用..尽量在无人使用时调用.
            System.out.println("finally !");
        }
    }

}
