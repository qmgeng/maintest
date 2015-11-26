package solr;

import bean.Student;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.servlet.SolrRequestParsers;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CloudSolrServerTest {
    private static CloudSolrServer cloudSolrServer;

    //    private static final SolrServer solrServer2 = new ConcurrentUpdateSolrServer(
//        solrUrl, 10000, 20);
    private static synchronized CloudSolrServer getCloudSolrServer(final String zkHost) {
        if (cloudSolrServer == null) {
            try {
                cloudSolrServer = new CloudSolrServer(zkHost);
            } catch (MalformedURLException e) {
                System.out.println("The URL of zkHost is not correct!! Its form must as below:\n zkHost:port");
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return cloudSolrServer;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        final String zkHost = "123.58.179.51:2181,123.58.179.52:2181,123.58.179.53:2181/solr";
        final String defaultCollection = "collection1";
        final int zkClientTimeout = 20000;
        final int zkConnectTimeout = 1000;
        CloudSolrServer cloudSolrServer = getCloudSolrServer(zkHost);
        System.out.println("The Cloud SolrServer Instance has benn created!");
        cloudSolrServer.setDefaultCollection(defaultCollection);
        cloudSolrServer.setZkClientTimeout(zkClientTimeout);
        cloudSolrServer.setZkConnectTimeout(zkConnectTimeout);
        cloudSolrServer.connect();
        System.out.println("The cloud Server has been connected !!!!");
        ZkStateReader zkStateReader = cloudSolrServer.getZkStateReader();
        ClusterState cloudState = zkStateReader.getClusterState();
        System.out.println(cloudState);
        // 测试实例！
        CloudSolrServerTest test = new CloudSolrServerTest();
        System.out.println("测试添加index！！！");
        test.addIndex(cloudSolrServer);
        System.out.println("测试查询query！！！！");
        test.search(cloudSolrServer, "id:*");

//        System.out.println("测试删除！！！！");
//        test.deleteAllIndex(cloudSolrServer);
//        System.out.println("删除所有文档后的查询结果：");
//        test.search(cloudSolrServer, "*:*");

//    long s1 = System.currentTimeMillis();
//    test.addRandomIndex(cloudSolrServer);
//    long s2 = System.currentTimeMillis();
//    System.out.println(s2 - s1);


        test.search(cloudSolrServer, "id:9999");


        // 查询语法
        SolrQuery q = new SolrQuery();
        // 基本的字段查询
        q.setQuery("TITLE:中国人");
        // 多字段或关系 TITLE:("中国人" AND "美国人" AND "英国人")
        // 多字段不包含的关系 TITLE:(* NOT "上网费用高" NOT "宽带收费不合理" )
        // 查询一个范围 BETWEEN 适用于数字和日期类型 NUM:[-90 TO 360 ] OR CREATED_AT:[" + date1 + " TO " + date2 + "]
        // 日期转换 不是惯用的 yyyy-MM-dd HH:mm:ss
        //String date1 = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(c.getStartTime().getTime());


        // release the resource
        cloudSolrServer.shutdown();
    }

    public static void solrOrder(CloudSolrServer solr) throws SolrServerException {
        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.addSortField("id", SolrQuery.ORDER.asc);
        QueryResponse rsp = solr.query(query);
        List<Student> beans = rsp.getBeans(Student.class);
        for (int i = 0; i < beans.size(); i++) {
            System.out.println(beans.get(i).getName());
        }
    }

    public static void likeQuery(CloudSolrServer solr) throws SolrServerException {
        SolrQuery query = new SolrQuery();
        query.setQuery("name:*天天*");
        QueryResponse rsp = solr.query(query);
        List<Student> beans = rsp.getBeans(Student.class);
        for (int i = 0; i < beans.size(); i++) {
            System.out.println(beans.get(i).getName());
        }
    }

    public static void pageQuery(CloudSolrServer solr) throws SolrServerException {
        SolrQuery query = new SolrQuery();
        query.setQuery("name:*天天*");
        query.setStart(0);
        query.setRows(10);
        QueryResponse rsp = solr.query(query);
        List<Student> beans = rsp.getBeans(Student.class);
        for (int i = 0; i < beans.size(); i++) {
            System.out.println(beans.get(i).getName());
        }
    }

    public static void multipleQuery1(CloudSolrServer solr) throws SolrServerException {
        SolrQuery query = new SolrQuery();
        query.setQuery("artist:*Tencent* name:*天天*");// 多条件 ||(或)的情况 多条件使用空格分隔
        query.setFields("name", "id_in_appstore", "artist");
        QueryResponse rsp = solr.query(query);
        List<Student> beans = rsp.getBeans(Student.class);
        for (int i = 0; i < beans.size(); i++) {
            System.out.println(beans.get(i).getName());
        }
    }

    //name包含"天天"且artist包含“Tencent”
    public static void multipleQuery2(CloudSolrServer solr) throws SolrServerException {
        SolrQuery query = new SolrQuery();
        query.setQuery("name:*天天*");// 多条件使用空格分隔
        query.setFilterQueries("artist:*Tencent*");
        query.setFields("name", "id_in_appstore", "artist");
        QueryResponse rsp = solr.query(query);
        List<Student> beans = rsp.getBeans(Student.class);
        for (int i = 0; i < beans.size(); i++) {
            System.out.println(beans.get(i).getName());
        }
    }

    /**
     * 查询总入口
     *
     * @param fields     查询字段
     * @param values     查询key值 field:key
     * @param start      起始位置
     * @param count      读取总数
     * @param sortfields 排序字段
     * @param flags      排序标志
     * @param fecteField 分面统计字段
     * @return QueryResponse
     * @author:Jonathan.Wei
     * @date:2013-11-27
     */
    public static QueryResponse search(SolrServer solr, String[] fields, String[] values,
                                       String[] fqs, String[] fqValues, int start, int count,
                                       String[] sortfields, Boolean[] flags, String[] fecteField) {
        // 检测输入是否合法
        if (null == fields || null == values || fields.length != values.length) {
            return null;
        }
        if (null == sortfields || null == flags
                || sortfields.length != flags.length) {
            return null;
        }
        SolrQuery query = null;
        try {
            // 初始化查询对象
            query = new SolrQuery();
            query.setQuery(fields[0] + ":" + values[0]);
            // 设置起始位置与返回结果数
            if (start != 0) {
                query.setStart(start);
            }
            if (count != 0) {
                query.setRows(count);
            }
            if (null != fecteField) {
                query.setFacet(true);
                query.setFacetLimit(20);
                query.setFacetMinCount(1);
                query.addFacetField(fecteField);
            }
            boolean isFq = false;
            if (fqs != null && fqs.length > 0) {
                if (fqs.length == fqValues.length) {
                    isFq = true;
                }
            }
            if (isFq) {
                for (int i = 0; i < flags.length; i++) {
                    String fq = fqs[i] + ":" + fqValues[i];
                    query.setFilterQueries(fq);
                }
            }
            // 设置排序
            for (int i = 0; i < sortfields.length; i++) {
                if (flags[i]) {
                    query.addSortField(sortfields[i], SolrQuery.ORDER.asc);
                } else {
                    query.addSortField(sortfields[i], SolrQuery.ORDER.desc);
                }
            }
        } catch (Exception e) {
        }

        QueryResponse rsp = null;
        try {
            rsp = solr.query(query);
        } catch (Exception e) {
            return null;
        }
        // 返回查询结果
        return rsp;
    }

    private void addIndex(SolrServer solrServer) {
        try {
            SolrInputDocument doc1 = new SolrInputDocument();
            doc1.addField("id", "1");
            doc1.addField("user_name", "张民");
            SolrInputDocument doc2 = new SolrInputDocument();
            doc2.addField("id", "2");
            doc2.addField("user_name", "刘俊");
            SolrInputDocument doc3 = new SolrInputDocument();
            doc3.addField("id", "3");
            doc3.addField("user_name", "刘俊2");
            Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
            docs.add(doc1);
            docs.add(doc2);
            docs.add(doc3);
            solrServer.add(docs);
            solrServer.commit();
        } catch (SolrServerException e) {
            System.out.println("Add docs Exception !!!");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Unknowned Exception!!!!!");
            e.printStackTrace();
        }
    }

    private void addRandomIndex(SolrServer solrServer) {
        int i = 0;
        while (i < 10000) {
            try {
                for (int j = 0; j < 100; j++) {
                    SolrInputDocument doc1 = new SolrInputDocument();
                    doc1.addField("id", String.valueOf(i));
                    doc1.addField("user_name", "张民");
                    Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
                    docs.add(doc1);
                    solrServer.add(docs);
                    i++;
                }
                solrServer.commit();
            } catch (SolrServerException e) {
                System.out.println("Add docs Exception !!!");
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                System.out.println("Unknowned Exception!!!!!");
                e.printStackTrace();
            }
        }

    }

    public void search(SolrServer solrServer, String String) {
        SolrQuery query = new SolrQuery();
        query.setQuery(String);
        try {
            QueryResponse response = solrServer.query(query);
            SolrDocumentList docs = response.getResults();
            System.out.println("文档个数：" + docs.getNumFound());
            System.out.println("查询时间：" + response.getQTime());
            for (SolrDocument doc : docs) {
                String name = (String) doc.getFieldValue("user_name");
                String id = (String) doc.getFieldValue("id");
                System.out.println("id: " + id);
                System.out.println("name: " + name);
                System.out.println();
            }
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Unknowned Exception!!!!");
            e.printStackTrace();
        }
    }

    public void deleteAllIndex(SolrServer solrServer) {
        try {
            solrServer.deleteByQuery("*:*");// delete everything!
            solrServer.commit();
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Unknowned Exception !!!!");
            e.printStackTrace();
        }
    }

    public void query01(SolrServer solr, String queryString) {
        SolrParams solrParams = SolrRequestParsers
                .parseQueryString(queryString);
        try {
            QueryResponse rsp = solr.query(solrParams);
//            List<PatentBean> results = rsp.getBeans(PatentBean.class);
//            for (PatentBean bean : results)
//                System.out.println(bean.toString());
            rsp.getResults();
        } catch (SolrServerException e) {
            e.printStackTrace();
        }
    }


//    {
//        "id": 310683,
//            "name": "A Blobber Popper",
//            "type_name": "家庭游戏",
//            "app_updated_time": "2013-10-16T16:00:00Z",
//            "id_in_data": 718985737,
//            "artist_id": 628,
//            "artist": "Timo Lehtikevari",
//            "type_id": 33,
//            "img_hit": "http://a1227.phobos.apple.com/us/r30/Purple4/v4/45/93/47/4593474d-9d07-f59b-01e2-8788bee510eb/mzl.llswugwf.75x75-65.png",
//            "is_free": true,
//            "_version_": 1453475483495694300
//    }

    public void query02(SolrServer solr, String queryString) {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.add("q", queryString);
        solrParams.add("start", "0");
        solrParams.add("rows", "10");
        try {
            QueryResponse rsp = solr.query(solrParams);
            rsp.getResults();

        } catch (SolrServerException e) {
            e.printStackTrace();
        }
    }
}
