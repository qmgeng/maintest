//package hbase;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
//import org.apache.hadoop.hbase.filter.Filter;
//import org.apache.hadoop.hbase.filter.FilterList;
//import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
//import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.security.UserGroupInformation;
//
//import java.io.IOException;
//import java.util.LinkedHashMap;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//
//public class HbaseUtils {
//    private static Configuration config = null;
//    private static HTablePool tp = null;
//    // �Զ�����hbase-site.xml
//    static Configuration conf = HBaseConfiguration.create();
//
//    static {
//        // ʹ��keytab��½
//        UserGroupInformation.setConfiguration(conf);
//        try {
//            UserGroupInformation.loginUserFromKeytab("weblog/dev@HADOOP.HZ.NETEASE.COM", "/home/weblog/weblog.keytab");
//            // ��ʱ���ø���kerberos��10Сʱ���ڣ����Ƽ����ػ��̶߳��ڵ���
//            UserGroupInformation.getCurrentUser().reloginFromKeytab();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /*
//     * ��ȡhbase�ı�
//     */
//    public static HTableInterface getTable(String tableName) {
//
//        if (StringUtils.isEmpty(tableName))
//            return null;
//
//        return tp.getTable(getBytes(tableName));
//    }
//
//    /* ת��byte���� */
//    public static byte[] getBytes(String str) {
//        if (str == null)
//            str = "";
//
//        return Bytes.toBytes(str);
//    }
//
//    /**
//     * ��ѯ����
//     */
//    public static TBData getDataMap(String tableName, String startRow,
//                                    String stopRow, Integer currentPage, Integer pageSize)
//            throws IOException {
//        List<Map<String, String>> mapList = null;
//        mapList = new LinkedList<Map<String, String>>();
//
//        ResultScanner scanner = null;
//        // Ϊ��ҳ�����ķ�װ����������и�����������
//        TBData tbData = null;
//        try {
//            // ��ȡ��󷵻ؽ������
//            if (pageSize == null || pageSize == 0L)
//                pageSize = 100;
//
//            if (currentPage == null || currentPage == 0)
//                currentPage = 1;
//
//            // ������ʼҳ�ͽ���ҳ
//            Integer firstPage = (currentPage - 1) * pageSize;
//
//            Integer endPage = firstPage + pageSize;
//
//            // �ӱ����ȡ��HBASE�����
//            HTableInterface table = getTable(tableName);
//            // ��ȡɸѡ����
//            Scan scan = getScan(startRow, stopRow);
//            // ��ɸѡ������������(true��ʶ��ҳ,���巽��������)
//            scan.setFilter(packageFilters(true));
//            // ����1000������
//            scan.setCaching(1000);
//            scan.setCacheBlocks(false);
//            scanner = table.getScanner(scan);
//            int i = 0;
//            List<byte[]> rowList = new LinkedList<byte[]>();
//            // ����ɨ�������� ������Ҫ��ѯ����������row keyȡ��
//            for (Result result : scanner) {
//                String row = toStr(result.getRow());
//                if (i >= firstPage && i < endPage) {
//                    rowList.add(getBytes(row));
//                }
//                i++;
//            }
//
//            // ��ȡȡ����row key��GET����
//            List<Get> getList = getList(rowList);
//            Result[] results = table.get(getList);
//            // �������
//            for (Result result : results) {
//                Map<byte[], byte[]> fmap = packFamilyMap(result);
//                Map<String, String> rmap = packRowMap(fmap);
//                mapList.add(rmap);
//            }
//
//            // ��װ��ҳ����
//            tbData = new TBData();
//            tbData.setCurrentPage(currentPage);
//            tbData.setPageSize(pageSize);
//            tbData.setTotalCount(i);
//            tbData.setTotalPage(getTotalPage(pageSize, i));
//            tbData.setResultList(mapList);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            closeScanner(scanner);
//        }
//
//        return tbData;
//    }
//
//    private static int getTotalPage(int pageSize, int totalCount) {
//        int n = totalCount / pageSize;
//        if (totalCount % pageSize == 0) {
//            return n;
//        } else {
//            return ((int) n) + 1;
//        }
//    }
//
//    // ��ȡɨ��������
//    private static Scan getScan(String startRow, String stopRow) {
//        Scan scan = new Scan();
//        scan.setStartRow(getBytes(startRow));
//        scan.setStopRow(getBytes(stopRow));
//
//        return scan;
//    }
//
//    /**
//     * ��װ��ѯ����
//     */
//    private static FilterList packageFilters(boolean isPage) {
//        FilterList filterList = null;
//        // MUST_PASS_ALL(���� AND) MUST_PASS_ONE������OR��
//        filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        Filter filter1 = null;
//        Filter filter2 = null;
//        filter1 = newFilter(getBytes("family1"), getBytes("column1"),
//                CompareOp.EQUAL, getBytes("condition1"));
//        filter2 = newFilter(getBytes("family2"), getBytes("column1"),
//                CompareOp.LESS, getBytes("condition2"));
//        filterList.addFilter(filter1);
//        filterList.addFilter(filter2);
//        if (isPage) {
//            filterList.addFilter(new FirstKeyOnlyFilter());
//        }
//        return filterList;
//    }
//
//    private static Filter newFilter(byte[] f, byte[] c, CompareOp op, byte[] v) {
//        return new SingleColumnValueFilter(f, c, op, v);
//    }
//
//    private static void closeScanner(ResultScanner scanner) {
//        if (scanner != null)
//            scanner.close();
//    }
//
//    /**
//     * ��װÿ������
//     */
//    private static Map<String, String> packRowMap(Map<byte[], byte[]> dataMap) {
//        Map<String, String> map = new LinkedHashMap<String, String>();
//
//        for (byte[] key : dataMap.keySet()) {
//
//            byte[] value = dataMap.get(key);
//
//            map.put(toStr(key), toStr(value));
//
//        }
//        return map;
//    }
//
//    /* ����ROW KEY���ϻ�ȡGET���󼯺� */
//    private static List<Get> getList(List<byte[]> rowList) {
//        List<Get> list = new LinkedList<Get>();
//        for (byte[] row : rowList) {
//            Get get = new Get(row);
//
//            get.addColumn(getBytes("family1"), getBytes("column1"));
//            get.addColumn(getBytes("family1"), getBytes("column2"));
//            get.addColumn(getBytes("family2"), getBytes("column1"));
//            list.add(get);
//        }
//        return list;
//    }
//
//    /**
//     * ��װ���õ������ֶ�����
//     */
//    private static Map<byte[], byte[]> packFamilyMap(Result result) {
//        Map<byte[], byte[]> dataMap = null;
//        dataMap = new LinkedHashMap<byte[], byte[]>();
//        dataMap.putAll(result.getFamilyMap(getBytes("family1")));
//        dataMap.putAll(result.getFamilyMap(getBytes("family2")));
//        return dataMap;
//    }
//
//    private static String toStr(byte[] bt) {
//        return Bytes.toString(bt);
//    }
//
//    public static void main(String[] args) throws IOException {
//        // �ó�row key����ʼ�кͽ�����
//        // #<0<9<:
//        String startRow = "aaaa#";
//        String stopRow = "aaaa:";
//        int currentPage = 1;
//        int pageSize = 20;
//        // ִ��hbase��ѯ
//        System.out.println(getDataMap("datacube:evaluation", startRow, stopRow, currentPage, pageSize));
//
//
//        System.out.println(getDataMap("datacube:evaluation", startRow, stopRow, currentPage, pageSize));
//
//
//        System.out.println(getDataMap("datacube:evaluation", startRow, stopRow, currentPage, pageSize));
//
//    }
//}
//
//class TBData {
//    private Integer currentPage;
//    private Integer pageSize;
//    private Integer totalCount;
//    private Integer totalPage;
//    private List<Map<String, String>> resultList;
//
//    public Integer getCurrentPage() {
//        return currentPage;
//    }
//
//    public void setCurrentPage(Integer currentPage) {
//        this.currentPage = currentPage;
//    }
//
//    public Integer getPageSize() {
//        return pageSize;
//    }
//
//    public void setPageSize(Integer pageSize) {
//        this.pageSize = pageSize;
//    }
//
//    public Integer getTotalCount() {
//        return totalCount;
//    }
//
//    public void setTotalCount(Integer totalCount) {
//        this.totalCount = totalCount;
//    }
//
//    public Integer getTotalPage() {
//        return totalPage;
//    }
//
//    public void setTotalPage(Integer totalPage) {
//        this.totalPage = totalPage;
//    }
//
//    public List<Map<String, String>> getResultList() {
//        return resultList;
//    }
//
//    public void setResultList(List<Map<String, String>> resultList) {
//        this.resultList = resultList;
//    }
//
//    @Override
//    public String toString() {
//        return "TBData{" +
//                "currentPage=" + currentPage +
//                ", pageSize=" + pageSize +
//                ", totalCount=" + totalCount +
//                ", totalPage=" + totalPage +
//                ", resultList=" + resultList +
//                '}';
//    }
//}
