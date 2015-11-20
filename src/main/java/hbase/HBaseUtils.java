//package hbase;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//
///**
// * Created by qmgeng on 2015/11/11.
// */
//public class HBaseUtils {
//    private ExecutorService pool = Executors.newFixedThreadPool(10);    // 这里创建了10个 Active RPC Calls
//    public Datas getDatasFromHbase(final List<String> rowKeys,
//        final List<String> filterColumn, boolean isContiansRowkeys,
//        boolean isContainsList)
//    {
//        if (rowKeys == null || rowKeys.size() <= 0)
//        {
//            return Datas.getEmptyDatas();
//        }
//        final int maxRowKeySize = 1000;
//        int loopSize = rowKeys.size() % maxRowKeySize == 0 ? rowKeys.size()
//            / maxRowKeySize : rowKeys.size() / maxRowKeySize + 1;
//        ArrayList<Future<List<Data>>> results = new ArrayList<Future<List<Data>>>();
//        for (int loop = 0; loop < loopSize; loop++)
//        {
//            int end = (loop + 1) * maxRowKeySize > rowKeys.size() ? rowKeys
//                .size() : (loop + 1) * maxRowKeySize;
//            List<String> partRowKeys = rowKeys.subList(loop * maxRowKeySize,
//                end);
//            HbaseDataGetter hbaseDataGetter = new HbaseDataGetter(partRowKeys,
//                filterColumn, isContiansRowkeys, isContainsList);
//            synchronized (pool)
//            {
//                Future<List<Data>> result = pool.submit(hbaseDataGetter);
//                results.add(result);
//            }
//        }
//        Datas datas = new Datas();
//        List<Data> dataQueue = new ArrayList<Data>();
//        try
//        {
//            for (Future<List<Data>> result : results)
//            {
//                List<Data> rd = result.get();
//                dataQueue.addAll(rd);
//            }
//            datas.setDatas(dataQueue);
//        }
//        catch (InterruptedException | ExecutionException e)
//        {
//            e.printStackTrace();
//        }
//        return datas;
//    }
//
//}
