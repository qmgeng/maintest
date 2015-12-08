package hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by qmgeng on 15/12/5.
 */
public class BatchHbaseScan {
    //获取店铺一天内各分钟PV值的入口函数
    public static ConcurrentHashMap<String, String> getUnitMinutePV(long uid, long startStamp, long endStamp) {
        long min = startStamp;
        int count = (int) ((endStamp - startStamp) / (60 * 1000));
        List<String> lst = new ArrayList<String>();
        for (int i = 0; i <= count; i++) {
            min = startStamp + i * 60 * 1000;
            lst.add(uid + "_" + min);
        }
        return parallelBatchMinutePV(lst);
    }

    //多线程并发查询，获取行
    private static ConcurrentHashMap<String, String> parallelBatchMinutePV(List<String> lstKeys) {
        ConcurrentHashMap<String, String> hashRet = new ConcurrentHashMap<String, String>();
        int parallel = 3;
        List<List<String>> lstBatchKeys = null;
        if (lstKeys.size() < parallel) {
            lstBatchKeys = new ArrayList<List<String>>(1);
            lstBatchKeys.add(lstKeys);
        } else {
            lstBatchKeys = new ArrayList<List<String>>(parallel);
            for (int i = 0; i < parallel; i++) {
                List<String> lst = new ArrayList<String>();
                lstBatchKeys.add(lst);
            }

            for (int i = 0; i < lstKeys.size(); i++) {
                lstBatchKeys.get(i % parallel).add(lstKeys.get(i));
            }
        }

        List<Future<ConcurrentHashMap<String, String>>> futures = new ArrayList<Future<ConcurrentHashMap<String, String>>>(5);

        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("ParallelBatchQuery");
        ThreadFactory factory = builder.build();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(lstBatchKeys.size(), factory);

        for (List<String> keys : lstBatchKeys) {
            Callable<ConcurrentHashMap<String, String>> callable = new HbaseDataScanner(keys);
            FutureTask<ConcurrentHashMap<String, String>> future = (FutureTask<ConcurrentHashMap<String, String>>) executor.submit(callable);
            futures.add(future);
        }
        executor.shutdown();

        // Wait for all the tasks to finish
        try {
            boolean stillRunning = !executor.awaitTermination(
                    5000000, TimeUnit.MILLISECONDS);
            if (stillRunning) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } catch (InterruptedException e) {
            try {
                Thread.currentThread().interrupt();
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

        // Look for any exception
        for (Future f : futures) {
            try {
                if (f.get() != null) {
                    hashRet.putAll((ConcurrentHashMap<String, String>) f.get());
                }
            } catch (InterruptedException e) {
                try {
                    Thread.currentThread().interrupt();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        return hashRet;
    }


}

//调用接口类，实现Callable接口
class HbaseDataScanner implements Callable<ConcurrentHashMap<String, String>> {
    private List<String> keys;

    public HbaseDataScanner(List<String> lstKeys) {
        this.keys = lstKeys;
    }

    public ConcurrentHashMap<String, String> call() throws Exception {
        return getBatchMinutePV(keys);
    }

    //批量查询
    private ConcurrentHashMap<String, String> getBatchMinutePV(List<String> lstKeys) {
        ConcurrentHashMap<String, String> hashRet = null;
        List<Get> lstGet = new ArrayList<Get>();
        for (String key : lstKeys) {
            Get g = new Get(key.getBytes());
            lstGet.add(g);
        }
        Result[] res = null;
        try {
            //TODO
            res = getTable("datacube:").get(lstGet);
        } catch (IOException e1) {
            System.out.println("tableMinutePV exception, e=" + e1.getStackTrace());
        }

        if (res != null && res.length > 0) {
            hashRet = new ConcurrentHashMap<String, String>(res.length);
            for (Result re : res) {
                if (re != null && !re.isEmpty()) {
                    try {
                        byte[] key = re.getRow();
                        //TODO
                        byte[] value = re.getValue("family".getBytes(), "values".getBytes());
                        if (key != null && value != null) {
                            hashRet.put(String.valueOf(Bytes.toLong(key,
                                    Bytes.SIZEOF_LONG)), String.valueOf(Bytes
                                    .toLong(value)));
                        }
                    } catch (Exception e2) {
                        System.out.println(e2.getStackTrace());
                    }
                }
            }
        }

        return hashRet;
    }

    private HTableInterface getTable(String tableName) {
        HTable table = null;
        Configuration conf = HBaseConfiguration.create();
        try {
            table = new HTable(conf, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }
}
