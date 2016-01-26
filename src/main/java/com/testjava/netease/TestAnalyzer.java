package com.testjava.netease;

import com.netease.jurassic.analyzer.client.ResultServerProtocol;
import com.netease.jurassic.analyzer.data.StatisticResultWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by qmgeng on 2015/8/10.
 */
public class TestAnalyzer {
    public static final Logger LOG = Logger.getLogger(TestAnalyzer.class);

    public static String getData(ResultServerProtocol resultServer, String product, String date, String category,
                                 String keyStr, int pvOuv) throws IOException {
        String res = "";
        if (keyStr.contains("=indepCount")) {
            String k = keyStr.replaceAll("=indepCount", "");
            int[] itemCount = resultServer.getFormItemCountLocally(product, date, category, new String[]{k});

            if (null != itemCount && itemCount.length > 0) {
                res = String.valueOf(itemCount[0]);
            }

        } else {
            StatisticResultWritable statisticResults = resultServer.getStatisticResultHdfs(product, date, category, keyStr);
            if (null != statisticResults) {
                switch (pvOuv) {
                    case 0:// 返回pv
                        res = String.valueOf(statisticResults.getPv());
                        break;
                    case 1:// 返回uv
                        res = String.valueOf(statisticResults.getUv());
                        break;
                    default:
                        break;
                }
            } else {
                LOG.info("null statisticResults, product=" + product + ", date=" + date + ", category=" + category
                        + ", keyStr=" + keyStr + ", pvOuv=" + pvOuv);
            }
        }

        return res;
    }

    public static void main(String[] args) {
        System.out.println("begin...");

        int count = 0;
        Configuration conf = new Configuration();
        try {
            // 读取json
            List<ReaperUrl> reaperUrls = new ArrayList<ReaperUrl>();
            ObjectMapper mapper = new ObjectMapper();
            File fl = new File("/home/weblog/cube_monitor/source/datacube/com.testjava.netease.ReaperUrl.com.testjava.json");
            TypeReference<List<ReaperUrl>> listReaperUrl = new TypeReference<List<ReaperUrl>>() {
            };
            reaperUrls = mapper.readValue(fl, listReaperUrl);
            System.out.println(reaperUrls.size());

            NewAnalyzerReaper reaper = new NewAnalyzerReaper();
            for (int i = 0; i < 20; i++) {
                List<KpiData> kpiDataList = reaper.getData("pro4", reaperUrls, "20150810");
                count++;
                System.out.println(kpiDataList.toString());
            }

            System.out.println("count=" + count);
            System.out.println("sleeping...");
            Thread thread = Thread.currentThread();
            thread.sleep(20 * 60 * 1000);

            for (int i = 0; i < 10; i++) {
                List<KpiData> kpiDataList = reaper.getData("pro4", reaperUrls, "20150810");
                count++;
                System.out.println(kpiDataList.toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("count=" + count);
        }

        System.out.println("count=" + count);

        System.out.println("end...");
    }
}
