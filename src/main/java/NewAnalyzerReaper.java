import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.Logger;

import com.netease.jurassic.analyzer.client.ResultServerProtocol;
import com.netease.jurassic.analyzer.data.StatisticResultWritable;
import com.netease.jurassic.analyzer.data.WebResultForm;
import com.netease.jurassic.analyzer.data.WebResultForm.Item;

public class NewAnalyzerReaper {

  protected static final Logger LOG = Logger.getLogger(NewAnalyzerReaper.class);

  public synchronized ResultServerProtocol getResultServer(String pro) throws IOException {
    if (resultServer != null) {
      return resultServer;
    }
    Configuration conf = new Configuration();
    resultServer =
        RPC.getProtocolProxy(
            ResultServerProtocol.class,
            ResultServerProtocol.versionID,
            new InetSocketAddress("10.130.11.38", 19881), null, conf, NetUtils.getDefaultSocketFactory(conf), 0, null).getProxy();

    return resultServer;
//    if (remoteIPAndPort.containsKey(pro)) {
//      if (resultServerMap.get(pro) != null) {
//        return resultServerMap.get(pro);
//      }
//      ResultServerProtocol resultServer =
//          RPC.getProtocolProxy(
//              ResultServerProtocol.class,
//              ResultServerProtocol.versionID,
//              new InetSocketAddress(remoteIPAndPort.get(pro).getIp(), Integer.parseInt(remoteIPAndPort.get(pro)
//                  .getPort())), null, conf, NetUtils.getDefaultSocketFactory(conf), 0, null).getProxy();
//      resultServerMap.put(pro, resultServer);
//      return resultServerMap.get(pro);
//    } else {
//      if (resultServerMap.get("def") != null) {
//        return resultServerMap.get("def");
//      }
//      ResultServerProtocol resultServer =
//          RPC.getProtocolProxy(
//              ResultServerProtocol.class,
//              ResultServerProtocol.versionID,
//              new InetSocketAddress(remoteIPAndPort.get("def").getIp(), Integer.parseInt(remoteIPAndPort.get("def")
//                  .getPort())), null, conf, NetUtils.getDefaultSocketFactory(conf), 0, null).getProxy();
//      resultServerMap.put("def", resultServer);
//      return resultServerMap.get("def");
//    }
  }

  private static ResultServerProtocol resultServer =null;
  private static Map<String, ResultServerProtocol> resultServerMap = new HashMap<String, ResultServerProtocol>();

  /**
   * 解析配置得到ReaperUrl，注意ReaperUrl中未设置key
   */
  public static ReaperUrl parseReaperUrl(String conf) throws Exception {
    ReaperUrl ru = new ReaperUrl();
    String[] arr = conf.split(";");
    if (arr.length != 4) {
      throw new Exception("conf error");
    }
    ru.setPro(arr[0]);
    ru.setCla(arr[1]);
    ru.setStrSeg(arr[3]);
    ru.setUseNew(true);
    if ("pv".equalsIgnoreCase(arr[2])) {
      ru.setPvOuv(0);
    } else if ("uv".equalsIgnoreCase(arr[2])) {
      ru.setPvOuv(1);
    } else {
      throw new Exception("conf error, not pv O uv");
    }

    return ru;
  }

  /**
   * ** pvOuv=0, pv; pvOuv=1, uv;
   * <p/>
   * 获取单个统计项的值，用于配置数据项时的测试
   * 
   * @throws IOException
   */
  public String getData(String proid, String product, String date, String category, String keyStr, int pvOuv)
      throws IOException {
    String res = "";
    getResultServer(proid);
    if (keyStr.contains("=indepCount")) {
      String k = keyStr.replaceAll("=indepCount", "");
      int[] itemCount = resultServer.getFormItemCountLocally(product, date, category, new String[] {k});

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

  /**
   * pvOuv=0, pv; pvOuv=1, uv; 不保证返回结果列表和urls参数顺序相同; 结果列表可能会比urls小，若某个ReaperUrl没能有查询值，则不返回该项的查询结果。
   *
   * @throws IOException
   */
  public List<KpiData> getData(String proid, List<ReaperUrl> urls, String reaperDay) throws IOException {
    System.out.println("proid="+proid+",urls=" + urls.size() + ", reaperDay=" + reaperDay);
    getResultServer(proid);
    List<KpiData> res = new ArrayList<KpiData>();

    Map<String, List<ReaperUrl>> urlGroupMap = new HashMap<String, List<ReaperUrl>>();// <(product,category),

    for (ReaperUrl reaperUrl : urls) {
      String key = reaperUrl.getPro() + "," + reaperUrl.getCla() + "," + reaperUrl.checkIndepCount();
      List<ReaperUrl> list = urlGroupMap.get(key);
      if (null == list) {
        list = new ArrayList<ReaperUrl>();
        urlGroupMap.put(key, list);
      }

      list.add(reaperUrl);
    }


    for (String proCategory : urlGroupMap.keySet()) {
      List<ReaperUrl> urlGroup = urlGroupMap.get(proCategory);
      String[] args = new String[urlGroup.size()];

      for (int i = 0; i < args.length; ++i) {
        args[i] = urlGroup.get(i).parseArgs();
      }
      String[] arr = proCategory.split(",");
      String pro = arr[0];
      String category = arr[1];
      boolean isIndepCount = Boolean.valueOf(arr[2]);

      try {
        if (isIndepCount) {
          int itemCount[] = resultServer.getFormItemCountLocally(pro, reaperDay, category, args);

          for (int i = 0; null != itemCount && i < args.length; ++i) {
            if (0 != itemCount[i]) {// 查询到该项有值
              res.add(new KpiData(urlGroup.get(i).getKey(), "", itemCount[i]));
            }
          }
        } else {
          List<String> definedArgs = new ArrayList<String>(); // 独立项
          for (int i = 0; i < args.length; i++) {
            definedArgs.add(args[i]);
          }
          StatisticResultWritable[] definedStatisticResults =
              resultServer.getStatisticResultHdfs(pro, reaperDay, category, definedArgs.toArray(new String[0]));

          LOG.info("definedStatisticResults: " + Arrays.toString(definedStatisticResults));

          for (int i = 0; null != definedStatisticResults && definedStatisticResults.length > 0 && i < args.length; ++i) {
            if (null != definedStatisticResults[i]) {// 查询到该项有值
              switch (urlGroup.get(i).getPvOuv()) {
                case 0:// 返回pv
                  res.add(new KpiData(urlGroup.get(i).getKey(), "", definedStatisticResults[i].getPv()));
                  break;
                case 1:// 返回uv
                  res.add(new KpiData(urlGroup.get(i).getKey(), "", definedStatisticResults[i].getUv()));
                  break;
                default:
                  break;
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.error("getData：Newanalyzer fetch error " + Arrays.toString(args), e);
        throw e;
      }

    }

    return res;
  }


}
