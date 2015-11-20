import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;

/**
 * 处理从xml中获取配制文件
 *
 * @author lujun
 */
public class ReaperUrl {

  public static final String INDEPCOUNT = "indepCount";
  private String pro; // analyzer中的产品名称
  private String cla; // classifier
  private int level; // 数据级次
  private String key; // 关键字
  private List<Seg> seg; // 参数
  private boolean useNew = false;
  private int pvOuv = 0;// 0: pv; 1: uv;

  public int getPvOuv() {
    return pvOuv;
  }

  public void setPvOuv(int pvOuv) {
    this.pvOuv = pvOuv;
  }

  public boolean isUseNew() {
    return useNew;
  }

  public void setUseNew(boolean useNew) {
    this.useNew = useNew;
  }

  public boolean checkIndepCount() {
    if (null != seg && !seg.isEmpty()) {
      for (Seg aSeg : seg) {
        if (INDEPCOUNT.equals(aSeg.getValue())) {
          return true;
        }
      }
    }
    return false;
  }

  public List<Seg> getSegList(String argsStr) {
    List<Seg> list = new ArrayList<Seg>();

    if (StringUtils.isNotBlank(argsStr)) {
      for (String s : argsStr.split(",")) {
        Seg se = new Seg();
        if (s.contains("=")) {
          String[] strs = s.split("=");
          se.setKey(strs[0]);
          se.setValue(strs[1]);
        } else {
          se.setKey(s);
          se.setValue("");
        }

        list.add(se);
      }
    }

    return list;
  }

  /**
   * 解析出用于新analyzer查询的参数
   */
  public String parseArgs() {
    StringBuilder sb = new StringBuilder();
    String lastKey = null;
    for (Seg s : seg) {
      if (StringUtils.isBlank(s.getValue()) || INDEPCOUNT.equals(s.getValue())) {
        lastKey = s.getKey();
      } else {
        sb.append(s.getKey()).append("=").append(s.getValue()).append(",");
      }
    }

    if (null != lastKey) {
      sb.append(lastKey);
    } else if (sb.length() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }

    return sb.toString();
  }

  /**
   * 判断是否是列表数据
   */
  public boolean isList() {
    if (seg.size() > 0 && StringUtils.isBlank(seg.get(seg.size() - 1).getValue())) {
      return true;
    } else {
      return false;
    }
  }

  public String getPro() {
    return pro;
  }

  public void setPro(String pro) {
    this.pro = pro;
  }

  public String getCla() {
    return cla;
  }

  public void setCla(String cla) {
    this.cla = cla;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public List<Seg> getSeg() {
    return seg;
  }

  public void setStrSeg(String s) {
    this.seg = getSegList(s);
  }

  public void setSeg(List<Seg> seg) {
    this.seg = seg;
  }

  @Override
  public String toString() {
    return toJSONString();
  }

  @SuppressWarnings("unchecked")
  public String toJSONString() {
    JSONObject object = new JSONObject();
    object.put("cla", this.cla);
    object.put("pro", this.pro);
    object.put("level", this.level + "");
    object.put("key", this.key);
    object.put("pvOuv", this.pvOuv);
    JSONArray jarry = new JSONArray();
    for (Seg seg : this.seg) {
      JSONObject obj_seg = new JSONObject();
      obj_seg.put("key", seg.getKey());
      obj_seg.put("value", seg.getValue());
      obj_seg.put("regx", seg.getRegx());
      obj_seg.put("mula", seg.getaMul());
      jarry.add(obj_seg);
    }
    object.put("seg", jarry);
    return object.toString();
  }

}
