import java.util.Date;

public class KpiData {

  private String key;
  private Date day;
  private String source;
  private double dbvalue = 0.0;

  public KpiData() {

  }

  public KpiData(String key, double value) {
    this.key = key;
    this.dbvalue = value;
  }

  public KpiData(String key, String source, double value) {
    this.key = key;
    this.source = source;
    this.dbvalue = value;
  }

  @Override
  public String toString() {
    return "key=" + key + ", dbvalue=" + dbvalue;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public long getValue() {
    return (long) (this.dbvalue);
  }

  public Date getDay() {
    return day;
  }

  public void setDay(Date day) {
    this.day = day;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public double getDbvalue() {
    return dbvalue;
  }

  public void setDbvalue(double dbvalue) {
    this.dbvalue = dbvalue;
  }


}
