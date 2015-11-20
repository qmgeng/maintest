/**
 * 读取参数的链接关键字，值
 *
 * @author lujun
 */
public class Seg {

    private String key;
    private String value;
    private Integer mula = new Integer(0);
    private Integer regx = new Integer(0);

    public boolean isMul() {
        return this.mula == 1;
    }

    public Integer getaMul() {
        return mula;
    }

    public void setMula(Integer mul) {
        this.mula = mul;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isRegxx() {
        return this.regx == 1;
    }

    public Integer getRegx() {
        return regx;
    }

    public void setRegx(Integer regx) {
        this.regx = regx;
    }

}
