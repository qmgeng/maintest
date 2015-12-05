package hbase.page;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * hbase分页查询
 *
 * @author Gavin.peng
 *         <p/>
 *         2013-7-9 下午07:27:21 × dashboard
 */
public class PageScan {

    private String sKey;
    private String eKey;
    private int pageSize;
    private long startTime;
    private Map<Integer, String> pageRowKey;

    public PageScan(String startKey, String endKey, int pageSize) {
        this.sKey = startKey;
        this.eKey = endKey;
        this.pageSize = pageSize;
        this.startTime = System.currentTimeMillis();
    }

    public synchronized void setPageNoStartKey(int pageNo, String startKey) {
        Preconditions.checkArgument(pageNo >= 0, "must be positive %s", pageNo);
        if (pageRowKey == null) {
            pageRowKey = new HashMap<Integer, String>();
        }
        String prePageRowKey = pageRowKey.get(pageNo);
        if (prePageRowKey != null) {
            pageRowKey.remove(pageNo);
        }
        pageRowKey.put(pageNo, startKey);
    }

    public String getPageStartKey(int pageNo) throws Exception {
        if (pageNo <= 1) {
            return this.sKey;
        }
        this.startTime = System.currentTimeMillis();
        if (this.pageRowKey == null) {
            throw new Exception(" pageNo must start with 1 when useHasNext is true ");
        }
        String nextK = this.pageRowKey.get(pageNo);
        if (nextK == null) {
            return this.sKey;
        }
        return nextK;
    }

    public String getKEY() {
        return this.sKey + this.eKey + this.pageSize;
    }

    public String getsKey() {
        return sKey;
    }

    public void setsKey(String sKey) {
        this.sKey = sKey;
    }

    public String geteKey() {
        return eKey;
    }

    public void seteKey(String eKey) {
        this.eKey = eKey;
    }


    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        PageScan pre = (PageScan) obj;
        if (this.sKey.equals(pre.getsKey()) && this.eKey.equals(pre.geteKey())
                && this.pageSize == pre.pageSize)
            return true;
        return false;
    }

    @Override
    public int hashCode() {

        return new StringBuilder(this.sKey).append(this.eKey)
                .append(this.pageSize).toString().hashCode();
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

}
