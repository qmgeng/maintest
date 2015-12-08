package solr;


import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.solr.client.solrj.SolrServerException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPInputStream;


public class AtmoUpdate {
    String Base_URL;

    AtmoUpdate(String Base_URL) {
        this.Base_URL = Base_URL;
    }

    private static String sendHttpMessage(String url, String method, String contents) {
        try {

            URL serverUrl = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) serverUrl
                    .openConnection();
            conn.setConnectTimeout(20000);

            conn.setRequestMethod(method);// "POST" ,"GET"

            conn.addRequestProperty("Accept", "*/*");
            conn.addRequestProperty("Accept-Language", "zh-cn");
            conn.addRequestProperty("Accept-Encoding", "gzip, deflate");
            conn.addRequestProperty("Cache-Control", "no-cache");
            conn.addRequestProperty("Accept-Charset", "UTF-8");
            conn.addRequestProperty("Content-type", "application/json");

            if (method.equalsIgnoreCase("GET")) {
                conn.connect();
            } else if (method.equalsIgnoreCase("POST")) {

                conn.setDoOutput(true);
                conn.connect();
                conn.getOutputStream().write(contents.getBytes());
            } else {
                throw new RuntimeException("your method is not implement");
            }

            InputStream ins = conn.getInputStream();

            // 处理GZIP压缩的
            if (null != conn.getHeaderField("Content-Encoding")
                    && conn.getHeaderField("Content-Encoding").equals("gzip")) {
                byte[] b = null;
                GZIPInputStream gzip = new GZIPInputStream(ins);
                byte[] buf = new byte[1024];
                int num = -1;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                while ((num = gzip.read(buf, 0, buf.length)) != -1) {
                    baos.write(buf, 0, num);
                }
                b = baos.toByteArray();
                baos.flush();
                baos.close();
                gzip.close();
                ins.close();
                return new String(b, "UTF-8").trim();
            }

            String charset = "UTF-8";
            InputStreamReader inr = new InputStreamReader(ins, charset);
            BufferedReader br = new BufferedReader(inr);

            String line = "";
            StringBuffer sb = new StringBuffer();
            do {
                sb.append(line);
                line = br.readLine();
            } while (line != null);
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }

    }

    public static void main(String[] args) throws SolrServerException, IOException {

        AtmoUpdate atmoUpdate = new AtmoUpdate("http://123.58.179.61:8080/solr");
        //atmoUpdate.AddIndex();
        atmoUpdate.UpdateIndex();

    }

    private String sendHttpMessage(String url, String contents) {
        return sendHttpMessage(url, "POST", contents);
    }

    public void AddIndex() {
        JSONArray content = new JSONArray();
        JSONObject json = new JSONObject();
        try {
            json.put("id", "book1");
            json.put("key", 5333333);
            json.put("editor", "Science Fiction");
        } catch (JSONException e1) {
            e1.printStackTrace();
        }
        content.add(json);
        sendHttpMessage(Base_URL + "/update", content.toString());
    }

    public void UpdateIndex() {
        JSONArray content = new JSONArray();
        JSONObject json = new JSONObject();
        JSONObject set = new JSONObject();
        JSONObject inc = new JSONObject();
        JSONObject add = new JSONObject();

        try {
            set.put("set", "010"); // {"set":"Neal Stephenson"}
            inc.put("inc", "王五"); //{"inc":3}
            add.put("add", "lily"); //{"add":"Cyberpunk"}
            json.put("key", "442161045,00AJ0003|577616,http://ent.163.com/photoview/00AJ0003/577616.html");
            json.put("id", set);
            json.put("editor", inc);
            json.put("editor_3g", add);
            content.add(json);
            System.out.println(content);
            System.out.println(json);
        } catch (final JSONException e) {
        }
        sendHttpMessage(Base_URL + "/update", content.toString());
    }
}

