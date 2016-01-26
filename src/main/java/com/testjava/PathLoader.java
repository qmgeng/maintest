package com.testjava;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;

/**
 * Created by qmgeng on 15/12/5.
 */
public class PathLoader {
    public static void main(String[] args) throws UnsupportedEncodingException {
        System.out.println(getProjectPath());
        System.out.println(getRealPath());
    }

    public static String getProjectPath() throws UnsupportedEncodingException {
        URL url = PathLoader.class.getProtectionDomain().getCodeSource().getLocation();
        String filePath = URLDecoder.decode(url.getPath(), "utf-8");
        if (filePath.endsWith(".jar")) {
            filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
        }
        File file = new File(filePath);
        filePath = file.getAbsolutePath();
        return filePath;
    }

    public static String getRealPath() {
        String realPath = PathLoader.class.getClassLoader().getResource("").getFile();
        File file = new File(realPath);
        realPath = file.getAbsolutePath();
        try {
            realPath = URLDecoder.decode(realPath, "utf-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return realPath;
    }
}
