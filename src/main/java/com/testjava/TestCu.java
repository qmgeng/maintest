package com.testjava;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Created by qmgeng on 15/12/11.
 */
public class TestCu {

    public static void main(String[] args) {
        new TestCu().getX();

    }

    final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("test").build();
    final ExecutorService ayncService = Executors.newFixedThreadPool(1, threadFactory);

    public String getX() {


//        new Thread(new Runnable() {
//            public void run() {
//                try {
//                    Thread.sleep(1000000000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                System.out.println("Mythread 线程");
//            }
//        }).start();

        ayncService.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000000000);
                    System.out.println("aync");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
//        ayncService.shutdown();

        return "111";

    }
}
