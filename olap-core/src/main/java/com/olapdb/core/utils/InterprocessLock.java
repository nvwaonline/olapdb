package com.olapdb.core.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

@Slf4j
public class InterprocessLock {
    public static void main(String[] args)  {
        CuratorFramework zkClient = getZkClient();
        String lockPath = "/locktest";
        InterProcessMutex lock = new InterProcessMutex(zkClient, lockPath);
        //模拟50个线程抢锁
        for (int i = 0; i < 10; i++) {
            new Thread(new TestThread(i, lock)).start();
        }
    }


    static class TestThread implements Runnable {
        private Integer threadFlag;
        private InterProcessMutex lock;

        public TestThread(Integer threadFlag, InterProcessMutex lock) {
            this.threadFlag = threadFlag;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                while(true){
                    if(lock.acquire(0, TimeUnit.MILLISECONDS)){
                        System.out.println("第"+threadFlag+"线程获取到了锁");
                        lock.release();
                        Thread.sleep(1000);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static CuratorFramework getZkClient() {
        String zkServerAddress = "pickup02";
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, 5000);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkServerAddress)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        return zkClient;
    }
}
