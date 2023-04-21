package com.olapdb.core.workingarea;

import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;

@Slf4j
public class ZkLockTest {
    public static void main(String[] args)  {
        String server = "datalake";
        ZkClient zkClient = new ZkClient(server, 5000, 50000);
        String lockPath = "/olap";
//        ZookeeperLock lock = new ZookeeperLock(zkClient, lockPath, "test");
        //模拟50个线程抢锁
        for (int i = 0; i < 20; i++) {
            new Thread(new TestThread(i, new ZookeeperLock(zkClient, lockPath, "test"))).start();
        }
    }


    static class TestThread implements Runnable {
        private Integer threadFlag;
        private ZookeeperLock lock;

        public TestThread(Integer threadFlag, ZookeeperLock lock) {
            this.threadFlag = threadFlag;
            this.lock = lock;
        }

        @Override
        public void run() {
            try {
                while(true){
                    if(lock.lock("ww")){
                        System.out.println("第"+threadFlag+"线程获取到了锁");
                    }
                    Thread.sleep(1000);
                    lock.unlock();
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
