package ru.rashid.locker;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;


class ReentrantZKLockerTest {

    private String connectionString = "localhost:2181";
    private String rootPath = "ps";
    private String appName = "myApp";
    private String lockNodeName = "myLock";
    private ReentrantZKLocker zkLocker1;
    private ReentrantZKLocker zkLocker2;

    @BeforeEach
    void setUp() {
        CuratorFramework curatorFramework = createInstance(connectionString, rootPath, appName);
        zkLocker1 = new ReentrantZKLocker(curatorFramework, lockNodeName);
        zkLocker2 = new ReentrantZKLocker(curatorFramework, lockNodeName);
    }


    @Test
    void lock() throws InterruptedException {

        Thread zkLock1Thread = new Thread(() -> {
            while (true) {
                try {
                    TimeUnit.SECONDS.sleep(1);

                    zkLocker1.lock();
                    System.out.println("zkLocker1 locked");
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread zkLock2Thread = new Thread(() -> {
            int i = 0;
            while (true) {
                try {
                    zkLocker2.lock();
                    System.out.println("zkLocker2 locked");
                    TimeUnit.SECONDS.sleep(1);
                    if (++i == 10) {
                        zkLocker2.unlock();
                        System.out.println("zkLocker2 unlocked");
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        zkLock1Thread.start();
        zkLock2Thread.start();
        zkLock1Thread.join();
        zkLock2Thread.join();
    }

    private CuratorFramework createInstance(String connectionString, String rootPath, String appName) {
        return CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .retryPolicy(new ExponentialBackoffRetry(1000, 10))
                .namespace(rootPath + "/" + appName)
                .build();
    }
}