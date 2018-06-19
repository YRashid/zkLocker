package ru.rashid.locker;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;


class ReentrantZKLockersTest {

    private static final String DELIMITER = "/";
    private String connectionString = "localhost:2181";
    private String rootPath = "ps";
    private String appName = "myApp";
    private String lockNodeName = "myLock";
    private ReentrantZKLocker zkLocker1;
    private ReentrantZKLocker zkLocker2;
    private CuratorFramework curatorFramework;


    @BeforeEach
    void setUp() {
        curatorFramework = createInstance(connectionString, rootPath, appName);
        curatorFramework.start();
    }


    @Test
    void lockerOnRepeatingCallsTest() throws InterruptedException {
        zkLocker1 = new ReentrantZKLockerOnRepeatingCalls(curatorFramework, lockNodeName);
        zkLocker2 = new ReentrantZKLockerOnRepeatingCalls(curatorFramework, lockNodeName);
        endlessTest();
    }


    @Test
    void lockerOnCallbacksTest() throws InterruptedException {
        zkLocker1 = new ReentrantZKLockerOnCallbacks(connectionString, rootPath, appName, lockNodeName);
        zkLocker2 = new ReentrantZKLockerOnCallbacks(connectionString, rootPath, appName, lockNodeName);
        endlessTest();
    }

    private void endlessTest() throws InterruptedException {
        Thread zkLock1Thread = new Thread(() -> {
            while (true) {
                try {
                    zkLocker1.lock();
                    System.out.println("zkLocker1 acquiring lock");
                    TimeUnit.SECONDS.sleep(1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        Thread zkLock2Thread = new Thread(() -> {
            while (true) {
                try {
                    zkLocker2.lock();
                    System.out.println("zkLocker2 acquiring lock");
                    TimeUnit.SECONDS.sleep(1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        Thread remover = new Thread(() -> {
            while (true) {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    List<String> children = curatorFramework.getChildren().forPath(DELIMITER + lockNodeName);
                    children.sort(Comparator.naturalOrder());
                    String firstNodeName = children.get(0);
                    curatorFramework.delete().forPath(DELIMITER + lockNodeName + DELIMITER + firstNodeName);
                    System.out.println("Removed node: " + DELIMITER + lockNodeName + DELIMITER + firstNodeName);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        zkLock1Thread.start();
        zkLock2Thread.start();
        remover.start();
        zkLock1Thread.join();
        zkLock2Thread.join();
        remover.join();
    }

    private CuratorFramework createInstance(String connectionString, String rootPath, String appName) {
        return CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .retryPolicy(new ExponentialBackoffRetry(1000, 10))
                .namespace(rootPath + "/" + appName)
                .build();
    }
}