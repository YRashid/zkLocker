package ru.rashid.locker;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Реализация на колбеках (usingWatcher)
 * Каждый экземпляр класса создает свою последовательную эфемерную ноду в LOCK_NODE, при вызове метода lock.
 * Created by Rashid Iaraliev on 20.06.2018.
 */
@Slf4j
public class ReentrantZKLockerOnCallbacks implements ReentrantZKLocker {

    private static final String SEQUENCE_NODE_PREFIX = "/member_";
    private static final String DELIMITER = "/";
    private final String connectionString;
    private final String rootPath;
    private final String appName;
    private final String LOCK_NODE_PATH;

    private volatile CuratorFramework curatorFramework;
    private String mySequenceNodeName;

    @Getter
    private AtomicBoolean isAcquired = new AtomicBoolean(false);

    public ReentrantZKLockerOnCallbacks(String connectionString, String rootPath, String appName, String lockNodeName) {
        this.connectionString = connectionString;
        this.rootPath = rootPath;
        this.appName = appName;
        this.LOCK_NODE_PATH = DELIMITER + lockNodeName;
    }

    @Override
    public void lock() throws InterruptedException {
        startDoubleCheckedLocking();
        while (!isAcquired.get()) {
            TimeUnit.SECONDS.sleep(1L);
        }
    }

    private void startDoubleCheckedLocking() {
        if (curatorFramework == null) {
            synchronized (this) {
                if (curatorFramework == null) {
                    start();
                }
            }
        }
    }


    /**
     * Встать в очередь на блокировку
     */
    private void start() {
        curatorFramework = createInstance(connectionString, rootPath, appName);
        curatorFramework.start();
        prepareNodes();
        updateIsAcquiredAndWatch();
    }

    /**
     * Удалить текущую эфемерную ноду, если она осталась (достигается рестартом curatorFramework)
     * Встать в очередь на блокировку (создать новую последователдьную эфемерную ноду)
     */
    private void restart() {
        log.error("Restarting ReentrantZKLockerOnCallbacks");
        curatorFramework.close();
        start();
    }

    /**
     * Создать основную ноду LOCK_NODE, если ее еще никто не создал.
     * Создать эфемерную последовательную ноду в LOCK_NODE, соответствующую текущему экземпляру класса.
     * Сохранить в mySequenceNodeName имя созданной эфемерной ноды.
     * <p>
     * Если невозможно создать эфемерную ноду, то пытаемся заново встать в очередь
     */
    private void prepareNodes() {
        try {
            String fullPath = curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(LOCK_NODE_PATH);
            log.info("Successfully created full path node: {}", fullPath);
        } catch (Exception unimportant) {
            log.debug("[unimportant] Error while create persistent node {}. Error: {}", LOCK_NODE_PATH, unimportant.getMessage());
        }

        String path = LOCK_NODE_PATH + SEQUENCE_NODE_PREFIX;
        try {
            String fullPath = curatorFramework.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);
            mySequenceNodeName = fullPath.substring(fullPath.lastIndexOf('/') + 1);
            log.info("Successfully created node: {}", mySequenceNodeName);
        } catch (Exception e) {
            log.error("Error while create ephemeral sequential node: {}. CuratorFramework state: {}. Error: {}", path, curatorFramework.getState(), e);
            restart();
        }
    }

    /**
     * 1. Подписаться на события изменений в эфемерных нодах созданных в LOCK_NODE.
     * После каждого события нужно заново переподписываться
     * 2. Если изменения произошли, то вычислить isAcquired.
     */
    private void updateIsAcquiredAndWatch() {
        try {
            addNewWatcher();
            updateIsAcquired();
        } catch (Exception e) {
            isAcquired.set(false);
            log.error("Error while updating isAcquired. Default isAcquired: {}. Error: {}.", isAcquired, e);
            restart();
        }
    }

    private void updateIsAcquired() throws Exception {
        List<String> children = curatorFramework.getChildren().forPath(LOCK_NODE_PATH);
        isAcquired.set(computeMyPosition(children));
        log.info("Updates in lock node: {}. isAcquired: {}", children, isAcquired);
    }

    /**
     * Вычислить позицию текущего экземпляра в списке эфемерных нод.
     * Сортируем ноды, чтобы на всех экземплярах класса порядок был один.
     *
     * @param children список эфемерных нод созданных в LOCK_NODE
     * @return являемся ли первыми
     */
    private boolean computeMyPosition(List<String> children) {
        children.sort(Comparator.naturalOrder());
        int index = children.indexOf(mySequenceNodeName);
        if (index == -1) {
            throw new RuntimeException("Ephemeral node is expired");
        }
        return index == 0;
    }

    private void addNewWatcher() throws Exception {
        curatorFramework.getChildren().usingWatcher((Watcher) watchedEvent -> updateIsAcquiredAndWatch()).forPath(LOCK_NODE_PATH);
    }

    private CuratorFramework createInstance(String connectionString, String rootPath, String appName) {
        return CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .namespace(rootPath + "/" + appName)
                .build();
    }

    /**
     * todo: curatorFramework.close() + stop watcher
     */
    @Override
    public void unlock() {
        throw new UnsupportedOperationException();
    }


}
