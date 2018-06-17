package ru.rashid.locker;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Реализация на вызовах getChildren
 * Блокировка не между потоками, а между экземплярами класса ReentrantZKLocker.
 * Каждый экземпляр класса создает свою последовательную эфемерную ноду в LOCK_NODE.
 * <p>
 * Захват блокировки определяется позицией экземпляра созданной последовательной ноды в списке всех эфемерных нод,
 * * отсортированных в лексикографическом порядке.
 * <p>
 * Если экземпляр приложения перестал работать, то эфемерная нода пропадает и количество экземпляров создавших свои ноды
 * * в LOCK_NODE уменьшается
 * Created by Rashid Iaraliev on 11.06.2018.
 */
@Slf4j
public class ReentrantZKLocker {

    private static final String SEQUENCE_NODE_PREFIX = "/member_";
    private static final String DELIMITER = "/";
    private final String LOCK_NODE_PATH;
    private final CuratorFramework curatorFramework;
    private volatile String mySequenceNodeName;

    public ReentrantZKLocker(CuratorFramework curatorFramework, String lockNodeName) {
        LOCK_NODE_PATH = DELIMITER + lockNodeName;
        this.curatorFramework = curatorFramework;
        if (curatorFramework.getState().equals(CuratorFrameworkState.LATENT)) {
            curatorFramework.start();
        }
    }

    public void lock() throws InterruptedException {
        while (!checkIsAcquired()) {
            TimeUnit.SECONDS.sleep(1L);
        }
    }

    /**
     * Проверить захвачена ли блокировка
     */
    private boolean checkIsAcquired() {
        prepareNodesDoubleCheckedLocking();
        try {
            List<String> children = curatorFramework.getChildren().forPath(LOCK_NODE_PATH);
            children.sort(Comparator.naturalOrder());
            int index = children.indexOf(mySequenceNodeName);
            if (index == -1) {
                mySequenceNodeName = null;
            }
            return index == 0;
        } catch (Exception e) {
            log.error("Error while updating position: {}.", e);
            deleteMySequenceNode();
            return false;
        }
    }

    private void prepareNodesDoubleCheckedLocking() {
        if (mySequenceNodeName == null) {
            synchronized (this) {
                if (mySequenceNodeName == null) {
                    prepareNodes();
                }
            }
        }
    }

    /**
     * Создать основную ноду LOCK_NODE, если ее еще никто не создал.
     * Создать эфемерную последовательную ноду в LOCK_NODE, соответствующую текущему экземпляру класса.
     * Сохранить в mySequenceNodeName имя созданной эфемерной ноды.
     * <p>
     * Exception: Если невозможно создать эфемерную ноду, то приложение не должно работать.
     */
    private void prepareNodes() {
        try {
            String fullPath = curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(LOCK_NODE_PATH);
            log.info("Successfully created full path node: {}", fullPath);
        } catch (Exception unimportant) {
            log.debug("Error while create persistent node {}. Error: {}", LOCK_NODE_PATH, unimportant);
        }

        String path = LOCK_NODE_PATH + SEQUENCE_NODE_PREFIX;
        try {
            String fullPath = curatorFramework.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);
            mySequenceNodeName = fullPath.substring(fullPath.lastIndexOf('/') + 1);
            log.info("Successfully created node: {}", mySequenceNodeName);
        } catch (Exception e) {
            log.error("Error while create ephemeral sequential node: {}. CuratorFramework state: {}. Error: {}", path, curatorFramework.getState(), e);
            throw new RuntimeException(e);
            //todo: Здесь нужно найти по UUID свою ноду и проверить создалась ли она
        }
    }

    private void deleteMySequenceNode() {
        while (true) {
            try {
                curatorFramework.delete().forPath(LOCK_NODE_PATH + DELIMITER + mySequenceNodeName);
                break;
            } catch (KeeperException.NoNodeException success) {
                break;
            } catch (Exception unimportant) {
                log.error("Error while deleting node: {}", unimportant);
            }
        }
        mySequenceNodeName = null;
    }

    public void unlock() {
        deleteMySequenceNode();
    }

}