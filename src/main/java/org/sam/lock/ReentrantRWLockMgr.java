package org.sam.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.TimeUnit;

/**
 * Created by sammylp on 2016/9/1 0001.
 */
public class ReentrantRWLockMgr {
    private InterProcessReadWriteLock lock;
    private InterProcessMutex rLock;
    private InterProcessMutex wLock;

    private String clientNamespace;
    private String lockPath;

    private static final int LOCK_TIME = 1;//default is 1s
    private static final Logger LOGGER = LoggerFactory.getLogger(ReentrantRWLockMgr.class);

    public ReentrantRWLockMgr(ApplicationContext context, String clientNamespace, String lockPath) {
        this.clientNamespace = clientNamespace;
        this.lockPath = lockPath;

        ZkConnectionFactoryBean bean = (ZkConnectionFactoryBean) context.getBean("&ZkConnectionFactoryBean");
        CuratorFramework client = bean.getObject();

    }

    public void init(CuratorFramework client, String clientNamespace, String lockPath) {
        lock = new InterProcessReadWriteLock(client, String.format("/%s/%s", clientNamespace, lockPath));
        rLock = lock.readLock();
        wLock = lock.writeLock();
    }

    /**
     * get the lock ,default is get write lock
     * @param time
     * @param unit
     * @throws Exception
     */
    public void tryAcquireLock(long time, TimeUnit unit) throws Exception {
        long tId = Thread.currentThread().getId();
        LOGGER.debug("begin to get lock, pid:" + tId);
        if(!wLock.acquire(time, unit)) {
            LOGGER.error("fail to get lock:" + tId);
            throw new IllegalStateException(String.format("thread:%s,ns:%s,lock path:%s couldn't acquire lock", Thread.currentThread().getId(), clientNamespace, lockPath));
        }
    }

    /**
     * try to get lock ,default get write lock and time out limit is 1s
     * @throws Exception
     */
    public void tryAcquireLock() throws Exception {
        tryAcquireLock(LOCK_TIME, TimeUnit.SECONDS);
    }

    public void tryAcquireWriteLock(long time, TimeUnit unit) throws Exception {
        tryAcquireLock(time, unit);
    }

    public void tryAcquireReadLock(long time, TimeUnit unit) throws Exception {
        long tId = Thread.currentThread().getId();
        LOGGER.debug("begin to get lock, pid:" + tId);
        if(!rLock.acquire(time, unit)) {
            LOGGER.error("fail to get lock " + tId);
            throw new IllegalStateException(String.format("thread:%s,ns:%s,lock path:%s couldn't acquire lock", Thread.currentThread().getId(), clientNamespace, lockPath));
        }
    }

    /**
     * must be called after lock is acquired!!!
     * @throws Exception
     */
    public void release() throws Exception {
        try {
            if(wLock.isAcquiredInThisProcess())
                wLock.release();
            if(rLock.isAcquiredInThisProcess())
                rLock.release();
        }catch (Exception e) {
            throw new IllegalStateException(String.format("ns:%s,lock path:%s couldn't release lock", clientNamespace, lockPath));
        }
    }
}
