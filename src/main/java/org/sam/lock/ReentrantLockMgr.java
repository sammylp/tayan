package org.sam.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.TimeUnit;

/**
 * Created by sammy on 2016/8/25 0025.
 * reentrant lock factory.
 */
public class ReentrantLockMgr {
    private InterProcessMutex lock;
    private final String clientNamespace;
    private final String lockPath;

    private static final Logger LOGGER = LoggerFactory.getLogger(ReentrantLockMgr.class);

    ZkConnectionFactoryBean zkConnectionFactoryBean;

    private static final int LOCK_TIME = 1;//default is 1s

    public ReentrantLockMgr(ApplicationContext context, String clientNamespace, String lockPath) {
        zkConnectionFactoryBean  = (ZkConnectionFactoryBean) context.getBean("&ZkConnectionFactoryBean");
        CuratorFramework client = zkConnectionFactoryBean.getObject();
        initLock(client, clientNamespace, lockPath);
        this.clientNamespace = clientNamespace;
        this.lockPath = lockPath;
    }

    public ReentrantLockMgr(ZkConnectionFactoryBean zkConnectionBean, String clientNamespace, String lockPath) {
        CuratorFramework client = zkConnectionFactoryBean.getObject();
        initLock(client, clientNamespace, lockPath);
        this.clientNamespace = clientNamespace;
        this.lockPath = lockPath;
    }

    public ReentrantLockMgr(CuratorFramework client, String clientNamespace, String lockPath) {
        initLock(client, clientNamespace, lockPath);
        this.clientNamespace = clientNamespace;
        this.lockPath = lockPath;
    }

    private void initLock(CuratorFramework client, String clientNamespace, String lockPath) {
        lock = new InterProcessMutex(client, String.format("/%s/%s", clientNamespace, lockPath));
    }

    public void tryAcquireLock(long time, TimeUnit unit) throws Exception {
        long tId = Thread.currentThread().getId();
        LOGGER.debug("begin to get lock, pid:" + tId);
        if(!lock.acquire(time, unit)) {
            LOGGER.error("fail to get lock " + tId);
            throw new IllegalStateException(String.format("thread:%s,ns:%s,lock path:%s couldn't acquire lock", Thread.currentThread().getId(), clientNamespace, lockPath));
        }
    }

    /**
     * use default config, lock time is 1s
     * @throws Exception
     */
    public void tryAcquireLock() throws Exception {
        tryAcquireLock(LOCK_TIME, TimeUnit.SECONDS);
    }

    /**
     * must be called after lock is acquired!!!
     * @throws Exception
     */
    public void release() throws Exception{
        try {
            lock.release();
        }catch (Exception e) {
            throw new IllegalStateException(String.format("ns:%s,lock path:%s couldn't release lock", clientNamespace, lockPath));
        }
    }

    public String getClientNamespace() {
        return clientNamespace;
    }

    public String getLockPath() {
        return lockPath;
    }
}
