package org.sam.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.TimeUnit;

/**
 * Created by sammy on 2016/9/1 0001.
 * non-reentrant lock factory.
 */
public class NonReentrantLockMgr {
    private InterProcessSemaphoreMutex lock;
    private final String clientNamespace;
    private final String lockPath;
    ZkConnectionFactoryBean zkConnectionFactoryBean;

    private static final int LOCK_TIME = 1;//default is 1s
    private static final Logger LOGGER = LoggerFactory.getLogger(NonReentrantLockMgr.class);

    public NonReentrantLockMgr(ApplicationContext context, String lockPath, String clientNamespace) {
        zkConnectionFactoryBean  = (ZkConnectionFactoryBean) context.getBean("&ZkConnectionFactoryBean");
        this.lockPath = lockPath;
        this.clientNamespace = clientNamespace;

        CuratorFramework client = zkConnectionFactoryBean.getObject();
        init(client, clientNamespace, lockPath);
    }

    private void init(CuratorFramework client, String clientNS, String lockPath) {
        lock = new InterProcessSemaphoreMutex(client, String.format("/%s/%s", clientNS, lockPath));
    }

    public void tryAcquireLock(long time, TimeUnit unit) throws Exception {
        long tId = Thread.currentThread().getId();
        LOGGER.debug("begin to get lock, pid:" + tId);

        if(!lock.acquire(time, unit)) {
            LOGGER.error("fail to get lock, pid:" + tId);
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
     * 获取锁后一定要调用此方法释放锁！
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
