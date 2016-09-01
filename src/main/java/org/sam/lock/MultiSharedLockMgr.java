package org.sam.lock;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * multi share reentrant lock. get several locks at the same time
 * Created by sammylp on 2016/9/1 0001.
 */
public class MultiSharedLockMgr {
    private String clientNamespace;
    private List<String> lockPaths;
    private List<InterProcessLock> locks;
    private InterProcessMultiLock multiLock;

    private static final int LOCK_TIME = 1;//1s
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiSharedLockMgr.class);

    public MultiSharedLockMgr(ApplicationContext context, String clientNamespace, List<String> lockPaths) {
        this.clientNamespace = clientNamespace;
        this.lockPaths = lockPaths;

        ZkConnectionFactoryBean bean = (ZkConnectionFactoryBean) context.getBean("&ZkConnectionFactoryBean");
        CuratorFramework client = bean.getObject();
        init(client, clientNamespace, lockPaths);
    }

    public void init(CuratorFramework client, String clientNamespace, List<String> lockPaths) {
        /*for(String path : lockPaths) {
            InterProcessLock lock = new InterProcessMutex(client, String.format("/%s/%s", clientNamespace, path));
            locks.add(lock);
        }*/

        multiLock = new InterProcessMultiLock(client, lockPaths);
    }

    /**
     * get all locks at the same time.
     * @param time
     * @param unit
     * @throws Exception
     */
    public void tryAcquireLock(long time, TimeUnit unit) throws Exception {
        long tId = Thread.currentThread().getId();
        LOGGER.debug("begin to get lock, pid:" + tId);
        if(!multiLock.acquire(time, unit)) {
            LOGGER.error("fail to get lock " + tId);
            throw new IllegalStateException(String.format("thread:%s,ns:%s,lock path:%s couldn't acquire lock", Thread.currentThread().getId(), clientNamespace, StringUtils.join(lockPaths, "|")));
        }
    }

    public void release() {
        try {
            multiLock.release();
        } catch (Exception e) {
            throw new IllegalStateException(String.format("ns:%s,lock path:%s couldn't release lock", clientNamespace, StringUtils.join(lockPaths, "|")));
        }
    }

    public String getClientNamespace() {
        return clientNamespace;
    }

    public List<String> getLockPath() {
        return lockPaths;
    }


}
