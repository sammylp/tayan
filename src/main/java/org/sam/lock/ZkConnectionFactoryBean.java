package org.sam.lock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.List;

/**
 * Created by sammy on 2016/8/31 0031.
 */
public class ZkConnectionFactoryBean implements FactoryBean<CuratorFramework>, InitializingBean, DisposableBean {

    private volatile CuratorFramework client;
    private List<IZkConnectionStateListener> listeners;
    private String zkConnAddr;

    private static final int CONNECTION_TIMEOUT = 2000;//2s
    private static final int SESSION_TIMEOUT = 30 * 1000;

    @Override
    public void destroy() throws Exception {
        client.close();
    }

    @Override
    public CuratorFramework getObject() {
        return client;
    }

    @Override
    public Class<?> getObjectType() {
        return CuratorFramework.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    /**
     * mainly for zk connection creation
     *
     * @throws Exception
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        RetryPolicy policy = new ExponentialBackoffRetry(200, 5);
        client = CuratorFrameworkFactory.builder().connectString(zkConnAddr)
                .retryPolicy(policy)
                .connectionTimeoutMs(CONNECTION_TIMEOUT)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .build();
        addListener();
        client.start();
    }

    private void addListener() {
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                for (IZkConnectionStateListener listener : listeners) {
                    listener.run(client, connectionState);
                }
            }
        });
    }

    public String getZkConnAddr() {
        return zkConnAddr;
    }

    public void setZkConnAddr(String zkConnAddr) {
        this.zkConnAddr = zkConnAddr;
    }

    public List<IZkConnectionStateListener> getListeners() {
        return listeners;
    }

    public void setListeners(List<IZkConnectionStateListener> listeners) {
        this.listeners = listeners;
    }
}
