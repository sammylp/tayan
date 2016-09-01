package org.sam.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;

/**
 * Created by sammy on 2016/8/31 0031.
 */
public class ZkConnectionLostListener implements  IZkConnectionStateListener {
    @Resource
    ZkConnectionFactoryBean bean;

    private static final Logger LOGGER = LoggerFactory.getLogger(ZkConnectionLostListener.class);

    @Override
    public void run(CuratorFramework client, ConnectionState state) {
        if(state == ConnectionState.LOST) {
            try {
                bean.afterPropertiesSet();
            } catch (Exception e) {
                LOGGER.error("zk connection lost" + ErrorStackTraceHelper.getStackTraceAsString(e));
            }
        }
    }
}
