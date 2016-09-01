package org.sam.lock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;

/**
 * Created by sammy on 2016/8/31 0031.
 */
public interface IZkConnectionStateListener {
    void run(CuratorFramework client, ConnectionState state);
}
