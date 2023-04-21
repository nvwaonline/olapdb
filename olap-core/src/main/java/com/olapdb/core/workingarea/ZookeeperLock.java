package com.olapdb.core.workingarea;

import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 邱润泽
 **/
@Slf4j
public class ZookeeperLock {
    private ZkClient zkClient;
    private String rootPath;
    private String nodePath;
    private String path;

    public ZookeeperLock(ZkClient zkClient, String rootPath, String path) {
        this.zkClient = zkClient;
        this.rootPath = rootPath;
        this.path = path;
        buildRoot();
    }

    // 构建根节点
    public void buildRoot() {
        if (!zkClient.exists(rootPath)) {
            zkClient.createPersistent(rootPath);
        }
    }

    // 释放锁
    public synchronized void unlock() {
        if (!StringUtils.isEmpty(nodePath)) {
            zkClient.delete(nodePath);

//            List<String> list = zkClient.getChildren(rootPath)
//                    .stream()
//                    .sorted()
//                    .map(p -> rootPath + "/" + p)
//                    .collect(Collectors.toList());      // 判断当前是否为最小节点
//            log.info("OLAP unlock nodePath = {}  allPath = {}", nodePath, list);

            this.nodePath = "";
        }
    }

    // 尝试激活锁
    public synchronized boolean lock(String data) {
        nodePath = zkClient.createEphemeralSequential(rootPath + "/" + path, data);

        // 获取根节点下面所有的子节点
        List<String> list = zkClient.getChildren(rootPath)
                .stream()
                .sorted()
                .map(p -> rootPath + "/" + p)
                .collect(Collectors.toList());      // 判断当前是否为最小节点
        String firstNodePath = list.get(0);

        log.info("OLAP lock nodePath = {} {}", nodePath, firstNodePath.equals(nodePath));

        // 最小节点是不是当前节点
        if (firstNodePath.equals(nodePath)) {
            return true;
        } else {
            zkClient.delete(nodePath);
            nodePath = "";
            return false;
        }
    }
}
