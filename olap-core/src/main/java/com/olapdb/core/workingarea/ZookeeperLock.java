package com.olapdb.core.workingarea;

import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

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

    public void buildRoot() {
        if (!zkClient.exists(rootPath)) {
            zkClient.createPersistent(rootPath);
        }
    }

    public synchronized void unlock() {
        if (!StringUtils.isEmpty(nodePath)) {
            zkClient.delete(nodePath);

            this.nodePath = "";
        }
    }

    public synchronized boolean lock(String data) {
        nodePath = zkClient.createEphemeralSequential(rootPath + "/" + path, data);

        List<String> list = zkClient.getChildren(rootPath)
                .stream()
                .sorted()
                .map(p -> rootPath + "/" + p)
                .collect(Collectors.toList());
        String firstNodePath = list.get(0);

        log.info("OLAP lock nodePath = {} {}", nodePath, firstNodePath.equals(nodePath));

        if (firstNodePath.equals(nodePath)) {
            return true;
        } else {
            zkClient.delete(nodePath);
            nodePath = "";
            return false;
        }
    }
}
