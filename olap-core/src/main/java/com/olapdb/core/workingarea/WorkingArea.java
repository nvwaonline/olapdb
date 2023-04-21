package com.olapdb.core.workingarea;

import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;

@Slf4j
public class WorkingArea {
    private final static String WorkingAreaPath = "/olap/workingarea";
    private short sectionId;
    private ZookeeperLock lock;
    private boolean isLocked = false;
    private long segmentId;

    WorkingArea(ZkClient zkClient, short sectionId){
        this.sectionId = sectionId;
        this.lock = new ZookeeperLock(zkClient, WorkingAreaPath+sectionId, "section-");
    }

    public boolean isLocked(){
        return isLocked;
    }

    public boolean lock(long segmentId){
        if(isLocked){
            if(this.segmentId == segmentId)
                return true;
            else
                return false;
        }

        try {
            isLocked = lock.lock(segmentId+"");
            if(isLocked) {
                this.segmentId = segmentId;
                log.info("OLAP LOCK segment id = {} get the lock {}", segmentId, this.sectionId);
            }
        }catch (Exception ex){
            log.error("acquire zookeeper lock failed.", ex);
        }

        return isLocked;
    }

    public boolean release(long segmentId){
        if(this.segmentId != segmentId)
            return false;

        try {
            log.info("OLAP LOCK segment id = {} release the lock {}", segmentId, this.sectionId);
            lock.unlock();
            this.segmentId = 0;
            isLocked = false;
            return true;
        }catch (Exception e){
            log.error("OLAP LOCK segment id = {} release the lock {} failed", segmentId, this.sectionId, e);
            return false;
        }
    }
}
