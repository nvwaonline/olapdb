package com.olapdb.core.workingarea;

import com.olapdb.core.tables.Segment;
import com.olapdb.core.tables.Voxel;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.utils.Obase;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.zookeeper.Watcher;

import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

@Slf4j
public class WorkingAreaManager {
    private static ZkClient zkClient;
    private static TreeMap<Short, WorkingArea> workingAreaMap = new TreeMap<>(Collections.reverseOrder());

    public synchronized static void initZkClient() {
        if(zkClient == null) {
            try {
                String zkServerAddress = Obase.SERVER;
                zkClient = new ZkClient(zkServerAddress, 300000, 30000);

                zkClient.subscribeStateChanges(new IZkStateListener(){
                    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                        if (state == Watcher.Event.KeeperState.Disconnected) {
                            log.error("OLAP ERROR zookeeper disconnected");
                        } else if (state == Watcher.Event.KeeperState.SyncConnected) {
                            log.info("OLAP INFO zookeeper connect success");
                        }
                    }
                    public void handleNewSession() throws Exception {
                        log.info("OLAP INFO zookeeper reconnect success");
                    }

                    @Override
                    public void handleSessionEstablishmentError(Throwable throwable) throws Exception {
                        log.error("OLAP ERROR zookeeper connect failed");
                    }
                });
                if (!zkClient.exists("/olap")) {
                    zkClient.createPersistent("/olap");
                }

                List<HRegionLocation> locations = Obase.getRegionsInRange(Voxel.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false, true);
                initRegions(locations);
            }catch (Throwable e){
                log.error("OLAP ERROR in create zkclient", e);
                System.exit(0);
            }
        }
    }

    private static void initRegions(List<HRegionLocation> locations){
        for (HRegionLocation location : locations){
            byte[] startKey = location.getRegion().getStartKey();
            short sectionId = (startKey==null || startKey.length==0)?0: Bytez.toShort(startKey);
            log.info("sectionId = {} regionName = {}", sectionId, location.getRegion().getRegionNameAsString());

            WorkingArea section = new WorkingArea(zkClient, sectionId);
            workingAreaMap.put(sectionId, section);
        }
    }

    public static int getAreaCount(){
        if(zkClient == null){
            initZkClient();
        }

        return workingAreaMap.size();
    }

    public static long allocateSegmentId()throws Exception{
        while (true) {
            long[] ids = Segment.allocateSegmentIds(7);
            for (long id : ids) {
                if (lockSegment(id))
                    return id;
            }
            log.info("Applu workingarea failed, wait for retry");
            Thread.sleep(10000);
        }
    }

    synchronized public static boolean lockSegment(long segmentId ){
        WorkingArea workingArea = getWorkingAreaBySegmentId(segmentId);
        if(workingArea.isLocked())
            return false;

        return workingArea.lock(segmentId);
    }

    synchronized public static boolean releaseSegment(long segmentId ){
        WorkingArea workingArea = getWorkingAreaBySegmentId(segmentId);
        return workingArea.release(segmentId);
    }

    private static WorkingArea getWorkingAreaBySegmentId(long segId){
        if(zkClient == null){
            initZkClient();
        }

        short find = Bytez.toShort(Bytez.from(segId));
        return workingAreaMap.entrySet().stream().filter(e->e.getKey()<=find).findFirst().get().getValue();
   }
}
