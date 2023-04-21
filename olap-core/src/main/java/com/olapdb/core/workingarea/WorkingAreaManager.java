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

/**
 * 工作区域管理
 * Segment生成数据写入Region的时候，开启独占模式，防止多个segment生成数据导致数据被污染
 */

@Slf4j
public class WorkingAreaManager {
    private static ZkClient zkClient;
    private static TreeMap<Short, WorkingArea> workingAreaMap = new TreeMap<>(Collections.reverseOrder());

    public synchronized static void initZkClient() {
        if(zkClient == null) {
            //1. 初始化分布式集群锁
            try {
                String zkServerAddress = Obase.SERVER;
                zkClient = new ZkClient(zkServerAddress, 300000, 30000);

                zkClient.subscribeStateChanges(new IZkStateListener(){
                    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                        if (state == Watcher.Event.KeeperState.Disconnected) {
                            log.error("OLAP ERROR zookeeper 断开连接");
                        } else if (state == Watcher.Event.KeeperState.SyncConnected) {
                            log.info("OLAP INFO zookeeper 连接成功");
                        }
                    }
                    public void handleNewSession() throws Exception {
                        log.info("OLAP INFO zookeeper 重新连接成功");
                    }

                    @Override
                    public void handleSessionEstablishmentError(Throwable throwable) throws Exception {
                        log.error("OLAP ERROR zookeeper 连接失败");
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

    /**
     * 根据region信息 创建集群信息
     * @param locations
     */
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

    /**
     * 分配一个新的segment id for write.
     * @return
     * @throws Exception
     */
    public static long allocateSegmentId()throws Exception{
        while (true) {
            long[] ids = Segment.allocateSegmentIds(7);
            for (long id : ids) {
                if (lockSegment(id))
                    return id;
            }
            log.info("获取空闲的工作区域失败，等待重试");
            Thread.sleep(10000);
        }
    }

    /**
     * 尝试独占segment写入
     * @param segmentId
     * @return
     */
    synchronized public static boolean lockSegment(long segmentId ){
        WorkingArea workingArea = getWorkingAreaBySegmentId(segmentId);
        if(workingArea.isLocked())
            return false;

        return workingArea.lock(segmentId);
    }

    /**
     * 释放segment写入
     * @param segmentId
     * @return
     */
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
