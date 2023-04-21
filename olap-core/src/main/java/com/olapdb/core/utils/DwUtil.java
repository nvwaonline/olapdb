package com.olapdb.core.utils;

import com.alibaba.fastjson.JSONObject;
import com.olapdb.core.OlapOperation;
import com.olapdb.core.config.CuboidPhase;
import com.olapdb.core.domain.MultiMeasureStat;
import com.olapdb.core.domain.SingleMeasureStat;
import com.olapdb.core.hll.HLLDistinct;
import com.olapdb.core.tables.*;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.utils.Obase;
import com.olapdb.obase.utils.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DwUtil{
    public static List<String> timeSortDims(List<String> dims, List<String> entityDims){
        /**
         * 对维度序列做时间维度提前按时间粒度排序，其他维度按字符顺序排序
         * 时间维度 OLAP_YEAR,OLAP_MONTH,OLAP_DAY,OLAP_HOUR,OLAP_MINUTE,OLAP_SECOND,OLAP_MILLISECOND是保留维度名称，放在维度组合的最前面
         * @return
         */
        List<String> orderDims = new Vector<>(dims);
        Collections.sort(orderDims);

        /**
         * entityDim提前 这样无时间列、有实体列时能自动转换从可以实体索引的列
         */
        if(entityDims != null && !entityDims.isEmpty()){
            for(int i= entityDims.size()-1; i>=0; i--){
                String entityDim = entityDims.get(i);
                if(orderDims.contains(entityDim)){
                    orderDims.remove(entityDim);
                    orderDims.add(0, entityDim);
                }
            }
        }

        for(int i= TimeUtil.OlapTimeDims.size()-1; i>=0; i--){
            String timeDim = TimeUtil.OlapTimeDims.get(i);
            if(orderDims.contains(timeDim)){
                orderDims.remove(timeDim);
                orderDims.add(0, timeDim);
            }
        }

        return orderDims;
    }

    public static List<String> timeSortDimsWithEntity(List<String> dims, List<String> entityDims, String entityDim){
        List<String> orderDims = timeSortDims(dims, entityDims);
        if(orderDims.contains(entityDim)){
            orderDims.remove(entityDim);
            orderDims.add(0, entityDim);
        }

        return orderDims;
    }


    public static String timeSortDimensionString(String dimString){

        List<String> dims = Arrays.asList(dimString.split(","));
        List<String> orderDims = new ArrayList<>(dims);

        for(int i= TimeUtil.OlapTimeDims.size()-1; i>=0; i--){
            String timeDim = TimeUtil.OlapTimeDims.get(i);
            Optional<String> find = orderDims.stream().filter(e->e.split(":")[0].equals(timeDim)).findFirst();
            if(find.isPresent()){
                orderDims.remove(find.get());
            }
            orderDims.add(0, timeDim+":string");
        }

        return combine(orderDims, ",");
    }

    public static String dimensionsRemoveType(String dimString){
        List<String> dims = Arrays.asList(dimString.split(",")).stream().map(var->var.split(":")[0]).collect(Collectors.toList());
        return StringUtils.join(dims,":");
    }

    public static String unify(List<String> dims, List<String> entityDims){
        return combine(timeSortDims(dims, entityDims), ":");
    }

    public static String combine(List<String> dims, String joint){
        StringBuffer sb = new StringBuffer();
        dims.forEach(e->{
            if(sb.length() > 0)sb.append(joint);
            sb.append(e);
        });

        return sb.toString();
    }

    public static List<String> dimStringToSortedList(String dimString, List<String> entityDims){
        List<String> dims = Arrays.asList(dimString.split(":"));
        return timeSortDims(dims, entityDims);
    }

    public static MultiMeasureStat measureStatFromFact(Cube cube, JSONObject jsonObject){
        List<String> measures = cube.getPureMeasureList();
        MultiMeasureStat multiMeasureStat = new MultiMeasureStat(measures.size());

        if(jsonObject.containsKey(OlapOperation.getKey())&&jsonObject.getString(OlapOperation.getKey()).equals(OlapOperation.REMOVE.getName()))
            multiMeasureStat.setCount(-1);
        else
            multiMeasureStat.setCount(1);

        for(int i=0; i<measures.size(); i++) {
            String measure = measures.get(i);
            SingleMeasureStat singleMeasureStat = new SingleMeasureStat();
            double value;
            try {
                value = jsonObject.getDoubleValue(measure);
            }catch (Exception e){
                log.info("从事实数据【{}】中提取度量字段【{}】失败， 默认使用0代替", jsonObject, measure);
                value = 0;
            }
            singleMeasureStat.setMax((float) value);
            singleMeasureStat.setMin((float) value);
            singleMeasureStat.setSum((float) value*multiMeasureStat.getCount());
            multiMeasureStat.getMeasures()[i] = singleMeasureStat;
        }

        return multiMeasureStat;
    }

    public static HLLDistinct distinctFromFact(Cube cube, JSONObject jsonObject){
        List<String> measures = cube.getMeasureList();

        for(String measure : measures) {
            int pos = measure.indexOf(".");
            if(pos>0){
                String child = measure.substring(pos +1);
                measure = measure.substring(0, pos);
                switch(child){
                    case "distinct":
                        HLLDistinct hllDistinct = new HLLDistinct(14);
                        if(jsonObject.containsKey(measure))
                            hllDistinct.add(jsonObject.get(measure).toString());
                        else
                            hllDistinct.add("null");
                        return hllDistinct;
                    default:
                        continue;
                }
            }
        }

        return null;
    }

    public static Voxel getVoxel(Map<String, Voxel> voxelMap, String voxelName){
        synchronized (voxelMap) {
            Voxel voxel = voxelMap.get(voxelName);
            if (voxel == null) {
                voxel = new Voxel(voxelName, false);
                voxelMap.put(voxelName, voxel);
            }
            return voxel;
        }
    }

    public static int indexOf(String source, String searchStr, int count){
        int pos = 0;
        for(int i=0; i<count; i++){
            if(i >0 ){
                pos += searchStr.length();
            }
            pos = source.indexOf(searchStr, pos);
            if(pos <0 )return pos;
        }

        return pos;

    }


    public static HLLDistinct hllDistinctFrom(byte[] bytes){
        try {
            ByteBuffer in = ByteBuffer.allocate(20000);
            in.put(bytes).flip();
            HLLDistinct hllCounter = new HLLDistinct(14);
            hllCounter.readRegisters(in);
            return hllCounter;
        }catch (Exception e){
            return null;
        }
    }
    public static byte[] hllDistinctTo(HLLDistinct hllDistinct){
        if(hllDistinct == null){
            hllDistinct = new HLLDistinct(14);
        }

        try {
            ByteBuffer out = ByteBuffer.allocate(20000);
            hllDistinct.writeRegisters(out);
            out.flip();
            return Bytez.getBytes(out);
        }catch (Exception e){
            return null;
        }
    }

    public static void flushSegment(Segment segment) {
        try {
            byte[] startRow = Bytez.from(segment.getId());
            byte[] endRow = Bytez.from(segment.getId()+1);
            List<HRegionLocation> locations = Obase.getRegionsInRange(Voxel.class, startRow, endRow, false, false);

            for(HRegionLocation location : locations) {
                Obase.flushRegion(location.getRegion().getRegionName());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //收集集群服务器信息
    public static void execFlushSegment(Segment segment){
        execFlushSegment(segment.getId());
    }

    public static void execFlushSegment(long segId){
        byte[] startRow = Bytez.from(segId);
        byte[] endRow = Bytez.from(segId+1);

        try{
            List<HRegionLocation> locations = Obase.getRegionsInRange(Voxel.class, startRow, endRow, false, false);
            for(HRegionLocation location : locations) {
                Map<String, String> paras = new HashMap<>();
                paras.put("segment_id", segId+"");
                Util.execRegionOperation(Voxel.class, location.getRegion().getStartKey(), "execFlushRegion", paras);
            }
        }catch (Exception e){
            log.info("OLAP execFlushRegion result = {}", e.getMessage());
        }
    }

    //紧缩 segment
    public static void compactSegment(Segment segment) {
        compactSegment(segment.getId());
    }

    public static void compactSegment(long segmentId) {
        try {
            byte[] startRow = Bytez.from(segmentId);
            byte[] endRow = Bytez.from(segmentId+1);
            List<HRegionLocation> locations = Obase.getRegionsInRange(Voxel.class, startRow, endRow, false, false);

            for(HRegionLocation location : locations) {
                Obase.compactRegion(location.getRegion().getRegionName());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static boolean executeSegTask(SegTask job)throws Exception {
        if(job instanceof SegCombineTask){
            return executeSegCombineTask((SegCombineTask)job);
        }

        if(job instanceof SegBuildTask){
            return executeSegBuildTask((SegBuildTask)job);
        }

        if(job instanceof  SegMendTask){
            return executeSegMendTask((SegMendTask)job);
        }

        return false;
    }

    public static boolean killSegTask(SegTask job)throws Exception {
        if(job instanceof  SegCombineTask){
            return killSegCombineTask((SegCombineTask)job);
        }

        if(job instanceof  SegBuildTask){
            return killSegBuildTask((SegBuildTask)job);
        }

        if(job instanceof  SegMendTask){
            return killSegMendTask((SegMendTask)job);
        }

        return false;
    }

    public static boolean segTaskIsStillRunning(SegTask job)throws Exception {
        if(job instanceof  SegCombineTask){
            return segCombineTaskIsStillRunning((SegCombineTask)job);
        }

        if(job instanceof  SegBuildTask){
            return segBuildTaskIsStillRunning((SegBuildTask)job);
        }

        if(job instanceof  SegMendTask){
            return segMendTaskIsStillRunning((SegMendTask)job);
        }

        return false;
    }


    //远程执行分片组合任务
    private static boolean executeSegCombineTask(SegCombineTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "execCombineSegment", paras);
    }
    private static boolean killSegCombineTask(SegCombineTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "killCombineSegment", paras);
    }

    //远程检测分片组合任务是否正在执行
    private static boolean segCombineTaskIsStillRunning(SegCombineTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "execSegCombineJobIsStillRunning", paras);
    }

    //远程执行分片组合任务
    private static boolean executeSegMendTask(SegMendTask job)throws Exception {
        /**
         * 随机选择执行分区
         */
//        byte partid = (byte)(Voxel.PARTION_COUNT * Math.random());

        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cuboid_name", job.getName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "execAddCuboid", paras);
    }

    private static boolean killSegMendTask(SegMendTask job)throws Exception {
        /**
         * 随机选择执行分区
         */
//        byte partid = (byte)(Voxel.PARTION_COUNT * Math.random());

        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cuboid_name", job.getName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "killAddCuboid", paras);
    }

    //远程检测分片构建任务是否正在执行
    private static boolean segMendTaskIsStillRunning(SegMendTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cuboid_name", job.getName());
        paras.put("segment_id", job.getSegId()+"");
        if(Util.execRegionOperation(Voxel.class, startRow, "execCuboidAddJobIsStillRunning", paras))
            return true;

        return false;
    }

    //远程执行分片构建任务
    private static boolean executeSegBuildTask(SegBuildTask job)throws Exception {
        /**
         * 随机选择执行分区
         */
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "execSegBuild", paras);
    }
    private static boolean killSegBuildTask(SegBuildTask job)throws Exception {
        /**
         * 随机选择执行分区
         */
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "killSegBuild", paras);
    }

    //远程检测分片构建任务是否正在执行
    private static boolean segBuildTaskIsStillRunning(SegBuildTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        if(Util.execRegionOperation(Voxel.class, startRow, "execSegBuildJobIsStillRunning", paras))
            return true;

        return false;
    }

    //收集集群服务器信息
    public static String execCollectInfo(HRegionLocation location){
        Map<String, String> paras = new HashMap<>();
        String info = "";
        try{
            Util.execRegionOperation(Voxel.class, location.getRegion().getStartKey(), "execCollectInfo", paras);
        }catch (Exception e){
            info = e.getMessage();
        }

        return info;
    }

    //合并region
    public static boolean checkVoxelRegionEmpty(RegionInfo regionInfo) {
        try {
            Scan scan = new Scan().withStartRow(regionInfo.getStartKey()).withStopRow(regionInfo.getEndKey());
            scan.setCaching(1);
            ResultScanner rs = Obase.getTable(Voxel.class).getScanner(scan);
            Result r = rs.next();

            rs.close();

            if(r == null )
                return true;
            else
                return false;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 扫描一个voxel region中的数据来自哪些 segment
     *
     * @param regionInfo
     * @return
     */
    public static List<Long> scanVoxelRegionSegmentIds(RegionInfo regionInfo) {
        List<Long> segmentIds = new Vector<>();
        try {
            Scan scan = null;
            byte[] startKey = null;
            long segid = 0;
            byte partid = 0;
            while(true){
                if(segmentIds.isEmpty()){
                    scan = new Scan().withStartRow(regionInfo.getStartKey()).withStopRow(regionInfo.getEndKey());
                }else{
                    startKey = Bytez.add(Bytez.from(partid), Bytez.from(segid+1));
                    scan = new Scan().withStartRow(startKey).withStopRow(regionInfo.getEndKey());
                }

                scan.setCaching(1);
                ResultScanner rs = Obase.getTable(Voxel.class).getScanner(scan);
                Result r = rs.next();

                rs.close();

                if(r == null )
                    break;
                else{
                    partid = r.getRow()[0];
                    segid = Bytez.toLong(r.getRow(), 1);
                    segmentIds.add(segid);
                }
            }
            return segmentIds;
        }catch (Exception e){
            log.error("scanVoxelRegionSegmentIds failed." , e);
            return null;
        }
    }

    public static boolean verifyCubeData(String cubeName) {
        List<Segment> segments = Segment.stream(cubeName).filter(e -> e.inProduce()).collect(Collectors.toList());
        List<Cuboid> cuboids = Cuboid.stream(cubeName).filter(e -> e.getPhase() == CuboidPhase.PRODUCTIVE).collect(Collectors.toList());

        for (Segment segment : segments) {
            for (Cuboid cuboid : cuboids) {
                if (segment.getVoxelCount(cuboid.getId()) <= 0) {
                    log.info("为什么会有聚合数为0呢？ segid = {} cuboid id = {} item count={}", segment.getId(), cuboid.getId(), segment.getVoxelCount(cuboid.getId()));
                }
            }
        }

        return true;
    }


    /**
     * 放宽查询要求 要求所有的couples都会成对出现
     * @param cube
     * @param dims
     * @return
     */
    public static List<String> looseQueryDimentionsManual(Cube cube, List<String> dims){
        Cuboid cuboid = new Cuboid(cube.getIdenticalName() +":"+ DwUtil.unify(dims, cube.getIndexDimensionList()));
        if(!cuboid.needConnect() && cuboid.getPhase() == CuboidPhase.PRODUCTIVE)
            return dims;

        return looseDimentions(cube, dims);
    }


    public static List<String> looseDimentions(Cube cube, List<String> dims){
        Set<String> looseSet = new HashSet<>();
        looseSet.addAll(dims);

        /**
         * 层级维度优化
         */
        for(List<String> hirDims : cube.getHierachyDimensionList()){
            for(int i=hirDims.size()-1; i>=0; i--){
                if(looseSet.contains(hirDims.get(i))){
                    looseSet.addAll(hirDims.subList(0,i));
                    break;
                }
            }
        }

        /**
         * 联合维度优化
         */
        for(List<String> jointDims : cube.getJointDimensionList()){
            for(String cdim : jointDims){
                if(looseSet.contains(cdim)){
                    looseSet.addAll(jointDims);
                    break;
                }
            }
        }

        /**
         * 强制维度优化
         */
        for(String manDims : cube.getMandatoryDimensionList()){
            looseSet.add(manDims);
        }

        return looseSet.stream().collect(Collectors.toList());
    }
}
