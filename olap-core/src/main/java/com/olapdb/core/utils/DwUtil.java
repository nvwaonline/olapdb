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
        List<String> orderDims = new Vector<>(dims);
        Collections.sort(orderDims);

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
                log.info("field {} {} faild replaced by 0", jsonObject, measure);
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

    private static boolean segCombineTaskIsStillRunning(SegCombineTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "execSegCombineJobIsStillRunning", paras);
    }

    private static boolean executeSegMendTask(SegMendTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cuboid_name", job.getName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "execAddCuboid", paras);
    }

    private static boolean killSegMendTask(SegMendTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cuboid_name", job.getName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "killAddCuboid", paras);
    }

    private static boolean segMendTaskIsStillRunning(SegMendTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cuboid_name", job.getName());
        paras.put("segment_id", job.getSegId()+"");
        if(Util.execRegionOperation(Voxel.class, startRow, "execCuboidAddJobIsStillRunning", paras))
            return true;

        return false;
    }

    private static boolean executeSegBuildTask(SegBuildTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "execSegBuild", paras);
    }
    private static boolean killSegBuildTask(SegBuildTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        return Util.execRegionOperation(Voxel.class, startRow, "killSegBuild", paras);
    }

    private static boolean segBuildTaskIsStillRunning(SegBuildTask job)throws Exception {
        byte[] startRow = Bytez.from(job.getSegId());

        Map<String, String> paras = new HashMap<>();
        paras.put("cube_name", job.getCubeIdenticalName());
        paras.put("segment_id", job.getSegId()+"");
        if(Util.execRegionOperation(Voxel.class, startRow, "execSegBuildJobIsStillRunning", paras))
            return true;

        return false;
    }

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
                    log.info("is it correct? segid = {} cuboid id = {} item count={}", segment.getId(), cuboid.getId(), segment.getVoxelCount(cuboid.getId()));
                }
            }
        }

        return true;
    }

    public static List<String> looseQueryDimentionsManual(Cube cube, List<String> dims){
        Cuboid cuboid = new Cuboid(cube.getIdenticalName() +":"+ DwUtil.unify(dims, cube.getIndexDimensionList()));
        if(!cuboid.needConnect() && cuboid.getPhase() == CuboidPhase.PRODUCTIVE)
            return dims;

        return looseDimentions(cube, dims);
    }


    public static List<String> looseDimentions(Cube cube, List<String> dims){
        Set<String> looseSet = new HashSet<>();
        looseSet.addAll(dims);

        for(List<String> hirDims : cube.getHierachyDimensionList()){
            for(int i=hirDims.size()-1; i>=0; i--){
                if(looseSet.contains(hirDims.get(i))){
                    looseSet.addAll(hirDims.subList(0,i));
                    break;
                }
            }
        }

        for(List<String> jointDims : cube.getJointDimensionList()){
            for(String cdim : jointDims){
                if(looseSet.contains(cdim)){
                    looseSet.addAll(jointDims);
                    break;
                }
            }
        }

        for(String manDims : cube.getMandatoryDimensionList()){
            looseSet.add(manDims);
        }

        return looseSet.stream().collect(Collectors.toList());
    }
}
