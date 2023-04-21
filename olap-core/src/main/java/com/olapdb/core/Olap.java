package com.olapdb.core;

import com.alibaba.fastjson.JSONObject;
import com.olapdb.core.config.*;
import com.olapdb.core.hll.HLLDistinct;
import com.olapdb.core.tables.*;
import com.olapdb.core.utils.DwUtil;
import com.olapdb.core.utils.TimeUtil;
import com.olapdb.core.workingarea.WorkingAreaManager;
import com.olapdb.obase.data.Entity;
import com.olapdb.obase.utils.Obase;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class Olap {
    private final static long serialVersionUID  = 0x01B21DF2138E4000L;

    /**
     * submit precompiled segment to olap database
     * @param cubeName, cube name
     * @param facts, fact list
     * @throws Exception
     */
    public static void submitSegment(String cubeName, List<String> facts) throws Exception {
        Cube cube = new Cube(cubeName);
        if (cube.needConnect()) {
            log.info("Cube with name {} not exist.", cubeName);
            throw new Exception("Cube not exist.");
        }
        if (!cube.getEnable()) {
            log.info("Cube with name {} is disable.", cubeName);
            throw new Exception("Cube is disable");
        }

        ///////////////////////////////////////////////////////////////////////////////////////////
        //如果指定时间维度，把时间维度用层级时间维度组替换
        ///////////////////////////////////////////////////////////////////////////////////////////
        String timeDim = cube.getTimeDimension();
        String timeDimType = timeDim.isEmpty()?null:cube.getDimensionType(timeDim);
        AtomicLong youngestDataTime = new AtomicLong(0);
        AtomicLong eldestDataTime = new AtomicLong(0);
        ///////////////////////////////////////////////////////////////////////////////////////////


        if (facts == null || facts.isEmpty()) {
            log.info("Table {} facts is empty", cubeName);
            throw new Exception("facts is empty");
        }

        log.info("CubeName= [{}] start prebuild", cube.getIdenticalName());

        Segment segment = null;
        SegBuildTask job = null;

        long segId = WorkingAreaManager.allocateSegmentId();

        try{
            DwUtil.execFlushSegment(segId);

            segment = Segment.newInstance(cube, SegmentLevel.LEVEL_0, segId);
            segment.setType(SegmentType.BUILD);
            segment.setPhase(SegmentPhase.CREATED);
            segment.setFactCount(facts.size());
            segment.connect();

            job = SegBuildTask.newInstance(segment);
            job.setPhase(TaskPhase.CREATED);
            job.setStartTime(System.currentTimeMillis());
            job.connect();

            List<String> sortedDimList = cube.getDimensionList();

            String cuboidName = cube.getIdenticalName() + ":" + DwUtil.unify(sortedDimList, cube.getIndexDimensionList());
            Cuboid cuboid = Cuboid.stream(cube.getIdenticalName())
                    .filter(e -> e.getPhase() == CuboidPhase.CHARGING || e.getPhase() == CuboidPhase.PRODUCTIVE)
                    .filter(e -> e.getName().equals(cuboidName))
                    .findFirst()
                    .get();
            if (cuboid == null) {
                log.info("Cube [{}] prebuild failed.", cube.getIdenticalName());
                throw new Exception("Cuboid " + cuboidName + " not found.");
            }

            try {
                job.setHostName(InetAddress.getLocalHost().getHostName());
            } catch (Exception e) {
            }

            Map<String, Voxel> voxelMap = new HashMap<>();
            long startTime = System.currentTimeMillis();
            final AtomicLong factCounter = new AtomicLong(0);

            final long segid = segment.getId();
            String cubodingNamePrefix = segid + ":" + cuboid.getId() + ":";

            AtomicReference<Exception> exceptionReference = new AtomicReference<>();
            Optional<String> find = facts.stream().parallel().filter(e -> {
                try {
                    JSONObject jo = JSONObject.parseObject(e);

                    if(!timeDim.isEmpty()){
                        TimeUtil.replaceTimeDimensionToTimeHierachy(jo, timeDim,timeDimType, youngestDataTime, eldestDataTime);
                    }

                    List<Object> jsonArray = new ArrayList<>(sortedDimList.size() +1);
                    for (String dim : sortedDimList) {
                        if (OlapOperation.getKey().equals(dim) && !jo.containsKey(OlapOperation.getKey())) {
                            jsonArray.add(OlapOperation.ADD.getName());
                        } else {
                            jsonArray.add(jo.get(dim));
                        }
                    }

                    String cubodingName = cubodingNamePrefix;
                    cubodingName += Obase.mapper.writeValueAsString(jsonArray);
                    Voxel voxel = DwUtil.getVoxel(voxelMap, cubodingName);

                    //add measure stat
                    voxel.addStat(DwUtil.measureStatFromFact(cube, jo));

                    //add distinct
                    HLLDistinct distinct = DwUtil.distinctFromFact(cube, jo);
                    if (distinct != null) {
                        voxel.combineDistinct(distinct);
                    }

                    if (factCounter.incrementAndGet() % 10000 == 0) {
                        log.info("Cube [{}] process fact: {}", cube.getIdenticalName(), factCounter.get());
                    }
                    return false;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    exceptionReference.set(ex);
                    return true;
                }
            }).findFirst();

            if(find.isPresent()){
                log.error("data process failed {}", find.get(), exceptionReference.get());
                throw new Exception("data process failed", exceptionReference.get());
            }

            voxelMap.entrySet().removeIf(e->!e.getValue().valid());

            if(voxelMap.isEmpty())throw new Exception("Build failed. voxelMap is Empty.");

            Obase.saveAll(voxelMap.values().stream().filter(e -> e.valid()).collect(Collectors.toList()), Durability.USE_DEFAULT);
            job.setVoxelCount(cuboid.getId(), voxelMap.size());
            job.setVoxelSize(cuboid.getId(), voxelMap.values().stream().mapToLong(Voxel::size).sum());

            if(youngestDataTime.get() != 0)
                segment.setYoungestDataBirthTime(youngestDataTime.get());
            else
                segment.setYoungestDataBirthTime(System.currentTimeMillis());
            if(eldestDataTime.get() != 0)
                segment.setEldestDataBirthTime(eldestDataTime.get());
            else
                segment.setEldestDataBirthTime(System.currentTimeMillis());

            DwUtil.execFlushSegment(segment);
            job.setPhase(TaskPhase.PREPARED);

            log.info("Cube [{}] prebuild task complete factCount = {} voxelCounter = {} spent time = {}", cube.getIdenticalName(), facts.size(), voxelMap.size(), System.currentTimeMillis() - startTime);
            voxelMap.clear();
        } catch (Exception ex) {
            if (job != null) {
                job.setPhase(TaskPhase.FAILED);
                job.setHint(ex.toString());
            }

            throw ex;
        }finally {
            WorkingAreaManager.releaseSegment(segId);
        }
    }

    /**
     * submit facts record to olap. if the size of facts is small, use this method will reduce the mount of segments,
     * and relieve the pressure of master server.
     * @param factName, fact name
     * @param facts, fact list
     */
    public static void submitFacts(String factName, List<String> facts){
        List<Fact> factList = facts.stream().map(e->{
            Fact fact = Fact.newInstance(factName);
            fact.setContent(e);
            return fact;
        }).collect(Collectors.toList());
        Obase.saveAll(factList);
    }

    /**
     * delete facts from fact table
     * @param factName, fact name
     */
    public static void deleteFacts(String factName){
        log.info("delete FACT data {}.......................", factName);
        AtomicLong counter = new AtomicLong(0);
        //清空原有Fact
        List<Fact> deleteSet = new Vector<>();
        Fact.stream(factName, 0, Long.MAX_VALUE).forEach(e -> {
            deleteSet.add(e);
            if (deleteSet.size() >= 100000) {
                counter.addAndGet(deleteSet.size());
                Obase.deleteAll(deleteSet);
                deleteSet.clear();
                log.info("delete FACT data {}, finished {}", factName, counter.get());
            }
        });
        if (!deleteSet.isEmpty()) {
            counter.addAndGet(deleteSet.size());
            Obase.deleteAll(deleteSet);
        }
        log.info("delete FACT data {} finished. total items: {}", factName, counter.get());

        try{
            thrinkFactTable();
        }catch (Exception e){}
    }

    private static void thrinkFactTable() throws Exception {
        List<HRegionLocation> regionLocations = Obase.getRegionsInRange(Fact.class, null, HConstants.EMPTY_END_ROW, false,false);

        if(regionLocations.size() <= 64)return;

        AtomicLong atomicCounter = new AtomicLong(0);
        List<byte[]> emptyRegions = new Vector<>();
        regionLocations.stream().forEach(e->{
            try {
                log.info("Region[{}][{}][{}]", atomicCounter.incrementAndGet(), regionIsEmpty(e), e.getRegion().getRegionNameAsString());
                if (regionIsEmpty(e)) {
                    emptyRegions.add(e.getRegion().getEncodedNameAsBytes());
                    if (emptyRegions.size() >= 2){
                        log.info("combine [{}] regions to one.", emptyRegions.size());
                        Obase.mergeRegion(emptyRegions.toArray(new byte[0][0]));
                        emptyRegions.clear();
                    }
                } else {
                    emptyRegions.clear();
                }
            }catch(Exception ex){
                ex.printStackTrace();
            }
        });
    }

    private static boolean regionIsEmpty(HRegionLocation regionLocation){
        RegionInfo regionInfo = regionLocation.getRegion();
        Scan scan  =new Scan().withStartRow(regionInfo.getStartKey()).withStopRow(regionInfo.getEndKey());
        scan.setCacheBlocks(false);
        scan.setCaching(10);
        Long count = Entity.stream(Fact.class, scan).limit(1).count();
        if(count == 0)
            return true;

        return false;
    }
}
