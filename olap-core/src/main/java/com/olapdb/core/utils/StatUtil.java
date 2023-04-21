package com.olapdb.core.utils;

import com.alibaba.fastjson.JSONObject;
import com.olapdb.core.OlapOperation;
import com.olapdb.core.config.CuboidPhase;
import com.olapdb.core.config.SegmentPhase;
import com.olapdb.core.config.TaskPhase;
import com.olapdb.core.tables.*;
import com.olapdb.obase.utils.Util;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

@Slf4j
public class StatUtil {
     public static List<String> compactSegmentDeleteChangeInfo(Cube cube, Segment segment){
        Vector<String> records = new Vector<>();

        long time = System.currentTimeMillis();

        for(int cuboidId : segment.getCuboidIds()) {
            JSONObject data = new JSONObject(true);
            data.put("schema", cube.getSchemaName());
            data.put("cube", cube.getTableName());
            data.put("segment", segment.getId());
            data.put("cuboid", cuboidId);
            data.put("count", segment.getVoxelCount(cuboidId));
            data.put("size", segment.getVoxelSize(cuboidId));
            data.put(OlapOperation.getKey(), OlapOperation.REMOVE.getName());
            data.put("addTime", Util.formatTimeString(time));
            records.add(data.toJSONString());
        }

        return records;
    }


    public static List<String> compactTaskChangeInfo(Cube cube, Segment segment, SegTask task){
        Vector<String> records = new Vector<>();

        long time = System.currentTimeMillis();

        for(int cuboidId : task.getVoxelCountIds()) {
            JSONObject data = new JSONObject(true);
            data.put("schema", cube.getSchemaName());
            data.put("cube", cube.getTableName());
            data.put("segment", segment.getId());
            data.put("cuboid", cuboidId);
            data.put("count", task.getVoxelCount(cuboidId));
            data.put("size", task.getVoxelSize(cuboidId));
            data.put(OlapOperation.getKey(), OlapOperation.ADD.getName());
            data.put("addTime", Util.formatTimeString(time));
            records.add(data.toJSONString());
        }

        return records;
    }

    /**
     * 扫描数据库信息推送系统统计分析模型
     */
    public static JSONObject cubeStatisticInfo(Cube cube,
                                               List<Segment> segments,
                                               List<Cuboid> cuboids,
                                               List<SegBuildTask> segBuildTasks,
                                               List<SegCombineTask> segCombineTasks,
                                               List<SegMendTask> cuboidAddTasks){
        JSONObject data = new JSONObject(true);
        data.put("schema", cube.getSchemaName());
        data.put("cube", cube.getTableName());
        data.put("addTime", formatTimeStringInMinutes(System.currentTimeMillis()));

        data.put("factCount", segments.stream().filter(e->segmentHasValidFactInfo(e)).mapToLong(Segment::getFactCount).sum());
        data.put("voxelCount", segments.stream().mapToLong(Segment::getVoxelCount).sum());
        data.put("voxelSize", segments.stream().mapToLong(Segment::getVoxelSize).sum());

        data.put("segmentCount", segments.size());
        data.put("cuboidCount", cuboids.size());
        data.put("taskCount", segBuildTasks.size()+segCombineTasks.size()+cuboidAddTasks.size());
        data.put("segBuildTaskCount", segBuildTasks.size());
        data.put("segCombineTaskCount", segCombineTasks.size());
        data.put("cuboidAddTaskCount", cuboidAddTasks.size());

        segments.stream().collect(Collectors.groupingBy(Segment::getPhase)).values().forEach(tasks->{
            SegmentPhase phase = tasks.get(0).getPhase();
            data.put("segmentCount_"+ phase.getName() , tasks.size());
        });
        cuboids.stream().collect(Collectors.groupingBy(Cuboid::getPhase)).values().forEach(tasks->{
            CuboidPhase phase = tasks.get(0).getPhase();
            data.put("cuboidCount_"+ phase.getName() , tasks.size());
        });

        segBuildTasks.stream().collect(Collectors.groupingBy(SegTask::getPhase)).values().forEach(tasks->{
            TaskPhase phase = tasks.get(0).getPhase();
            data.put("segBuildTaskCount_"+ phase.getName() , tasks.size());
        });
        segCombineTasks.stream().collect(Collectors.groupingBy(SegTask::getPhase)).values().forEach(tasks->{
            TaskPhase phase = tasks.get(0).getPhase();
            data.put("segCombineTaskCount_"+ phase.getName() , tasks.size());
        });
        cuboidAddTasks.stream().collect(Collectors.groupingBy(SegTask::getPhase)).values().forEach(tasks->{
            TaskPhase phase = tasks.get(0).getPhase();
            data.put("cuboidAddTaskCount_"+ phase.getName() , tasks.size());
        });


        log.info("Cube[{}].stat = {}", cube.getIdenticalName(), data.toJSONString());

        return data;
    }

    private static String formatTimeStringInMinutes(long time){
        time -= time%60000;
        return Util.formatTimeString(time);
    }

    private static boolean segmentHasValidFactInfo(Segment segment){
        switch (segment.getPhase()){
            case PRODUCTIVE:
            case MERGING:
                return true;

            case CREATED:
            case ARCHIVE:
                return false;
        }

        return false;
    }
}
