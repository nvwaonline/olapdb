package com.olapdb.core.utils;

import com.olapdb.core.OlapOperation;
import com.olapdb.core.config.CuboidPhase;
import com.olapdb.core.config.SegmentPhase;
import com.olapdb.core.tables.*;
import com.olapdb.obase.utils.Obase;
import com.olapdb.obase.utils.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class CubeUtil {
    public static void buidCuboids(Cube cube){
        if(!verifyCube(cube))return;

        List<String> entityDimList = cube.getIndexDimensionList();

        List<String> factors = cube.getDimGroups();

        log.info("分组列表 = {}", factors);

        List<List<String>> combines = CombinationUtil.combiantion(factors.toArray(new String[0]));
        Collections.reverse(combines);
        List<String> valids = new Vector<>();
        int index = 0;
        for (List<String> combine : combines) {
            String dimString = StringUtils.join(combine,":");
            /**
             * 预构建Cuboid中，除基础分组可以包含OLAP_OPERATION维度外，其他维度组合不包含OLAP_OPERATION维度
             */
            List<String> dims = DwUtil.dimStringToSortedList(dimString, entityDimList);
            if(index == 0) {
                //第一个为基础分组，全维度
                valids.add(DwUtil.unify(dims, entityDimList));

                //如果维度包含OLAP_OPERATION选项，增加一个不包含此项的权维度Cuboid
                if(dims.contains(OlapOperation.getKey())){
                    List<String> dimsWithoutOlapOperation = new Vector<>();
                    dimsWithoutOlapOperation.addAll(dims);
                    dimsWithoutOlapOperation.remove(OlapOperation.getKey());
                    valids.add(DwUtil.unify(dimsWithoutOlapOperation, entityDimList));
                }
            }else{
//                dims.remove(Olap.OLAP_OPERATION);
                dims = DwUtil.looseDimentions(cube, dims);
                String unifyString = DwUtil.unify(dims, entityDimList);
                if(!valids.contains(unifyString)){
                    valids.add(unifyString);
                }
            }
            index += 1;
        }

        List<Cuboid> cuboids = new Vector<>();
        long[] ids = Util.getOrderIDs(Obase.getTable(Cuboid.class), Obase.getIdentify()+"_OLAP_cuboidUnifyIdentify-"+ cube.getIdenticalName(), valids.size());
        for (int i = 0; i < valids.size(); i++) {
            cuboids.add(Cuboid.newInstance(cube.getIdenticalName() + ":" + valids.get(i), (int) ids[i]));
        }
        cuboids.forEach(e->e.setPhase(CuboidPhase.PRODUCTIVE));

        //优化所有父子关系
        optimizeInheritedRelation(cube, cuboids);

        Obase.saveAll(cuboids);
    }

    public static String cubeNameFromIdenticalName(String identicalName){
        return identicalName.substring(0, identicalName.indexOf('-'));
    }

    public static void optimizeInheritedRelation(Cube cube, List<Cuboid> cuboids){
        //父子关系有改动的列表;
        final Set<Cuboid> changeSet = new HashSet<>();

        List<Segment> segments = Segment.stream(cube).filter(e->e.inProduce()).collect(Collectors.toList());

        cuboids.forEach(e->{
            Optional<Cuboid> find = findParent(e, cuboids, segments);
            if(find.isPresent() && e.getParent() != find.get().getId()){
                e.setParent(find.get().getId());
                changeSet.add(e);
            }
        });

        Obase.saveAll(new ArrayList<>(changeSet));
    }

    public static Optional<Cuboid> findParent(Cuboid cuboid, List<Cuboid> cuboids, List<Segment> segments){
        return findParent(cuboid, cuboids.stream(), segments);
    }

    public static Optional<Cuboid> findParent(Cube cube, Cuboid cuboid){
        return findParent(cuboid,
                Cuboid.stream(cube.getIdenticalName()).filter(e->e.getPhase() == CuboidPhase.PRODUCTIVE),
                Segment.stream(cube).filter(e->e.inProduce()).collect(Collectors.toList()));
    }

    public static Optional<Cuboid> findParent(Cuboid cuboid, Stream<Cuboid> cuboidStream, List<Segment> segments){
        List<String> subDims = cuboid.getDimList();

        return cuboidStream.filter(e->e.getPhase() == CuboidPhase.PRODUCTIVE)
                .filter(e->e.getDimList().size()>=subDims.size())
                .filter(e->e.getDimList().containsAll(subDims))
                .filter(e->e.getId() != cuboid.getId())
                .min((a, b)->{
                    long aVoxelCount = segments.stream().mapToLong(e->e.getVoxelCount(a.getId())).sum();
                    long bVoxelCount = segments.stream().mapToLong(e->e.getVoxelCount(b.getId())).sum();
                    long ret = aVoxelCount-bVoxelCount;
                    if(ret == 0){
                        ret = a.getDimList().size() - b.getDimList().size();
                        if(ret == 0) {
                            ret = a.getName().compareTo(b.getName());
                        }
                    }
                    return ret>0?1:ret<0?-1:0;
                });
    }

    public static Optional<Cuboid> findParentStrict(Cuboid cuboid, Stream<Cuboid> cuboidStream, List<Segment> segments){
        boolean excludeOlapOperation = !cuboid.getDimList().contains(OlapOperation.getKey());
        return findParent(cuboid, cuboidStream.filter(e->{
            if(excludeOlapOperation && e.getDimList().contains(OlapOperation.getKey()))
                return false;
            return true;
        }), segments);
    }

    public static List<Cuboid> findSons(Cuboid cuboid, Collection<Cuboid> cuboids){
        return cuboids.stream()
                .filter(e->e.getPhase()==CuboidPhase.PRODUCTIVE || e.getPhase()==CuboidPhase.CHARGING)
                .filter(e->e.getParent() == cuboid.getId())
                .collect(Collectors.toList());
    }

    public static Map<String, Voxel> buildCuboid(Cube cube, Cuboid cuboid, Cuboid parent, Segment segment, Stream<Voxel> parentVoxelStream){
        List<String> parentDimList = parent.getDimList();

        log.info("OLAP {} {} - > {} ...", segment.getIdentify(), parent.getName(), cuboid.getName() );
        long startTime = System.currentTimeMillis();

        /**
         * 计算子cuboid 的维度列表
         */
        List<String> sonDimList = cuboid.getDimList();
        /**
         * 计算需留下的列在Parent中的index
         */
        List<Integer> lefts = sonDimList.stream().map(e->parentDimList.indexOf(e)).collect(Collectors.toList());

        //构建 voxels
        Map<String, Voxel> voxelMap = new HashMap<>(100000);
        AtomicInteger atomicCounter = new AtomicInteger(0);

        String cubodingNamePrefix = segment.getId()+":"+ cuboid.getId()+":";

        parentVoxelStream.parallel().forEach(e->{
            String cubodingName = cubodingNamePrefix;
            try {
                cubodingName += e.getDimValueStringWith(lefts);
            }catch (Exception ex){ex.printStackTrace();}
            Voxel voxel = DwUtil.getVoxel(voxelMap, cubodingName);

            voxel.combine(e);

            atomicCounter.incrementAndGet();
        });


        /**
         * 清理掉互相抵消的
         */
        voxelMap.entrySet().removeIf(e->!e.getValue().valid());

        log.info("OLAP {} {} - > {} spent Time = {}  id: {} - > {}",  segment.getIdentify(), atomicCounter.get(), voxelMap.size(), System.currentTimeMillis()-startTime, parent.getId(), cuboid.getId() );

        return voxelMap;
    }

    public static boolean verifyCube(Cube cube){
        return true;
    }

    public static void destroyCube(Cube cube){
        if(cube.getEnable())return;

        /**
         * 清理SegBuildTask
         */
        SegBuildTask.stream(cube).forEach(e->{
            e.delete();
        });

        /**
         * 清理SegCombineTask
         */
        SegCombineTask.stream(cube).forEach(e->{
            e.delete();
        });

        /**
         * 清理CuboidAddTask
         */
        SegMendTask.stream(cube).forEach(e->{
            e.delete();
        });

        /**
         * 清理Segment
         */
        Segment.stream(cube).forEach(e->{
            destroySegment(cube, e);
        });

        /**
         * 清理Cuboid
         */
        while(true) {
            List<Cuboid> cuboids = Cuboid.stream(cube.getIdenticalName()).limit(10000).collect(Collectors.toList());
            if (cuboids.isEmpty())break;
            Obase.deleteAll(cuboids);
        }


        /**
         * 清理Cube
         */
        cube.delete();

        /**
         * 清理Cube对应的计数器
         */
        try {
            Util.clearOrderID(Obase.getTable(Cuboid.class), "cuboidUnifyIdentify-" + cube.getIdenticalName());
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void destroySegment(Cube cube, Segment segment){
        segment.delete();
        DwUtil.compactSegment(segment);
    }

    public static void clearCube(Cube cube){
        if(cube.getEnable())return;

        /**
         * 杀掉正在进行的任务
         */
        SegBuildTask.stream(cube).forEach(e->{
            try {
                DwUtil.killSegTask(e);
            }catch (Exception ex){
                log.error("OLAP Kill Task Failed", ex);
            }
        });

        /**
         * 杀掉正在进行的任务
         */
        SegCombineTask.stream(cube).forEach(e->{
            try {
                DwUtil.killSegTask(e);
            }catch (Exception ex){
                log.error("OLAP Kill Task Failed", ex);
            }
        });

        /**
         * 杀掉正在进行的任务
         */
        SegMendTask.stream(cube).forEach(e->{
            try {
                DwUtil.killSegTask(e);
            }catch (Exception ex){
                log.error("OLAP Kill Task Failed", ex);
            }
        });


        //等待2秒
        try {
            Thread.sleep(2000);
        }catch (Exception e){
            log.error("OLAP Kill Task Failed", e);
        }


        /**
         * 清理SegBuildTask
         */
        SegBuildTask.stream(cube).forEach(e->{
            e.delete();
        });

        /**
         * 清理SegCombineTask
         */
        SegCombineTask.stream(cube).forEach(e->{
            e.delete();
        });

        /**
         * 清理CuboidAddTask
         */
        SegMendTask.stream(cube).forEach(e->{
            e.delete();
        });

        /**
         * 清理Segment
         */
        Segment.stream(cube).forEach(e->{
            e.setPhase(SegmentPhase.ARCHIVE);
        });
    }

}
