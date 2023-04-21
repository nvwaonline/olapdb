package com.olapdb.core.tables;

import com.olapdb.core.config.TaskPhase;
import com.olapdb.core.utils.DwUtil;
import com.olapdb.obase.data.Bytez;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@Slf4j
public class SegMendTask extends SegTask {
    protected final static String tableName = "olapdb:seg_mend_task";

    public SegMendTask(byte[] row, boolean autoload) {
        super(row, autoload);
    }

    public SegMendTask(byte[] row) {
        super(row);
    }

    public SegMendTask(Result r) {
        super(r);
    }

    public static SegMendTask newInstance(Cuboid cuboid, long segid){
        SegMendTask instance = new SegMendTask(Bytez.add(Bytez.from(cuboid.getName()+"|"), Bytez.from(segid)), false);
        instance.setCuboidId(cuboid.getId());

        log.info("OLAP {} Created.", instance.getIdentify());
        return instance;
    }

    public SegMendTask(String name) {
        super(Bytez.from(name));
    }
    public SegMendTask(String cubeIdenticalName, List<String> dims, List<String> entityDims) {
        this(cubeIdenticalName + ":" + DwUtil.unify(dims, entityDims));
    }

    public String getName(){
        return Bytez.toString(this.getRow(), 0, this.getRow().length-9);
    }

    public String getCubeIdenticalName(){
        String name = this.getName();
        return name.substring(0, name.indexOf(":"));
    }

    public long getSegId(){
        return Bytez.toLong(this.getRow(), this.getRow().length-8);
    }

    public String getIdentify() {
        return "CuboidFixTask ["+this.getCubeIdenticalName()+"] ["+ this.getSegId() + "] [" + this.getCuboidId()+"]";
    }

    public Segment getSegment(){
        Optional<Segment> find = Segment.stream(this.getCubeIdenticalName()).filter(e->e.getId() == this.getSegId()).findFirst();
        if(find.isPresent())
            return find.get();
        return null;
    }

    @Override
    public void setPhase(TaskPhase value){
        log.info("OLAP {}  Phase {} --> {}", this.getIdentify(), this.getPhase().getName(), value.getName());
        super.setPhase(value);
    }

    public void setCuboidId(int value){
        this.setAttribute("cuboidId", Bytez.from(value));
    }
    public int getCuboidId(){
        return this.getAttributeAsInt("cuboidId");
    }

    public String getDimensions(){
        String name = this.getName();
        return name.substring(name.indexOf(":")+1);
    }

    public static Stream<SegMendTask> stream() {
        return stream(SegMendTask.class).map(e -> new SegMendTask(e));
    }
    public static Stream<SegMendTask> stream(Cube cube) {
        Scan scan = new Scan().withStartRow(Bytez.from(cube.getIdenticalName()+":")).withStopRow(Bytez.add(Bytez.from(cube.getIdenticalName()+":"), Bytez.from(Integer.MAX_VALUE)) );
        return stream(SegMendTask.class, scan).map(e -> new SegMendTask(e));
    }
    public static Stream<SegMendTask> stream(Cuboid cuboid) {
        Scan scan = new Scan().withStartRow(Bytez.from(cuboid.getName()+"|")).withStopRow(Bytez.add(Bytez.from(cuboid.getName()+"|"), Bytez.from(Integer.MAX_VALUE)) );
        return stream(SegMendTask.class, scan).map(e -> new SegMendTask(e));
    }
}
