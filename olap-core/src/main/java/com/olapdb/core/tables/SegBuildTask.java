package com.olapdb.core.tables;

import com.olapdb.core.config.SegmentType;
import com.olapdb.core.config.TaskPhase;
import com.olapdb.obase.data.Bytez;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.stream.Stream;

@Slf4j
public class SegBuildTask extends SegTask {
    protected final static String tableName = "olapdb:seg_build_task";

    public SegBuildTask(byte[] row, boolean autoload) {
        super(row, autoload);
    }

    public SegBuildTask(byte[] row) {
        super(row);
    }

    public SegBuildTask(Result r) {
        super(r);
    }

    public static SegBuildTask newInstance(Segment segment){
        if(segment.getType() != SegmentType.BUILD){
            throw new IllegalArgumentException("SegBuildTask must generate base on BUILD type segment");
        }

        SegBuildTask instance = new SegBuildTask(segment.getRow(), false);

        log.info("OLAP {}  Created. ", instance.getIdentify());

        return instance;
    }

    public Segment getSegment(){
        return new Segment(this.getRow());
    }

    public String getIdentify(){
        return "SegBuildTask ["+ this.getCubeIdenticalName() + "] [" + this.getSegId()+"]";
    }

    public String getCubeIdenticalName()
    {
        byte[] row = this.getRow();
        return Bytez.toString(row, 0, row.length-9);
    }

    public long getSegId(){
        byte[] row = this.getRow();
        return Bytez.toLong(row, row.length-8);
    }

    @Override
    public void setPhase(TaskPhase value){
        log.info("OLAP {}  Phase {} --> {}", this.getIdentify(), this.getPhase().getName(), value.getName());
        super.setPhase(value);
    }


    public static Stream<SegBuildTask> stream() {
        return stream(SegBuildTask.class).map(e -> new SegBuildTask(e));
    }
    public static Stream<SegBuildTask> stream(Cube cube) {
        byte[] startRow = Bytez.add(Bytez.from(cube.getIdenticalName() + "|"), Bytez.from(0));
        byte[] stopRow  = Bytez.add(Bytez.from(cube.getIdenticalName() + "|"), Bytez.from(Long.MAX_VALUE));
        Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);

        return stream(SegBuildTask.class, scan).map(e -> new SegBuildTask(e));
    }
}
