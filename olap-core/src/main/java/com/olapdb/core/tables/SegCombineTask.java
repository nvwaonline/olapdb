package com.olapdb.core.tables;

import com.olapdb.core.config.SegmentLevel;
import com.olapdb.core.config.SegmentType;
import com.olapdb.core.config.TaskPhase;
import com.olapdb.obase.data.Bytez;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.stream.Stream;

@Slf4j
public class SegCombineTask extends SegTask {
    protected final static String tableName = "olapdb:seg_combine_task";

    public SegCombineTask(byte[] row, boolean autoload) {
        super(row, autoload);
    }

    public SegCombineTask(byte[] row) {
        super(row);
    }

    public SegCombineTask(Result r) {
        super(r);
    }

    public static SegCombineTask newInstance(Segment segment){
        if(segment.getType() != SegmentType.COMBINE){
            throw new IllegalArgumentException("SegCombineTask must generate base on COMBINE type segment");
        }

        SegCombineTask instance = new SegCombineTask(segment.getRow(), false);
        instance.setLevel(segment.getLevel());

        log.info("OLAP {} Created. ", instance.getIdentify());

        return instance;
    }

    public String getIdentify() {
         return "SegCombineTask ["+this.getCubeIdenticalName()+"] ["+ this.getSegId() + "]";
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

    public Segment getSegment(){
        return new Segment(this.getRow());
    }

    @Override
    public void setPhase(TaskPhase value){
        log.info("OLAP {} Phase {} --> {}", this.getIdentify(), this.getPhase().getName(), value.getName());
        super.setPhase(value);
    }

    private void setLevel(SegmentLevel value){
        this.setAttribute("level", Bytez.from(value.getCode()));
    }
    public SegmentLevel getLevel(){
        return SegmentLevel.fromCode(this.getAttributeAsInt("level", 0));
    }

    public void setStartPosition(byte[] value){
        this.setAttribute("startPosition", value);
    }
    public byte[] getStartPosition(){
        return this.getAttribute("startPosition");
    }

    public void setStopPosition(byte[] value){
        this.setAttribute("stopPosition", value);
    }
    public byte[] getStopPosition(){
        return this.getAttribute("stopPosition");
    }

    public void setHStoreFile(String value){
        this.setAttribute("storeFile", Bytez.from(value));
    }
    public String getHStoreFile(){
        return this.getAttributeAsString("storeFile");
    }

    public static Stream<SegCombineTask> stream() {
        return stream(SegCombineTask.class).map(e -> new SegCombineTask(e));
    }
    public static Stream<SegCombineTask> stream(Cube cube) {
        byte[] startRow = Bytez.add(Bytez.from(cube.getIdenticalName() + "|"), Bytez.from(0));
        byte[] stopRow  = Bytez.add(Bytez.from(cube.getIdenticalName() + "|"), Bytez.from(Long.MAX_VALUE));
        Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);

        return stream(SegCombineTask.class, scan).map(e -> new SegCombineTask(e));
    }
}
