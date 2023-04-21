package com.olapdb.core.tables;

import com.olapdb.core.config.SegmentLevel;
import com.olapdb.core.config.SegmentPhase;
import com.olapdb.core.config.SegmentType;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.utils.Obase;
import com.olapdb.obase.utils.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Segment extends ArchiveableEntity {
    private static final long serialVersionUID  = 3585878926028121697L;
    private final static String tableName = "olapdb:segment";

    public Segment(byte[] row) {
        super(row);
    }
    public Segment(byte[] row, boolean autoload) {
        super(row, autoload);
    }
    public Segment(Result r) {
        super(r);
    }

    public static Segment newInstance(Cube cube, SegmentLevel segmentLevel){
        long id = allocateSegmentIds(1)[0];
        return newInstance(cube, segmentLevel, id);
    }

    public static Segment newInstance(Cube cube, SegmentLevel segmentLevel, long segmentId){
        byte[] row = Bytez.add(Bytez.from(cube.getIdenticalName()+"|"), Bytez.from(segmentId));
        Segment segment = new Segment(row, false);
        segment.setLevel(segmentLevel);

        log.info("OLAP {} Level = {} Created. ", segment.getIdentify(), segmentLevel.getName());
        return segment;
    }

    public static long[] allocateSegmentIds(int count){
        return Util.getDiscreteIDs(Obase.getTable(Segment.class), Obase.getIdentify()+"_OLAP_segmentUnifyIdentify", count);
    }

    public String getCubeIdenticalName()
    {
        byte[] row = this.getRow();
        return Bytez.toString(row, 0, row.length-9);
    }

    public String getIdentify(){
        return "Segment ["+ this.getCubeIdenticalName() + "] [" + this.getId()+"]";
    }

    public long getId(){
        byte[] row = this.getRow();
        return Bytez.toLong(row, row.length-8);
    }

    public long getOrderId(){
        return Util.reverse(this.getId())>>1;
    }

    private void setLevel(SegmentLevel value){
        this.setAttribute("level", Bytez.from(value.getCode()));
    }
    public SegmentLevel getLevel(){
        return SegmentLevel.fromCode(this.getAttributeAsInt("level", 0));
    }

    public void setType(SegmentType value){
        log.info("OLAP {}  Type {} --> {}", this.getIdentify(), this.getType().getName(), value.getName());
        this.setAttribute("type", Bytez.from(value.getCode()));
    }
    public SegmentType getType(){
        return SegmentType.fromCode(this.getAttributeAsInt("type", 0));
    }

    public void setPhase(SegmentPhase value){
        log.info("OLAP {}  Phase {} --> {}", this.getIdentify(), this.getPhase().getName(), value.getName());
        this.setAttribute("phase", Bytez.from(value.getCode()));
        if(value == SegmentPhase.ARCHIVE){
            this.setArchiveTime(System.currentTimeMillis());
        }
    }
    public SegmentPhase getPhase(){
        return SegmentPhase.fromCode(this.getAttributeAsInt("phase", 0));
    }

    public void setFactCount(long value){
        this.setAttribute("factCount", Bytez.from(value));
    }
    public long getFactCount(){
        return this.getAttributeAsLong("factCount", 0);
    }

    public void setYoungestDataBirthTime(long value){
        this.setAttribute("youngestDataBirthTime", Bytez.from(value));
    }
    public long getYoungestDataBirthTime(){
        return this.getAttributeAsLong("youngestDataBirthTime", 0);
    }

    public void setEldestDataBirthTime(long value){
        this.setAttribute("eldestDataBirthTime", Bytez.from(value));
    }
    public long getEldestDataBirthTime(){
        return this.getAttributeAsLong("eldestDataBirthTime", 0);
    }

    public long getVoxelCount()
    {
        return this.getCuboidIds().stream().mapToLong(e->this.getVoxelCount(e)).sum();
    }
    public void incVoxelCount(int cuboidId, long value){
        this.setVoxelCount(cuboidId, this.getVoxelCount(cuboidId) + value);
    }

    public void setVoxelCount(int cuboidId, long value){
        this.setAttribute("voxelCount", ""+cuboidId,  Bytez.from(value));
    }
    public long getVoxelCount(int cuboidId){
        return this.getAttributeAsLong("voxelCount", ""+cuboidId);
    }

    public long getVoxelSize()
    {
        return this.getCuboidIds().stream().mapToLong(e->this.getVoxelSize(e)).sum();
    }
    public void incVoxelSize(int cuboidId, long value){
        this.setVoxelSize(cuboidId, this.getVoxelSize(cuboidId) + value);
    }

    public void setVoxelSize(long cuboidId, long value){
        this.setAttribute("voxelSize", ""+cuboidId,  Bytez.from(value));
    }
    public long getVoxelSize(long cuboidId){
        return this.getAttributeAsLong("voxelSize", ""+cuboidId);
    }
    public List<Integer> getCuboidIds(){
        return this.getAttributeItems("voxelCount").stream().map(e->Integer.parseInt(e)).collect(Collectors.toList());
    }

    public boolean inProduce(){
        switch (this.getPhase()){
            case PRODUCTIVE:
            case MERGING:
                return true;
        }

        return false;
    }

    public boolean isReparing(){
        return getAttributeItems("reparings").size() > 0;
    }

    public void addReparing(String cuboidName){
        this.setAttribute("reparings", cuboidName, Bytez.from(true));
    }
    public void removeReparing(String cuboidName){
        this.deleteAttribute("reparings", cuboidName);
    }

    public String getSourceSegmentIds(){
        return this.getAttributeAsString("sourceSegmentIds");
    }
    public void setSourceSegmentIds(String value) {
        this.setAttribute("sourceSegmentIds", Bytez.from(value));
    }

    public static Stream<Segment> stream() {
        return stream(Segment.class).map(e -> new Segment(e));
    }
    public static Stream<Segment> rowStream() {
        Scan scan = new Scan();
        FirstKeyOnlyFilter fkof = new FirstKeyOnlyFilter();
        scan.setFilter(fkof);
        return stream(Segment.class, scan).map(e -> new Segment(e));
    }
    public static Stream<Segment> stream(Cube cube) {
        return stream(cube.getIdenticalName());
    }
    public static Stream<Segment> stream(String cubeName) {
        byte[] startRow = Bytez.add(Bytez.from(cubeName + "|"), Bytez.from(0));
        byte[] stopRow  = Bytez.add(Bytez.from(cubeName + "|"), Bytez.from(Long.MAX_VALUE));
        Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);

        return stream(Segment.class, scan).map(e -> new Segment(e));
    }
}
