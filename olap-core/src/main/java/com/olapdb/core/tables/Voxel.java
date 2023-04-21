package com.olapdb.core.tables;

import com.olapdb.core.domain.MultiMeasureStat;
import com.olapdb.core.domain.SingleMeasureStat;
import com.olapdb.core.hll.HLLDistinct;
import com.olapdb.core.utils.DwUtil;
import com.olapdb.core.utils.json.LazyArray;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.Entity;
import com.olapdb.obase.utils.Obase;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class Voxel extends Entity {
    private final static long serialVersionUID  = 8080241343223960771L;

    public static boolean USE_LAZY_ARRAY_FOR_JSON = true;
    private final static String tableName = "olapdb:voxel";

    public Voxel(byte[] row) {
        super(row);
    }
    public Voxel(String voxelName, boolean autoload) {
        super(rowkeyFromVoxelName(voxelName), autoload);
    }
    public Voxel(long segId, byte[] indicator, boolean autoload) {
        super(Bytez.add(Bytez.from(segId),indicator), autoload);
    }
    public Voxel(Result r) {
        super(r);
    }

    @Override
    public Put collect() {
        return super.collect().setDurability(Durability.SKIP_WAL);
    }

    public String getName(){
        byte[] row = this.getRow();
        return Bytez.toLong(row)+":"+Bytez.toInt(row, 8)+":"+Bytez.toString(row, 12);
    }

    private static byte[] rowkeyFromVoxelName(String voxelName){
        String sep = ":";
        int pos0 = voxelName.indexOf(sep);

        if(pos0 <0){
            return Bytez.from(voxelName);
        }
        int pos1 = voxelName.indexOf(sep,pos0+1);

        long segmentId = Long.parseLong(voxelName.substring(0,pos0));
        int cuboidId = Integer.parseInt(voxelName.substring(pos0+1,pos1));

        return rowkeyFromVoxelName(segmentId,cuboidId, voxelName.substring(pos1+1));
    }

    private static byte[] rowkeyFromVoxelName(long segId, int cuboidId, String dimValueString ){
        return Bytez.add(Bytez.add(Bytez.from(segId),Bytez.from(cuboidId)), Bytez.from(dimValueString));
    }

    public byte[] getIndicator(){
        return Bytez.copy(this.getRow(),8);
    }

    public int getCuboidId(){
        return Bytez.toInt(this.getRow(), 8);
    }
    public long getSegmentId(){
        return Bytez.toLong(this.getRow());
    }

    public List<Object> getDimValues(){
        try {
            return Obase.mapper.readValue(this.getDimValueString(), List.class);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public String getDimValueStringWith(List<Integer> lefts)throws Exception{
        if(USE_LAZY_ARRAY_FOR_JSON) {
            try {
                LazyArray lazyArray = LazyArray.parseArray(this.getDimValueString());
                return lazyArray.toJSONString(lefts);
            }catch (Exception e){
                log.error("OLAP USE_LAZY_ARRAY_FOR_JSON is not matched, change to normal mode. {} {}",  this.getDimValueString(), lefts, e);
                USE_LAZY_ARRAY_FOR_JSON = false;
            }
        }

        List<Object> jsonArray = getDimValues();
        List<Object> leftArray = new ArrayList<>(lefts.size());
        for (int i : lefts) leftArray.add(jsonArray.get(i));
        return Obase.mapper.writeValueAsString(leftArray);
    }

    public String getDimValueString(){
        return Bytez.toString(this.getRow(), 12);
    }

    public void setStat(MultiMeasureStat value){
        this.setAttribute("stat", value.toBytes());
    }
    public MultiMeasureStat getStat(){
        byte[] bytes = this.getAttribute("stat");
        if(bytes == null)return null;

        return MultiMeasureStat.from(bytes);
    }

    public long getFactCount(){
        byte[] bytes = this.getAttribute("stat");
        if(bytes == null)return 0;

        return Bytez.toLong(bytes);
    }

    public boolean valid(){
        if(getFactCount() != 0)
            return true;

        for(SingleMeasureStat singleMeasureStat : this.getStat().getMeasures()){
            if(singleMeasureStat.getSum() != 0){
                return true;
            }
        }

        return false;
    }

    public long size(){
        return getRowSize() + getStatSize() + getDistinctSize() + 10;
    }
    private int getRowSize(){
        return this.getRow().length;
    }
    private int getStatSize(){
        byte[] bytes = this.getAttribute("stat");
        if(bytes == null){
            return 0;
        }
        return bytes.length;
    }
    private int getDistinctSize(){
        byte[] bytes = this.getAttribute("distinct");
        if(bytes == null){
            return 0;
        }
        return bytes.length;
    }

    synchronized public void addStat(MultiMeasureStat statAdd){
        MultiMeasureStat stat = this.getStat();
        if(stat == null){
            this.setStat(statAdd);
        }else {
            stat.combine(statAdd);
            this.setStat(stat);
        }
    }

    synchronized public void combine(Voxel voxel){
        combineStat(voxel);
        //add distinct
        HLLDistinct distinct = voxel.getDistinct();
        if (distinct != null) {
            this.combineDistinct(distinct);
        }
    }

    private void combineStat(Voxel voxel){
        MultiMeasureStat stat = this.getStat();
        if(stat == null){
            this.setStat(voxel.getStat());
        }else {
            stat.combine(voxel.getStat());
            this.setStat(stat);
        }
    }

    public HLLDistinct getDistinct(){
        byte[] bytes = this.getAttribute("distinct");
        if(bytes == null)return null;

        return DwUtil.hllDistinctFrom(bytes);
    }
    public void setDistinct(HLLDistinct distinct){
        byte[] bytes = DwUtil.hllDistinctTo(distinct);
        this.setAttribute("distinct", bytes);
    }
    synchronized public void combineDistinct(HLLDistinct distinct){
        HLLDistinct current = this.getDistinct();
        if(current != null){
            current.merge(distinct);
        }else{
            current = distinct;
        }

        this.setDistinct(current);
    }

    public static Stream<Voxel> stream() {
        return stream(Voxel.class).map(e -> new Voxel(e));
    }

    public static Stream<Voxel> stream(Segment segment, boolean cacheBlocks) {
        byte[] startRow = Bytez.from(segment.getId());
        byte[] stopRow  = Bytez.from(segment.getId()+1);

        return stream(startRow, stopRow, cacheBlocks);
    }

    public static Stream<Voxel> stream(Segment segment, Cuboid cuboid) {
        byte[] startRow = Bytez.add(Bytez.from(segment.getId()), Bytez.from(cuboid.getId()));
        byte[] stopRow  = Bytez.add(Bytez.from(segment.getId()), Bytez.from(cuboid.getId()+1));

        return stream(startRow, stopRow, false);
    }

    private static Stream<Voxel> stream(byte[] startRow, byte[] stopRow, boolean cacheBlocks){
        Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);
        scan.setCacheBlocks(cacheBlocks);
        return stream(Voxel.class, scan).map(e->new Voxel(e));
    }
}
