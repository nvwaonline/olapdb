package com.olapdb.core.tables;

import com.olapdb.core.config.TaskPhase;
import com.olapdb.core.workingarea.WorkingAreaManager;
import com.olapdb.obase.data.Bytez;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;
import java.util.stream.Collectors;

public abstract class SegTask extends ArchiveableEntity {
    public SegTask(byte[] row, boolean autoload) {
        super(row, autoload);
    }

    public SegTask(byte[] row) {
        super(row);
    }

    public SegTask(Result r) {
        super(r);
    }

    public void setPhase(TaskPhase value){
        this.setAttribute("phase", Bytez.from(value.getCode()));
        this.setLastUpdateTime(System.currentTimeMillis());
        if(value == TaskPhase.ARCHIVE){
            this.setArchiveTime(System.currentTimeMillis());

            if(this instanceof SegBuildTask || this instanceof SegMendTask) {
                WorkingAreaManager.releaseSegment(this.getSegId());
            }
        }
    }
    public TaskPhase getPhase(){
        return TaskPhase.fromCode(this.getAttributeAsInt("phase", 0));
    }

    public abstract long getSegId();
    public abstract String getIdentify();
    public abstract Segment getSegment();

    public void setStartTime(long value) {
        this.setAttribute("startTime", Bytez.from(value));
    }
    public long getStartTime(){
        return this.getAttributeAsLong("startTime");
    }

    public void setLastUpdateTime(long value) {
        this.setAttribute("lastUpdateTime", Bytez.from(value));
    }
    public long getLastUpdateTime(){
        return this.getAttributeAsLong("lastUpdateTime");
    }

    public void setCompletedTime(long value) {
        this.setAttribute("completedTime", Bytez.from(value));
    }
    public long getCompletedTime(){
        return this.getAttributeAsLong("completedTime");
    }

    public String getHint(){
        return this.getAttributeAsString("hint");
    }
    public void setHint(String value) {
        this.setAttribute("hint", Bytez.from(value));
    }

    public String getHostName(){
        return this.getAttributeAsString("hostName");
    }
    public void setHostName(String value) {
        this.setAttribute("hostName", Bytez.from(value));
    }

    public void setSourceVoxelCount(long value){
        this.setAttribute("sourceVoxelCount", Bytez.from(value));
    }
    public long getSourceVoxelCount(){
        return this.getAttributeAsLong("sourceVoxelCount");
    }

    public void setScanVoxelCount(long value){
        this.setAttribute("scanVoxelCount", Bytez.from(value));
    }
    public long getScanVoxelCount(){
        return this.getAttributeAsLong("scanVoxelCount");
    }

    public void setGenerateVoxelCount(long value){
        this.setAttribute("generateVoxelCount", Bytez.from(value));
    }
    public long getGenerateVoxelCount(){
        return this.getAttributeAsLong("generateVoxelCount");
    }

    public void setRetryTimes(long value){
        this.setAttribute("retryTimes", Bytez.from(value));
    }
    public long getRetryTimes(){
        return this.getAttributeAsLong("retryTimes");
    }

    public double getTaskProgress(){
        if(this.getPhase() == TaskPhase.COMPLETED || this.getPhase() == TaskPhase.READY)
            return 1;

        if(this.getSourceVoxelCount() > 0)
            return Math.min(this.getScanVoxelCount()*1.0/this.getSourceVoxelCount(), 1);
        else
            return 0;
    }

    public void setCurrentPosition(byte[] value){
        this.setAttribute("currentPosition", value);
        this.setLastUpdateTime(System.currentTimeMillis());
    }
    public byte[] getCurrentPosition(){
        return this.getAttribute("currentPosition");
    }

    public void setVoxelCount(int cuboid, long value){
        this.setAttribute("voxelCount", ""+cuboid,  Bytez.from(value));
    }
    public long getVoxelCount(int cuboid){
        return this.getAttributeAsLong("voxelCount", ""+cuboid);
    }
    synchronized public void incVoxelCount(int cuboid){
        this.setVoxelCount(cuboid, this.getVoxelCount(cuboid)+1);
    }

    public void deleteVoxelCount(int cuboid){
        this.deleteAttribute("voxelCount", ""+cuboid);
    }

    public List<Integer> getVoxelCountIds(){
        return this.getAttributeItems("voxelCount").stream().map(e->Integer.parseInt(e)).collect(Collectors.toList());
    }

    public void clearVoxelCount()
    {
        this.getVoxelCountIds().forEach(e->{
            this.deleteVoxelCount(e);
        });
    }

    public void setVoxelSize(int cuboid, long value){
        this.setAttribute("voxelSize", ""+cuboid,  Bytez.from(value));
    }
    public long getVoxelSize(int cuboid){
        return this.getAttributeAsLong("voxelSize", ""+cuboid);
    }
    synchronized public void incVoxelSize(int cuboid, long size){
        this.setVoxelSize(cuboid, this.getVoxelSize(cuboid)+size);
    }

    public void deleteVoxelSize(int cuboid){
        this.deleteAttribute("voxelSize", ""+cuboid);
    }

    public List<Integer> getVoxelSizeIds(){
        return this.getAttributeItems("voxelSize").stream().map(e->Integer.parseInt(e)).collect(Collectors.toList());
    }

    public void clearVoxelSize()
    {
        this.getVoxelSizeIds().forEach(e->{
            this.deleteVoxelSize(e);
        });
    }
}
