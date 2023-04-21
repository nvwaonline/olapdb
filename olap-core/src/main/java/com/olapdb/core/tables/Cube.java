package com.olapdb.core.tables;

import com.olapdb.core.utils.DwUtil;
import com.olapdb.core.utils.TimeUtil;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.RemoteObject;
import com.olapdb.obase.data.TimeEntity;
import com.olapdb.obase.utils.Obase;
import com.olapdb.obase.utils.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Cube extends TimeEntity {
    private final static long serialVersionUID  = 1471214457L;

    @SuppressWarnings("unused")
    private final static String tableName = "olapdb:cube";

    public Cube(byte[] row) {
        super(row);
    }
    public Cube(Result r) {
        super(r);
    }
    public Cube(String name) {
        super(Bytez.from(name));
    }

    @Override
    public RemoteObject connect(){
        if(this.needConnect()){
            if(this.getId() == 0){
                this.setId((int) Util.getOrderID(Cube.class, Obase.getIdentify()+"_OLAP_cubeUnifyIdentify"));
            }

            super.connect();
        }

        return this;
    }

    public String getName(){
        return Bytez.toString(this.getRow());
    }

    public String getIdenticalName(){
        return getName() + "-"+this.getId();
    }

    public String getSchemaName(){
        String fullName = Bytez.toString(this.getRow());
        return fullName.substring(0, fullName.indexOf("."));
    }
    public String getTableName(){
        String fullName = Bytez.toString(this.getRow());
        return fullName.substring(fullName.indexOf(".")+1);
    }


    private void setId(int value){
        this.setAttribute("id", Bytez.from(value));
    }
    public int getId(){
        return this.getAttributeAsInt("id");
    }

    public void setBatchSize(int value){
        this.setAttribute("batchSize", Bytez.from(value));
    }
    public int getBatchSize(){
        return this.getAttributeAsInt("batchSize", 100000);
    }

    public void setAuditCuboid(boolean value){
        this.setAttribute("auditCuboid", Bytez.from(value));
    }
    public boolean getAuditCuboid(){
        return this.getAttributeAsBoolean("auditCuboid");
    }

    public void setEnable(boolean value){
        this.setAttribute("enable", Bytez.from(value));
    }
    public boolean getEnable(){
        return this.getAttributeAsBoolean("enable");
    }

    private void setDataSourceId(int value){
        this.setAttribute("dataSourceId", Bytez.from(value));
    }
    public int getDataSourceId(){
        return this.getAttributeAsInt("dataSourceId");
    }

    public void setMergeThreshold(long value){
        this.setAttribute("mergeThreshold", Bytez.from(value));
    }
    public long getMergeThreshold(){
        return this.getAttributeAsLong("mergeThreshold", 1000000000);
    }

    public void setLastScanNano(long value){
        this.setAttribute("lastScanNano", Bytez.from(value));
    }
    public long getLastScanNano(){
        return this.getAttributeAsLong("lastScanNano");
    }

    public String getFactName(){
        return this.getAttributeAsString("factName");
    }
    public void setFactName(String value) {
        this.setAttribute("factName", Bytez.from(value));
    }

    public boolean getDeleteFact(){
        return this.getAttributeAsBoolean("deleteFact");
    }
    public void setDeleteFact(boolean value) {
        this.setAttribute("deleteFact", Bytez.from(value));
    }

    public void setDataTTL(long value){
        this.setAttribute("dataTTL", Bytez.from(value));
    }
    public long getDataTTL(){
        return this.getAttributeAsLong("dataTTL", 0);
    }

    public String getDimensions(){
        return this.getAttributeAsString("dimensions");
    }
    public void setDimensions(String value) {
        this.setAttribute("dimensions", Bytez.from(value));
    }
    public List<String> getDimensionList(){
        String[] dimensions =  this.getDimensionsWithoutGroup().split(",");
        List<String> dimensionList = new ArrayList<>();
        for(String dim : dimensions){
            dimensionList.add(dim.split(":")[0]);
        }

        if(!this.getTimeDimension().isEmpty()){
            TimeUtil.replaceTimeDimension(dimensionList, getTimeDimension());
        }

        return DwUtil.timeSortDims(dimensionList, this.getIndexDimensionList());
    }
    public String getDimensionType(String dim){
        String[] dimensions =  this.getDimensionsWithoutGroup().split(",");
        for(String measure : dimensions){
            String[] segs = measure.split(":");
            if(segs.length ==2 && segs[0].equals(dim)){
                return segs[1];
            }
        }

        return null;
    }

    public List<String> getDimGroups(){
        List<String> groups = Arrays.asList(this.getDimensions().split("\\|"))
                .stream()
                .filter(e->!e.isEmpty())
                .map(group->DwUtil.dimensionsRemoveType(group))
                .collect(Collectors.toList());

        if(this.getTimeDimension().isEmpty()){
            return groups;
        }

        return groups.stream().map(e-> TimeUtil.replaceTimeDimension(e, this.getTimeDimension())).collect(Collectors.toList());
    }

    public String getDimensionsWithoutGroup(){
        return this.getDimensions().replace('|', ',');
    }

    public String getIndexDimensions(){
        return this.getAttributeAsString("indexDimensions");
    }
    public void setIndexDimensions(String value) {
        this.setAttribute("indexDimensions", Bytez.from(value));
    }
    public List<String> getIndexDimensionList(){
        return Arrays.asList(this.getIndexDimensions().split(":")).stream().filter(e->!e.isEmpty()).collect(Collectors.toList());
    }

    public String getHierachyDimensions(){
        return this.getAttributeAsString("hierachyDimensions");
    }
    public String getHierachyDimensionsForDisplay(){
        List<String> hierachyDimensionList = Arrays.asList(this.getHierachyDimensions().split(","))
                .stream()
                .filter(e->e.indexOf("OLAP_YEAR")<0&&e.indexOf("OLAP_MONTH")<0)
                .collect(Collectors.toList());
        return org.apache.commons.lang3.StringUtils.join(hierachyDimensionList, ",");
    }
    public void setHierachyDimensions(String value) {
        this.setAttribute("hierachyDimensions", Bytez.from(value));
    }
    public List<List<String>> getHierachyDimensionList(){
        return Arrays.asList(this.getHierachyDimensions().split(","))
                .stream()
                .filter(e->!e.isEmpty())
                .map(e->Arrays.asList(e.split(":"))).collect(Collectors.toList());
    }

    public String getJointDimensions(){
        return this.getAttributeAsString("jointDimensions");
    }
    public void setJointDimensions(String value) {
        this.setAttribute("jointDimensions", Bytez.from(value));
    }
    public List<List<String>> getJointDimensionList(){
        return Arrays.asList(this.getJointDimensions().split(","))
                .stream()
                .filter(e->!e.isEmpty())
                .map(e->Arrays.asList(e.split(":"))).collect(Collectors.toList());
    }

    public String getMandatoryDimensions(){
        return this.getAttributeAsString("mandatoryDimensions");
    }
    public void setMandatoryDimensions(String value) {
        this.setAttribute("mandatoryDimensions", Bytez.from(value));
    }
    public List<String> getMandatoryDimensionList(){
        return Arrays.asList(this.getMandatoryDimensions().split(":")).stream().filter(e->!e.isEmpty()).collect(Collectors.toList());
    }
    public String getMandatoryDimensionsForDisplay(){
        List<String> mandatoryDimensionList = Arrays.asList(this.getMandatoryDimensions().split(":"))
                .stream()
                .filter(e->!TimeUtil.OlapTimeDims.contains(e))
                .collect(Collectors.toList());
        return org.apache.commons.lang3.StringUtils.join(mandatoryDimensionList, ":");
    }

    public String getMeasures(){
        return this.getAttributeAsString("measures");
    }
    public void setMeasures(String value) {
        this.setAttribute("measures", Bytez.from(value));
    }
    public List<String> getMeasureList(){
        String[] measures =  this.getMeasures().split(",");
        List<String> measureList = new ArrayList<>();
        for(String measure : measures){
            if(!StringUtils.isEmpty(measure)) {
                measureList.add(measure.split(":")[0]);
            }
        }
        return measureList;
    }

    private List<String> pureMeasureList = null;
    public List<String> getPureMeasureList(){
        if(pureMeasureList == null){
            pureMeasureList = getMeasureList().stream().filter(e->e.indexOf(".")<0).collect(Collectors.toList());
        }

        return pureMeasureList;
    }

    public String getTimeDimension(){
        return this.getAttributeAsString("timeDimension");
    }
    public String setTimeDimension(String value) {
        String dimType = getDimensionType(value);
        if(dimType == null){
            log.info("Dimention [{}] not found. setTimeDimension failed.", value);
            return "Dimention [{}] not found. setTimeDimension failed.";
        }

        switch (dimType){
            case "date":
            case "datetime":
            case "timestamp":
                this.setAttribute("timeDimension", Bytez.from(value));
                break;
            default:
                log.error("Dimention [{}] type is [{}], not date,datetime or timestamp type. setTimeDimension failed.", value, dimType);
                return "Dimention [{}] type is [{}], not date,datetime or timestamp type. setTimeDimension failed.";
        }

        return null;
    }

    public String getSqlRowString(){
        String ret = this.getDimensionsWithoutGroup();
        if(! this.getTimeDimension().isEmpty()) {
            ret = DwUtil.timeSortDimensionString(ret);
        }

        ret += ",OLAP_COUNT:long";

        for(String measure : this.getMeasureList()){
            if(measure.endsWith(".distinct")){
                ret += "," + measure + ":long";
            }else {
                ret += "," + measure + ".min:double";
                ret += "," + measure + ".max:double";
                ret += "," + measure + ".sum:double";
            }
        }
        return ret;
    }

    public static Stream<Cube> stream() {
        return stream(Cube.class).map(e -> new Cube(e));
    }
    public static Stream<Cube> stream(String schemaName){
        byte[] start = Bytez.from(schemaName+".");
        Scan scan = new Scan().withStartRow(start).withStopRow(Bytez.next(start));
        return stream(Cube.class, scan).map(e -> new Cube(e));
    }
}
