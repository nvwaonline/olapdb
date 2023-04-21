package com.olapdb.core.tables;

import com.olapdb.core.config.CuboidPhase;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.Entity;
import com.olapdb.obase.utils.Obase;
import com.olapdb.obase.utils.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class Cuboid extends Entity {
    private final static long serialVersionUID  = 4362047217551921941L;

    @SuppressWarnings("unused")
    private final static String tableName = "olapdb:cuboid";

    public Cuboid(byte[] row) {
        super(row);
    }
    public Cuboid(Result r) {
        super(r);
    }
    public Cuboid(String cuboidName) {
        super(Bytez.from(cuboidName));
    }
    public Cuboid(String cuboidName, boolean autoload) {
        super(Bytez.from(cuboidName), autoload);
    }
    public static Cuboid newInstance(String cuboidIdenticalName, int id){
        Cuboid cuboid = new Cuboid(cuboidIdenticalName, false);
        cuboid.setId(id);
        log.info("OLAP {} Created. {}", cuboid.getIdentify(), cuboidIdenticalName);
        return cuboid;
    }
    public String getCubeIdenticalName(){
        String name = Bytez.toString(this.getRow());
        return name.substring(0, name.indexOf(":"));
    }
    public String getName(){
        return Bytez.toString(this.getRow());
    }

    public String getIdentify(){
        return "Cuboid  ["+ this.getCubeIdenticalName() + "] [" + this.getId()+"]";
    }

    //如果还没有分配id,给cuboid分配id
    public void register(){
        if(this.getId() == 0){
            String counter = Obase.getIdentify()+"_OLAP_cuboidUnifyIdentify-"+ this.getCubeIdenticalName();
            long[] ids = Util.getOrderIDs(Obase.getTable(Cuboid.class), counter, 1);
            this.setId((int)ids[0]);
            log.info("OLAP {} Created. {}", this.getIdentify(), this.getName());
        }
    }

    public boolean needRegister(){
        return this.getId() == 0;
    }

    public List<String> getDimList(){
        String name = Bytez.toString(this.getRow());
        return Arrays.asList(name.substring(name.indexOf(":")+1).split(":")).stream().filter(e->e!=null&&e.trim().length() > 0).collect(Collectors.toList());
    }

    private void setId(int value){
        this.setAttribute("id", Bytez.from(value));
    }
    public int getId(){
        return this.getAttributeAsInt("id");
    }

    public void setPhase(CuboidPhase value){
        log.info("OLAP {} Phase {} --> {}", this.getIdentify(), this.getPhase().getName(), value.getName());
        this.setAttribute("phase", Bytez.from(value.getCode()));
    }
    public CuboidPhase getPhase(){
        return CuboidPhase.fromCode(this.getAttributeAsInt("phase", 0));
    }

    public void setParent(int value){
        this.setAttribute("parent", Bytez.from(value));
    }
    public int getParent(){
        return this.getAttributeAsInt("parent");
    }

    public void addCheckSegId(long value){
        this.setAttribute("checkSegIds", ""+ value, Bytez.from(value));
    }
    public void removeCheckSegId(long value){
        this.deleteAttribute("checkSegIds", ""+ value);
    }
    public List<Long> getCheckSegIds(){
        return this.getAttributeItems("checkSegIds").stream().map(e->Long.parseLong(e)).collect(Collectors.toList());
    }

    public void setQueryCount(long value){
        this.setAttribute("queryCount", Bytez.from(value));
    }
    public long getQueryCount(){
        return this.getAttributeAsLong("queryCount");
    }

    public static Stream<Cuboid> stream() {
        return stream(Cuboid.class).map(e->new Cuboid(e));
    }
    public static Stream<Cuboid> stream(Cube cube) {
        return stream(cube.getIdenticalName());
    }
    public static Stream<Cuboid> stream(String cubeName) {
        byte[] startRow = Bytez.from(cubeName + ":");
        byte[] stopRow  = Bytez.next(startRow);
        Scan scan = new Scan().withStartRow(startRow).withStopRow(stopRow);

        return stream(Cuboid.class, scan).map(e -> new Cuboid(e));
    }
}
