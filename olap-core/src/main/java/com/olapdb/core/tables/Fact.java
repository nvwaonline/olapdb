package com.olapdb.core.tables;

import com.olapdb.core.scanners.HorizontalFactScanner;
import com.olapdb.obase.data.Bytez;
import com.olapdb.obase.data.Entity;
import com.olapdb.obase.utils.Obase;
import com.olapdb.obase.utils.Util;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.util.Arrays;
import java.util.List;
import java.util.Spliterators;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class Fact extends Entity {
    public final static int PARTION_COUNT = 16;
    protected final static String tableName = "olapdb:fact";

    public Fact(byte[] row, boolean autoload) {
        super(row, autoload);
    }

    public Fact(byte[] row) {
        super(row);
    }

    public Fact(Result r) {
        super(r);
    }

    public static Fact newInstance(String fact){
        byte[] row = Bytez.add(Bytez.from(fact+"|"), Bytez.from(Util.nanoTime()), Util.getLocalIPV4());

        int hashCode = Arrays.hashCode(row);
        if(hashCode <0)hashCode = 0-hashCode;
        byte partid = (byte)(hashCode%PARTION_COUNT);

        row = Bytez.add(Bytez.from(partid), row);

        return new Fact(row, false);
    }

    public String getFactName(){
        byte[] row = this.getRow();
        return Bytez.toString(row, 1, row.length - 9-1-4);
    }

    public Long getAddTimeNano(){
        byte[] row = this.getRow();
        return Bytez.toLong(row, row.length -8-4);
    }

    public byte[] getIndicator(){
        return Bytez.copy(this.getRow(),1);
    }

    public void setContent(String value){
        this.setAttribute("content", Bytez.from(value));
    }
    public String getContent(){
        return this.getAttributeAsString("content");
    }

    public static Stream<Fact> stream(String cubeName, long startTimeNano, long stopTimeNano) {
        byte[] startRow = Bytez.add(Bytez.from(cubeName + "|"), Bytez.from(startTimeNano));
        byte[] stopRow = Bytez.add(Bytez.from(cubeName + "|"), Bytez.from(stopTimeNano));
        return stream(startRow, stopRow, false);
    }

    public static Stream<Fact> stream(byte[] startRow, byte[] stopRow, boolean cacheBlocks){
        try {
            List<ResultScanner> scanners = new Vector<>();
            for (byte partid = 0; partid < PARTION_COUNT; partid++) {
                Scan scan = new Scan()
                        .withStartRow(Bytez.add(Bytez.from(partid), startRow),false)
                        .withStopRow(Bytez.add(Bytez.from(partid), stopRow),true);
                scan.setCacheBlocks(cacheBlocks);
                scanners.add(Obase.getTable(Fact.class).getScanner(scan));
            }

            HorizontalFactScanner horizontalFactScanner = new HorizontalFactScanner(scanners);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(horizontalFactScanner, 0), false);
        }catch (Exception e){
            log.error("创建Fact多分区并行扫描失败", e);
            return null;
        }
    }
    public static Stream<Fact> stream() {
        Scan scan = new Scan().setCacheBlocks(false);
        return stream(Fact.class, scan).map(e -> new Fact(e));
    }
}
