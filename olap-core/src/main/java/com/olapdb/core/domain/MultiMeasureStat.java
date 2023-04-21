package com.olapdb.core.domain;

import com.olapdb.obase.data.Bytez;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;

@Getter
@Setter
@Slf4j
@ToString
public class MultiMeasureStat {
    private long count;
    private SingleMeasureStat[] measures;

    public MultiMeasureStat(int measureCount){
        measures = new SingleMeasureStat[measureCount];
    }

    public void combine(MultiMeasureStat stat){
        this.count += stat.count;

        for(int i=0; i<measures.length; i++){
            measures[i].combine(stat.measures[i]);
        }
    }

    public byte[] toBytes(){
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(8+measures.length*12);
            baos.write(Bytez.from(count));
            for(int i=0; i<measures.length; i++){
                baos.write(Bytez.from((float) measures[i].getMin()));
                baos.write(Bytez.from((float) measures[i].getMax()));
                baos.write(Bytez.from((float) measures[i].getSum()));
            }
            baos.close();

            return baos.toByteArray();
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public static MultiMeasureStat from(byte[] bytes){
        int measureCount = (bytes.length - 8)/12;
        MultiMeasureStat multiMeasureStat = new MultiMeasureStat(measureCount);
        multiMeasureStat.count = Bytez.toLong(bytes);
        for(int i=0; i<measureCount; i++){
            multiMeasureStat.measures[i] = new SingleMeasureStat();
            multiMeasureStat.measures[i].setMin(Bytez.toFloat(bytes,i*12+8));
            multiMeasureStat.measures[i].setMax(Bytez.toFloat(bytes,i*12+12));
            multiMeasureStat.measures[i].setSum(Bytez.toFloat(bytes,i*12+16));
        }

        return multiMeasureStat;
    }
}
