package com.olapdb.core.domain;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Getter
@Setter
@Slf4j
@ToString
public class SingleMeasureStat {
    private float sum;
    private float max;
    private float min;

    public void combine(SingleMeasureStat stat){
        this.sum += stat.sum;
        this.max = Math.max(this.max, stat.max);
        this.min = Math.min(this.min, stat.min);
    }
}
