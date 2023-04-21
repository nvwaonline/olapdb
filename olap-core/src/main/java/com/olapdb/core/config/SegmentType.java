package com.olapdb.core.config;

import lombok.Getter;

@Getter
public enum SegmentType {
    DEFAULT  (0,"default"),
    BUILD    (1,"build"),
    COMBINE  (2,"combine"),
    ;

    private SegmentType(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static SegmentType fromName(String name){
        for(SegmentType segmentType : SegmentType.values()){
            if (segmentType.getName().equals(name)) {
                return segmentType;
            }
        }
        return null;
    }

    public static SegmentType fromCode(int code){
        for(SegmentType segmentType : SegmentType.values()){
            if (segmentType.getCode() == code) {
                return segmentType;
            }
        }

        return null;
    }

    private final int code;
    private final String name;
}
