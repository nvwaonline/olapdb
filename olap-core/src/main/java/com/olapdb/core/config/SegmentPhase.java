package com.olapdb.core.config;

import lombok.Getter;

@Getter
public enum SegmentPhase {
    CREATED             (0,"created"),              //created
    CHARGING            (1,"charging"),             //in building
    PRODUCTIVE          (2,"productive"),           //in production
    MERGING             (3,"merging"),              //merging
    ARCHIVE             (4,"archive"),              //archieved, to be deleted
    ;


//    created charging productive merging archive
    private SegmentPhase(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static SegmentPhase fromName(String segmentName){
        for(SegmentPhase phase : SegmentPhase.values()){
            if (phase.getName().equals(segmentName)) {
                return phase;
            }
        }
        return null;
    }

    public static SegmentPhase fromCode(int code){
        for(SegmentPhase segmentPhase : SegmentPhase.values()){
            if (segmentPhase.getCode() == code) {
                return segmentPhase;
            }
        }

        return null;
    }

    private final int code;
    private final String name;
}
