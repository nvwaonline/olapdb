package com.olapdb.core.config;

import lombok.Getter;

@Getter
public enum CuboidPhase {
    CANDIDATE  (0,"candidate"), //候选
    APPROVED   (1,"approved"),  //已通过审核
    CHARGING   (2,"charging"),  //准备中
    PRODUCTIVE (3,"productive"),//生产中
    DEPRECATED (4,"deprecated"),//已废弃
    ;

    private CuboidPhase(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static CuboidPhase fromName(String segmentName){
        for(CuboidPhase cuboidState : CuboidPhase.values()){
            if (cuboidState.getName().equals(segmentName)) {
                return cuboidState;
            }
        }
        return null;
    }

    public static CuboidPhase fromCode(int code){
        for(CuboidPhase cuboidState : CuboidPhase.values()){
            if (cuboidState.getCode() == code) {
                return cuboidState;
            }
        }

        return null;
    }

    private final int code;
    private final String name;
}
