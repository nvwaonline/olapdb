package com.olapdb.core.config;

import lombok.Getter;

@Getter
public enum CuboidPhase {
    CANDIDATE  (0,"candidate"),
    APPROVED   (1,"approved"),
    CHARGING   (2,"charging"),
    PRODUCTIVE (3,"productive"),
    DEPRECATED (4,"deprecated"),
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
