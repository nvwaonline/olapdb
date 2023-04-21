package com.olapdb.core.config;

import lombok.Getter;

@Getter
public enum SegmentLevel {
    LEVEL_0(0,"level00", 5, 20),
    LEVEL_1(1,"level01", 5, 10),
    LEVEL_2(2,"level02", 5, 5),
    LEVEL_3(3,"level03", 5, 5),
    LEVEL_4(4,"level04", 5, 5),
    LEVEL_5(5,"level05", 5, 5),
    LEVEL_6(6,"level06", 5, 5),
    LEVEL_7(7,"level07", 5, 5),
    LEVEL_8(8,"level08", 5, 5),
    LEVEL_9(9,"level09", 5, 5),
    LEVEL_10(10,"level10", 5, 5),
    LEVEL_11(11,"level11", 5, 5),
    LEVEL_12(12,"level12", 5, 5),
    LEVEL_13(13,"level13", 5, 5),
    LEVEL_14(14,"level14", 5, 5),
    LEVEL_15(15,"level15", 5, 5),
    LEVEL_16(16,"level16", 5, 5),
    LEVEL_17(17,"level17", 5, 5),
    LEVEL_18(18,"level18", 5, 5),
    LEVEL_19(19,"level19", 5, 5),
    LEVEL_20(20,"level20", 5, 5),
    LEVEL_21(21,"level21", 5, 5),
    LEVEL_22(22,"level22", 5, 5),
    LEVEL_23(23,"level23", 5, 5),
    LEVEL_24(24,"level24", 5, 5),
    ;

    private SegmentLevel(int code, String name, int mergeSmall, int mergeLarge) {
        this.code = code;
        this.name = name;
        this.mergeSmall = mergeSmall;
        this.mergeLarge = mergeLarge;
    }

    public static SegmentLevel fromName(String segmentName){
        for(SegmentLevel segmentType : SegmentLevel.values()){
            if (segmentType.getName().equals(segmentName)) {
                return segmentType;
            }
        }
        return null;
    }

    public static SegmentLevel fromCode(int code){
        for(SegmentLevel segmentType : SegmentLevel.values()){
            if (segmentType.getCode() == code) {
                return segmentType;
            }
        }

        return null;
    }

    public static SegmentLevel fromSourceVoxelCount(long count){
        int code = (int)(Math.log(count/500000.0)/Math.log(5));
        if(code <0 ) code = 0;
        return fromCode(code);
    }

    private final int code;
    private final String name;
    private final int mergeSmall;
    private final int mergeLarge;
}
