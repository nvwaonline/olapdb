package com.olapdb.core.config;

import lombok.Getter;

@Getter
public enum TaskPhase {
    CREATED    (0,"created"),
    PREPARED   (1,"prepared"),
    SUBMITED   (2,"submited"),
    EXECUTING  (3,"executing"),
    COMPLETED  (4,"completed"),
    FAILED_TRY (5,"failed_try"),
    FAILED     (6,"failed"),
    READY      (7,"ready"),
    ARCHIVE    (8,"archive"),
    ;

//    created execute complete failed archive

    private TaskPhase(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static TaskPhase fromName(String segmentName){
        for(TaskPhase phase : TaskPhase.values()){
            if (phase.getName().equals(segmentName)) {
                return phase;
            }
        }
        return null;
    }

    public static TaskPhase fromCode(int code){
        for(TaskPhase phase : TaskPhase.values()){
            if (phase.getCode() == code) {
                return phase;
            }
        }

        return null;
    }

    private final int code;
    private final String name;
}
