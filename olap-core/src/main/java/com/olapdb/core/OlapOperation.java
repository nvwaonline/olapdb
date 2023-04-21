package com.olapdb.core;

import lombok.Getter;

@Getter
public enum OlapOperation {
    ADD     (0,"ADD"),
    REMOVE  (1,"REMOVE"),
    ;

    private OlapOperation(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static String getKey(){
        return "OLAP_OPERATION";
    }

    public static OlapOperation fromName(String operationName){
        for(OlapOperation phase : OlapOperation.values()){
            if (phase.getName().equals(operationName)) {
                return phase;
            }
        }
        return null;
    }

    public static OlapOperation fromCode(int code){
        for(OlapOperation phase : OlapOperation.values()){
            if (phase.getCode() == code) {
                return phase;
            }
        }

        return null;
    }

    private final int code;
    private final String name;


    public static void main(String[] args){
        System.out.println(OlapOperation.ADD.toString());
    }
}
