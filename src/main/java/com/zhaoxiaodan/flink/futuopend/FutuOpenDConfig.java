package com.zhaoxiaodan.flink.futuopend;

import java.io.Serializable;

import lombok.Data;

@Data
public class FutuOpenDConfig implements Serializable {
    private String opendIP = "127.0.0.1";
    private Integer opendPort = 11111;
    private String codes;
    private String subType;
}
