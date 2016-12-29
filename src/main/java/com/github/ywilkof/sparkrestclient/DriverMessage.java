package com.github.ywilkof.sparkrestclient;

import lombok.Getter;
import lombok.Setter;

/**
 * Created by yonatan on 08.10.15.
 */
@Getter
@Setter
public class DriverMessage extends SparkResponse {

    DriverMessage() {}

    private DriverState state;
    private DriverMessageState messageState;
    private String executor_id;
    private String slave_id;
    private String message;

}
