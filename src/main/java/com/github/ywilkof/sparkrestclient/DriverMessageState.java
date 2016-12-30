package com.github.ywilkof.sparkrestclient;

/**
 * Created by yonatan on 09.10.15.
 */
public enum DriverMessageState {
	TASK_FINISHED, TASK_FAILED,TASK_RUNNING,TASK_SUBMITTED,TASK_RELAUNCHING,TASK_UNKNOWN,TASK_KILLED,TASK_ERROR,TASK_NOT_FOUND;
}
