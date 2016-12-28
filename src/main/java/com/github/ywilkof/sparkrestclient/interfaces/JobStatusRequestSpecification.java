package com.github.ywilkof.sparkrestclient.interfaces;

import com.github.ywilkof.sparkrestclient.DriverMessage;
import com.github.ywilkof.sparkrestclient.DriverState;
import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;

/**
 * Created by yonatan on 17.10.15.
 */
public interface JobStatusRequestSpecification {
	JobStatusRequestSpecification withSubmissionId(String submissionId)  throws FailedSparkRequestException;
	DriverState getSubmissionIdStatus()  throws FailedSparkRequestException;
	DriverMessage getSubmissionIdMessage() throws FailedSparkRequestException ;
	
}
