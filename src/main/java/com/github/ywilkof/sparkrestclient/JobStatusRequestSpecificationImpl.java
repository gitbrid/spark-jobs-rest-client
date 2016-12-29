package com.github.ywilkof.sparkrestclient;

import org.apache.http.client.methods.HttpGet;

import com.github.ywilkof.sparkrestclient.interfaces.CanValidateSubmissionId;
import com.github.ywilkof.sparkrestclient.interfaces.JobStatusRequestSpecification;

public class JobStatusRequestSpecificationImpl implements JobStatusRequestSpecification, CanValidateSubmissionId {

	private SparkRestClient sparkRestClient;
	private JobStatusResponse jobStatusResponse;

	public JobStatusRequestSpecificationImpl(SparkRestClient sparkRestClient) {
		this.sparkRestClient = sparkRestClient;
	}

	/**
	 * Gets the status of an existing Driver Application
	 * 
	 * @param submissionId
	 *            Id of submitted job to request status for.
	 * @return State of the application
	 * @throws FailedSparkRequestException
	 *             Request to Spark server failed, or the Spark Server could not
	 *             retrieve the status of the requested app.
	 */
	@Override
	public JobStatusRequestSpecification withSubmissionId(String submissionId) throws FailedSparkRequestException {
		final JobStatusResponse response = this.fetchJobStatusRequest(submissionId);
		this.jobStatusResponse = response;
		return this;
	}

	@Override
	public DriverState getSubmissionIdStatus() throws FailedSparkRequestException {
		return jobStatusResponse.getDriverState();
	}

	@Override
	public DriverMessage getSubmissionIdMessage() throws FailedSparkRequestException {
		DriverMessage driverMessage = new DriverMessage();
		String string = this.jobStatusResponse.getMessage().replaceAll("\n\\s+|\"", "").replaceAll("\\s+\\{", ".")
				.replaceAll(":\\s+", ":").replaceAll("\\}\n", "");
		String[] msg = string.split("\n");
		for (String str : msg) {
			this.getMessageValue(driverMessage, str);
		}
		driverMessage.setState(this.jobStatusResponse.getDriverState());
		return driverMessage;
	}

	private JobStatusResponse fetchJobStatusRequest(String submissionId) throws FailedSparkRequestException {
		assertSubmissionId(submissionId);
		final String url = "http://" + sparkRestClient.getMasterUrl() + "/v1/submissions/status/" + submissionId;
		final JobStatusResponse response = HttpRequestUtil.executeHttpMethodAndGetResponse(sparkRestClient.getClient(),
				new HttpGet(url), JobStatusResponse.class);
		if (!response.getSuccess()) {
			throw new FailedSparkRequestException("submit was not successful.");
		}
		return response;
	}

	/**
	 * get spark message infomations
	 * @param driverMessage
	 * @param str
	 */
	public void getMessageValue(DriverMessage driverMessage, String str) {
		String[] value = str.split(":");
		String startWith="executor_id.value:";
		if (str.startsWith(startWith)) {
			driverMessage.setExecutor_id(str.replace(startWith, ""));
		}
		startWith = "slave_id.value:";
		if (str.startsWith("slave_id.value:")) {
			driverMessage.setSlave_id(str.replace(startWith, ""));
		}
		startWith = "message:";
		if (str.startsWith(startWith)) {
			driverMessage.setMessage(str.replace(startWith, ""));
		}
		startWith = "state:";
		if (str.startsWith(startWith)) {
			driverMessage.setMessageState(DriverMessageState.valueOf(str.replace(startWith, "")));
		}
	}

}
