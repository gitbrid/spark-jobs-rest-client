package com.github.ywilkof.sparkrestclient;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by yonatan on 11.11.15.
 */
@RunWith(MockitoJUnitRunner.class)
public class SparkRestSubmitTest {

    @Test
    public void testSubmitJob() throws Exception {

		 String sparkVersion = "2.0.0";

	        SparkRestClient sparkRestClient = SparkRestClient.builder()
	                .clusterMode(ClusterMode.mesos)
	                .masterHost("10.100.23.7")
	                .sparkVersion(sparkVersion)
	                .masterPort(7077)
	                .build();
//		List list = new ArrayList();
//		list.add("20160904");
//		Set<String> set = new HashSet<String>();
//		set.add("hdfs://10.100.23.7:8020/datacenter/jar/command_yzdc_dw_spark-0.1.0.jar");
//		try{
//			String submissionId = sparkRestClient.prepareJobSubmit()
//                .appName("testSparkSubmit")
//                .appArgs(list)
//                .appResource("hdfs://10.100.23.7:8020/datacenter/jar/yzdc_dw_spark-hbase.0.98.jar")
//                .mainClass("com.yunzongnet.datacenter.spark.main.SparkMainBuShu")
//                .usingJars(set)
//                .withProperties()
//                .put("spark.mesos.executor.docker.image", "registry.wowotuan.me:443/spark-2-base:v1")
//                .put("spark.mesos.principal","yuanhuaiyu")
//                .put("spark.mesos.secret", "yuanhuaiyu")
//                .put("spark.executor.memory", "12G")
//                .put("spark.cores.max", "4")
//                .put("spark.executor.cores", "2")
//                .put("spark.mesos.uris", "hdfs://10.100.23.7:8020/datacenter/jar/mysql-connector-java-5.1.37.jar")
//                .put("spark.driver.extraLibraryPath", "/mnt/mesos/sandbox/mysql-connector-java-5.1.37.jar")
//                .submit();
//		System.out.println(submissionId);
//		}catch(Exception ex){
//			
//		}
	        DriverMessage result = sparkRestClient.checkJobStatus()
	        		.withSubmissionId("driver-20161228171247-158664").getSubmissionIdMessage();

	    	System.out.println(result.getMessage());
//	       
    }
   
}