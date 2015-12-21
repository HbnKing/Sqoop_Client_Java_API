package com;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.*;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.*;

public class Connection {

	public static String url = "http://10.1.9.91:12000/sqoop/";
	public static SqoopClient client = new SqoopClient(url);

	public static Long connection(){
		
		MConnection newCon = client.newConnection(1);
//		MConnection newCon = client.getConnection(1115);
		//Get connection and framework forms. Set name for connection
		MConnectionForms conForms = newCon.getConnectorPart();
		MConnectionForms frameworkForms = newCon.getFrameworkPart();
		newCon.setName("JAVA_API_TEST");
		
		//Set connection forms values
		conForms.getStringInput("connection.connectionString").setValue("jdbc:mysql://10.1.30.3:3306/Sqoop_test");
		conForms.getStringInput("connection.jdbcDriver").setValue("com.mysql.jdbc.Driver");
		conForms.getStringInput("connection.username").setValue("root");
		conForms.getStringInput("connection.password").setValue("root");
		
		frameworkForms.getIntegerInput("security.maxConnections").setValue(0);
		Status status = client.createConnection(newCon);
//		Status status = client.updateConnection(newCon);
		if(status.canProceed()) {
			System.out.println("Created. New Connection ID : " +newCon.getPersistenceId());
		} else {
			System.out.println("Check for status and forms error ");
		}


		
		MJob newjob = client.newJob(newCon.getPersistenceId(), org.apache.sqoop.model.MJob.Type.EXPORT);
//		MJob newjob = client.getJob(jid);
		MJobForms connectorForm = newjob.getConnectorPart();
		MJobForms frameworkForm = newjob.getFrameworkPart();

		newjob.setName("export_test");
		//Database configuration
//		connectorForm.getStringInput("table.schemaName").setValue("");
		//Input either table name or sql
		connectorForm.getStringInput("table.tableName").setValue("export_data_chinese");
//		connectorForm.getStringInput("table.sql").setValue("select id,name from table where ${CONDITIONS}");
		connectorForm.getStringInput("table.columns").setValue("number,chinese,double_number");

		//Input configurations
		frameworkForm.getStringInput("input.inputDirectory").setValue("/user/zpl_test_export/test_timestamp.dat");

		//Job resources
//		frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
//		frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

		Status status_2 = client.createJob(newjob);
//		Status status_2 = client.updateJob(newjob);
		if(status_2.canProceed()) {
		  System.out.println("New Job ID: "+ newjob.getPersistenceId());
		} else {
		  System.out.println("Check for status and forms error ");
		}

		//Print errors or warnings
		printMessage(newjob.getConnectorPart().getForms());
		printMessage(newjob.getFrameworkPart().getForms());
		return newjob.getPersistenceId();
	}
	
	private static void printMessage(List<MForm> formList) {
		  for(MForm form : formList) {
		    List<MInput<?>> inputlist = form.getInputs();
		    if (form.getValidationMessage() != null) {
		      System.out.println("Form message: " + form.getValidationMessage());
		    }
		    for (MInput<?> minput : inputlist) {
		      if (minput.getValidationStatus() == Status.ACCEPTABLE) {
		        System.out.println("Warning:" + minput.getValidationMessage());
		      } else if (minput.getValidationStatus() == Status.UNACCEPTABLE) {
		        System.out.println("Error:" + minput.getValidationMessage());
		      }
		    }
		  }
	}
	
	public static void submission(Long jid){
		MSubmission submission = client.startSubmission(jid);
		System.out.println("Status : " + submission.getStatus());
		if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
			System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
		System.out.println("Hadoop job id :" + submission.getExternalId());
		System.out.println("Job link : " + submission.getExternalLink());
		Counters counters = submission.getCounters();
		if(counters != null) {
			System.out.println("Counters:");
			for(CounterGroup group : counters) {
				System.out.print("\t");
				System.out.println(group.getName());
				for(Counter counter : group) {
					System.out.print("\t\t");
					System.out.print(counter.getName());
					System.out.print(": ");
					System.out.println(counter.getValue());
				}
			}
		}
		if(submission.getExceptionInfo() != null) {
			System.out.println("Exception info : " +submission.getExceptionInfo());
		}
	}
	
	public static void checkstatus(Long jid){
		MSubmission submission = client.getSubmissionStatus(jid);
		if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
			System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
	}
	
	public static void stopmission( Long jid ){
		client.stopSubmission(jid);
	}
	
	public static void Connection_describe(Long cid){
		//Use getJob(jid) for describing job.
		//While printing connection forms, pass connector id to getResourceBundle(cid).
		describe(client.getConnection(cid).getConnectorPart().getForms(), client.getResourceBundle(1));
		describe(client.getConnection(cid).getFrameworkPart().getForms(), client.getFrameworkResourceBundle());	
	}
	
	public static void Job_describe(Long jid){
		//Use getJob(jid) for describing job.
		//While printing connection forms, pass connector id to getResourceBundle(cid).
		describe(client.getJob(jid).getConnectorPart().getForms(), client.getResourceBundle(1));		
		describe(client.getJob(jid).getFrameworkPart().getForms(), client.getFrameworkResourceBundle());
	}
	
	private static void describe(List<MForm> forms, ResourceBundle resource) {
		for (MForm mf : forms) {
		    System.out.println(resource.getString(mf.getLabelKey())+":");
		    List<MInput<?>> mis = mf.getInputs();
		    for (MInput<?> mi : mis) {
		    	System.out.println(resource.getString(mi.getLabelKey()) + " : " + mi.getValue());
		    }
		    System.out.println();
		}
	}
	
	public static void main(String[] args) {
//		Long jid = connection();
//		Long jid = 13l;
//		Long cid = 1609l;
//		submission(jid);
//		checkstatus(jid);
//		stopmission(jid);
//		Connection_describe(cid);
//		Job_describe(jid);
	}

}
