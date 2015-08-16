package com.mqttkafkaBridge;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class Simulator{
	public static String timestamp;
	public static void main(String arg[]) throws InterruptedException
	{
		final kafka.javaapi.producer.Producer<Integer, String> producer;
		final String topic ="test";
		final Properties props = new Properties();
	    props.put("serializer.class", "kafka.serializer.StringEncoder");
	     props.put("metadata.broker.list", "localhost:9092");
	    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
	    int messageNo = 0; 
		System.out.println(" Simulator started...");
		int j=0;
		Date d =new Date();
		while(true)
		{
			messageNo++;			
			String tagid[]={"pratick","1001"};
			String readercode[]={"pratick","2","3"};
			int idx = new Random().nextInt(tagid.length);			
			String randomunitid = (tagid[idx]);
			int readeridx = new Random().nextInt(readercode.length);			
			String randomredercode = (readercode[readeridx]);
			
		    String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(d);
		    
		    long startTime = System.nanoTime();
		    Random t = new Random();
		    String data="deviceid="+randomredercode+",value="+messageNo+",date="+timestamp;
		    try
		    {
		    producer.send(new KeyedMessage<Integer, String>(topic, data));
		    System.out.println("prodcued...Topic.."+topic+"----"+messageNo+"......"+data);	
		    }
		    catch(Exception e)
		    {
		    System.out.println("Unable To publish into Kafaka Broker"+props);	
		    }
			Date d1 =new Date();			
			String timestamp1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(d1);
			Thread.sleep(100);	
			j++;
			if(messageNo==10000)
			{
			System.out.println("Start time:"+timestamp+"End time:"+timestamp1);
			System.exit(0);
			
			}
		}
		
	   
	}
	
}
