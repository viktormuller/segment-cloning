package com.twilio.segmentcloning;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.twilio.Twilio;
import com.twilio.base.Page;
import com.twilio.http.HttpClient;
import com.twilio.http.TwilioRestClient;
import com.twilio.rest.notify.v1.Service;
import com.twilio.rest.notify.v1.service.User;
import com.twilio.rest.notify.v1.service.UserReader;

public class Main {
	
	 public static final String ACCOUNT_SID = "ACddd55023e95e245f47506b888118f8f8";
	    public static final String AUTH_TOKEN = "b0774424b22af6e413c7c812daa771f5";
	    
	    public static final int SEGMENT_SIZE = 500;
	    
	    public static final String ORIGINAL_SEGMENT = "original segment";
	    public static final String CLONED_SEGMENT = "cloned segment";
	    
	    public static final int THREAD_POOL_SIZE =10;
	    
	    private static final int CONNECTION_TIMEOUT = 10000;
	    private static final int SOCKET_TIMEOUT = 30500;

	    public static void main(String[] args) {
	    	
	    	
	    	initTwilio();	        	        
	        
	        Service service = Service.creator().setFriendlyName("DeleteMe").create();	
	        System.out.println("SERVICE SID: " + service.getSid());
	        
	        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

	        //Create SEGMENT_SIZE number of new Users in ORIGINAL_SEGMENT
	        createSegment(service, executor); 
	        
	        executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
	        
	        //Clone Users in ORIGINAL_SEGMENT to CLONED_SEGMENT
	        cloneSegment(service, executor); 
	        
	        Service.deleter(service.getSid()).delete();
	    }

		public static void createSegment(Service service, ExecutorService executor) {
			long starttime = System.currentTimeMillis();
	        for (int i = 0; i < SEGMENT_SIZE; i++) {
	        	Runnable worker = new CreateUserTask(service.getSid(), "User" + i, ORIGINAL_SEGMENT );
	        	executor.execute(worker);	        	
			}
	        
	        executor.shutdown();
	        
	        try {
				executor.awaitTermination(10, TimeUnit.SECONDS);
				long endtime = System.currentTimeMillis();
		        System.out.println("Finished all threads"); 	        
		        System.out.println("Total execution time: " + (endtime-starttime));
		        System.out.println("Average rate: " + SEGMENT_SIZE*1000/(endtime-starttime));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public static void cloneSegment(Service service, ExecutorService executor) {
			long starttime;
			starttime = System.currentTimeMillis();

			UserReader reader = User.reader(service.getSid());
			
			for (Page<User> page = reader.firstPage();page.hasNextPage(); page =reader.nextPage(page)){
				
				List<User> userList = page.getRecords();

				for (User user : userList) {
					Runnable worker = new AddToSegmentTask(service.getSid(), user.getIdentity(), CLONED_SEGMENT );
					executor.execute(worker);
				}
						
			}

	        executor.shutdown();
	        
	        try {
				executor.awaitTermination(10, TimeUnit.SECONDS);
				long endtime = System.currentTimeMillis();
		        System.out.println("Finished all threads"); 	        
		        System.out.println("Total execution time: " + (endtime-starttime));
		        System.out.println("Average rate: " + SEGMENT_SIZE*1000/(endtime-starttime));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		public static void initTwilio() {
			RequestConfig config = RequestConfig.custom()
	                .setConnectTimeout(CONNECTION_TIMEOUT)
	                .setSocketTimeout(SOCKET_TIMEOUT)
	                .build();
	    	
	    	PoolingHttpClientConnectionManager conManager = new PoolingHttpClientConnectionManager();
	    	conManager.setDefaultMaxPerRoute(THREAD_POOL_SIZE);
	    	conManager.setMaxTotal(THREAD_POOL_SIZE*2);
	    	
	    	HttpClient httpClient = new ThrottledNetworkHttpClient(HttpClientBuilder.create()
            .useSystemProperties()
            .setConnectionManager(conManager)
            .setDefaultRequestConfig(config));
	    	
	    	// Initialize the client
	        Twilio.init(ACCOUNT_SID, AUTH_TOKEN);
	        Twilio.setRestClient(new TwilioRestClient.Builder(ACCOUNT_SID, AUTH_TOKEN).accountSid(ACCOUNT_SID).httpClient(httpClient).build());
		}

}
