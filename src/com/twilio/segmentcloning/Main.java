package com.twilio.segmentcloning;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.twilio.Twilio;
import com.twilio.base.Page;
import com.twilio.http.HttpClient;
import com.twilio.http.TwilioRestClient;
import com.twilio.rest.notify.v1.Service;
import com.twilio.rest.notify.v1.service.User;
import com.twilio.rest.notify.v1.service.UserReader;
import com.twilio.rest.notify.v1.service.user.SegmentMembership;

public class Main {
	
	 public static final String ACCOUNT_SID = "";
	    public static final String AUTH_TOKEN = "";
	    
	    public static final int SEGMENT_SIZE = 500;
	    
	    public static final String ORIGINAL_SEGMENT = "original segment";
	    public static final String CLONED_SEGMENT = "cloned segment";
	    
	    public static final int CONNECTION_POOL_SIZE =10;
	    
	    private static final int CONNECTION_TIMEOUT = 10000;
	    private static final int SOCKET_TIMEOUT = 30500;

	    public static void main(String[] args) {
	    	
	    	
	    	initTwilio();	        	        
	        
	        Service service = Service.creator().setFriendlyName("DeleteMe").create();	
	        System.out.println("SERVICE SID: " + service.getSid());
	        

	        //Create SEGMENT_SIZE number of new Users in ORIGINAL_SEGMENT
	        createSegment(service); 
	        
	     	        
	        //Clone Users in ORIGINAL_SEGMENT to CLONED_SEGMENT
	        cloneSegment(service); 
	        
	        Service.deleter(service.getSid()).delete();
	    }

		public static void createSegment(Service service) {
			long starttime = System.currentTimeMillis();
			
			List<ListenableFuture<User>> futureList = new LinkedList<ListenableFuture<User>>();
	        for (int i = 0; i < SEGMENT_SIZE; i++) {
	        	String identity = "User"+i;
	        	String segment = ORIGINAL_SEGMENT;
  
	              try {	          		
	          		futureList.add(User.creator(service.getSid(), identity).setSegment(segment).createAsync());	          		
	              } catch (Exception e) {
	      			e.printStackTrace();
	      		}
			}
	        
	        ListenableFuture<List<User>>listFuture = Futures.allAsList(futureList);
	        
	        try {
	        	//Test with large User set, not sure how this uses memory when 500K users are returned.
	        	//Although you would not need this step in production anyway
				listFuture.get();
			    long endtime = System.currentTimeMillis();
				System.out.println("Finished all threads"); 	        
				System.out.println("Total execution time: " + (endtime-starttime));
				System.out.println("Average rate: " + SEGMENT_SIZE*1000/(endtime-starttime));
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

		public static void cloneSegment(Service service) {
			long startTime = System.currentTimeMillis();

			UserReader reader = User.reader(service.getSid());
			
			int i = 0;
			for (Page<User> page = reader.firstPage();page.hasNextPage(); page =reader.nextPage(page)){
				long pageStartTime = System.currentTimeMillis();
				List<User> userList = page.getRecords();
				
				List<ListenableFuture<SegmentMembership>> futureList = new LinkedList<ListenableFuture<SegmentMembership>>();
				
				for (User user : userList) {
		        	String segment = CLONED_SEGMENT;
					futureList.add(SegmentMembership.creator(service.getSid(), user.getIdentity(), segment).createAsync());
				}
				
				ListenableFuture<List<SegmentMembership>>listFuture = Futures.allAsList(futureList);
				try {
					//This should be OK, given that default page size is 50
					listFuture.get();
					long pageEndTime = System.currentTimeMillis();
			        System.out.println("Finished page " + i); 	        
			        System.out.println("Total execution time: " + (pageEndTime-pageStartTime));
			        System.out.println("Average rate: " + page.getPageSize()*1000/(pageEndTime-pageStartTime));
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
				i++;
			}
			long endTime = System.currentTimeMillis();
			System.out.println("Finished all pages"); 	        
	        System.out.println("Total execution time: " + (endTime-startTime));
	        System.out.println("Effective rate (including reads): " + SEGMENT_SIZE*1000/(endTime-startTime));
			
		}

		public static void initTwilio() {
			RequestConfig config = RequestConfig.custom()
	                .setConnectTimeout(CONNECTION_TIMEOUT)
	                .setSocketTimeout(SOCKET_TIMEOUT)
	                .build();
	    	
	    	PoolingHttpClientConnectionManager conManager = new PoolingHttpClientConnectionManager();
	    	conManager.setDefaultMaxPerRoute(CONNECTION_POOL_SIZE);
	    	conManager.setMaxTotal(CONNECTION_POOL_SIZE*2);
	    	
	    	HttpClient httpClient = new ThrottledNetworkHttpClient(HttpClientBuilder.create()
            .useSystemProperties()
            .setConnectionManager(conManager)
            .setDefaultRequestConfig(config));
	    	
	    	// Initialize the client
	        Twilio.init(ACCOUNT_SID, AUTH_TOKEN);
	        Twilio.setRestClient(new TwilioRestClient.Builder(ACCOUNT_SID, AUTH_TOKEN).accountSid(ACCOUNT_SID).httpClient(httpClient).build());
		}

}
