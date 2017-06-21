package com.twilio.segmentcloning;

import com.twilio.rest.notify.v1.service.User;

public class CreateUserTask implements Runnable {  
    private String service;
    private String identity;
    private String segment;
    
    public CreateUserTask(String service, String identity, String segment){
    	this.service=service;
    	this.identity=identity;
    	this.segment=segment;
    }
    
     public void run() {  
        System.out.println(Thread.currentThread().getName()+" (Start) Create user: "+ identity + ", segment: " + segment);  
        long starttime = System.currentTimeMillis();
        try {
    		
    		User user = User.creator(service, identity).setSegment(segment).create();

        } catch (Exception e) {
			e.printStackTrace();
		}
        long endtime = System.currentTimeMillis();
    	System.out.println(Thread.currentThread().getName()+" (End) Create user: "+ identity + ", segment: " + segment + " latency: " + (endtime-starttime));  
    }  
}  