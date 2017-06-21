package com.twilio.segmentcloning;

import com.twilio.rest.notify.v1.service.user.SegmentMembership;

class AddToSegmentTask implements Runnable {  
    private String service;
    private String identity;
    private String segment;
    
    public AddToSegmentTask(String service, String identity, String segment){
    	this.service=service;
    	this.identity=identity;
    	this.segment=segment;
    }
    
     public void run() {  
    	 System.out.println(Thread.currentThread().getName()+" (Start) Adding to Segment: "+ identity + ", segment: " + segment);  
        long starttime = System.currentTimeMillis();
        try { 
        	
			SegmentMembership.creator(service, identity, segment).create();

        } catch (Exception e) {
			System.out.println(Thread.currentThread() + "Throwing exception");
        	e.printStackTrace();
		}
        long endtime = System.currentTimeMillis();
    	System.out.println(Thread.currentThread().getName()+" (End) Adding to Segment: "+ identity + ", segment: " + segment + " latency: " + (endtime-starttime));  
    }  
}  