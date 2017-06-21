package com.twilio.segmentcloning;

import java.util.Arrays;

import org.apache.http.impl.client.HttpClientBuilder;

import com.twilio.http.HttpClient;
import com.twilio.http.NetworkHttpClient;
import com.twilio.http.Request;
import com.twilio.http.Response;

public class ThrottledNetworkHttpClient extends HttpClient {
	
	private static NetworkHttpClient client; 
	
	public ThrottledNetworkHttpClient(HttpClientBuilder clientBuilder){
		client = new NetworkHttpClient(clientBuilder);
	}

	@Override
	public Response reliableRequest(Request request, int[] retryCodes, int retries, long delayMillis) {
		int[] augmentedRetryCodes = Arrays.copyOf(retryCodes, retryCodes.length +1);
		//Add 429 such that is retried.
		augmentedRetryCodes[augmentedRetryCodes.length-1] = 429;
		Response response = null;
        while (retries > 0) {
            response = makeRequest(request);

            if (!shouldRetry(response, augmentedRetryCodes)) {
                return response;
            }

            try {
            	long delayCoefficient = 1L; 
            	//If service asks us to slow down we use exponential back-off starting with 2x of default delay.
            	if (response.getStatusCode() == 429) {
            		delayCoefficient = Math.round(Math.pow(2, (RETRIES - retries + 1)));
            	}
            	System.out.println(Thread.currentThread().getName() + " Waiting for: " + delayMillis *  delayCoefficient);
                Thread.sleep(delayMillis *  delayCoefficient);
            } catch (final InterruptedException e) {
                // Delay failed, continue
            }

            // Decrement retries
            retries--;
        }
        return response;
	}
	
	@Override
	public Response makeRequest(Request request) {
		return client.makeRequest(request);
	}

}
