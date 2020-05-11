package com.test;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Logger;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

/**
 * This class is a AWS Lambda function which triggers based on PUT and DELETE in the source bucket.
 * 
 */
public class App implements RequestHandler<S3Event, String> {
	
	final static Logger logger = Logger.getLogger(App.class);

	@Override
	public String handleRequest(S3Event s3Event, Context context) {
		logger.info("Lambda function is invoked:" + s3Event.getRecords().toString());

		S3EventNotificationRecord record = s3Event.getRecords().get(0);

		// Retrieve the bucket & key for the uploaded S3 object that
		// caused this Lambda function to be triggered
		String eventSrcBucket = record.getS3().getBucket().getName();

		logger.info("This is the event bucket name " + eventSrcBucket);

		String objectKey = null;
		String key = record.getS3().getObject().getKey().replace('+', ' ');
		try {
			objectKey = URLDecoder.decode(key, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.error(e.getMessage());
		}

		logger.info("This is the event objectKey name " + objectKey);

		String fromBucket = eventSrcBucket;
		String toBucket = "useast2ohiobucket";
		String eventType = record.getEventName();

		logger.info("This is the eventType " + eventType);

		// Create the S3Client object
		Region region = Region.US_EAST_2;
		S3Client s3 = S3Client.builder().region(region).build();

		if (eventType != null && eventType.contains("Put")) {
			CopyBucketObject(s3, fromBucket, objectKey, toBucket);
		}
		else if (eventType != null && eventType.contains("Delete")) {
			DeleteBucketObject(s3, fromBucket, objectKey, toBucket);
		}
		return null;
	}

	public static void DeleteBucketObject(S3Client s3, String fromBucket, String objectKey, String toBucket) {
		logger.info("Deleting object " +objectKey +" in the destination bucket " +toBucket);
		DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(toBucket).key(objectKey).build();
        s3.deleteObject(deleteObjectRequest);
	}

	public static String CopyBucketObject(S3Client s3, String fromBucket, String objectKey, String toBucket) {
		logger.info("Copying object " +objectKey +"fromBucket "+ fromBucket +"toBucket " +toBucket);
		String encodedUrl = null;
		try {
			encodedUrl = URLEncoder.encode(fromBucket + "/" + objectKey, StandardCharsets.UTF_8.toString());
		} catch (UnsupportedEncodingException e) {
			logger.info("URL could not be encoded: " + e.getMessage());
		}
		CopyObjectRequest copyReq = CopyObjectRequest.builder().copySource(encodedUrl).bucket(toBucket).key(objectKey)
				.build();

		try {
			CopyObjectResponse copyRes = s3.copyObject(copyReq);
			return copyRes.copyObjectResult().toString();
		} catch (S3Exception e) {
			logger.error(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
		return "";
	}
}
