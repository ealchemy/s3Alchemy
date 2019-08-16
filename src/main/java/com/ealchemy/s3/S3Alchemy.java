package com.ealchemy.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3Alchemy {
    private String bucketName;
    private String region;

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public List<String> uploadFolder(String key, String folderPath) throws IOException {
        List<String> urls = new ArrayList<>();
        File folder = new File(folderPath);
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles) {
            if (file.isFile()) {
                urls.add(uploadFile( key, file));
            }
        }
        return urls;
    }

    public String uploadFile(String key, String filename) throws IOException {
        File file = new File(filename);
        return uploadFile(key, file);
    }

    public String uploadFile(String key, File file) throws IOException {
        AmazonS3 s3Client = getS3Client();
        String s3URL = "";
        String s3Key = "";
        try {
            if (key.isEmpty()) {
                s3Key=file.getName();
            }else{
                s3Key = key + "/" + file.getName();
            }
            boolean fileExists=s3Client.doesObjectExist(bucketName, s3Key);
            if (!fileExists) {
                PutObjectResult result = s3Client.putObject(
                        new PutObjectRequest(
                                bucketName,
                                s3Key,
                                file).withCannedAcl(CannedAccessControlList.PublicRead));
            }

            s3URL=s3Client.getUrl(bucketName, s3Key).toString();

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which " +
                    "means your request made it " +
                    "to Amazon S3, but was rejected with an error response" +
                    " for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        }
        return s3URL;
    }

    public Map<String, String> getFiles(String prefix){
        HashMap<String, String> map = new HashMap<>();
        AmazonS3 s3Client = getS3Client();
        ListObjectsRequest req = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(prefix);

        ObjectListing objectListing = s3Client.listObjects(req);
        for (S3ObjectSummary obj:objectListing.getObjectSummaries()){
            map.put(obj.getKey(), s3Client.getUrl(bucketName, obj.getKey()).toString());
        }
        return map;
    }


    public List<Label> getLabelsTags(String bucketName, String s3URL){
        List<Label> labels = new ArrayList<>();
        DetectLabelsRequest request = new DetectLabelsRequest();
        Image image = new Image();
        image.setS3Object(getS3RekognitionObject(bucketName, s3URL));
        request.setImage(image);
        request.setMinConfidence(75F);
        request.setMaxLabels(10);

        AmazonRekognition amazonRekognition = getAmazonRekognitionClient();

        try {
            DetectLabelsResult result = amazonRekognition.detectLabels(request);
            labels = result.getLabels();


        } catch(AmazonRekognitionException e) {
            e.printStackTrace();
        }
        return labels;
    }

    public List<Celebrity> getCelebrityTags(String bucketName, String s3URL){
        AmazonS3 s3Client = getS3Client();
        AmazonRekognition amazonRekognition = getAmazonRekognitionClient();
        Image image = new Image();
        image.setS3Object(getS3RekognitionObject(bucketName, s3URL));
        RecognizeCelebritiesRequest request = new RecognizeCelebritiesRequest()
                .withImage(image);

        RecognizeCelebritiesResult result =
                amazonRekognition.recognizeCelebrities(request);
        List<Celebrity> celebrityList = result.getCelebrityFaces();
        return celebrityList;
    }

    private S3Object
    getS3RekognitionObject(String bucketName, String s3URL){
        S3Object s3Object = new S3Object();
        s3Object.setBucket(bucketName);
        s3Object.setName(s3URL);

        return s3Object;
    }

    private AmazonS3 getS3Client(){
        AmazonS3 s3Client = null;
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .build();

        return s3Client;
    }



    private AmazonRekognition getAmazonRekognitionClient(){
        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder
                .standard()
                .withRegion(region)
                .build();
        return rekognitionClient;
    }
}
