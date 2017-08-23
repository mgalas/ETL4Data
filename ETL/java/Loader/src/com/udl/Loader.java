package com.udl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import com.cloudera.com.amazonaws.util.json.JSONException;
import com.cloudera.com.amazonaws.util.json.JSONObject;

public class Loader {
	
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
    	Configuration configuration = new Configuration();
        
        Path audit_path = new Path(args[0],"000000_0");
        Path stats_path = new Path(args[1],"000000_0");
        
        
        List<String> text = new ArrayList<>();

        try{
        	FileSystem hdfs = FileSystem.get( new URI("hdfs://udltest3.cs.ucl.ac.uk:8020"), configuration );
            BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(audit_path)));
            String line;
            line=br.readLine();
            while (line != null){
                text.add(line);
                line=br.readLine();
            }
                    
        }catch(Exception e){
            e.printStackTrace();
        }
        
        try{
        	FileSystem hdfs = FileSystem.get( new URI("hdfs://udltest3.cs.ucl.ac.uk:8020"), configuration );
            BufferedReader br=new BufferedReader(new InputStreamReader(hdfs.open(stats_path)));
            String line;
            line=br.readLine();
            while (line != null){
                text.add(line);
                line=br.readLine();
            }
                    
        }catch(Exception e){
            e.printStackTrace();
        }
        
        JSONObject json = new JSONObject();
        JSONObject extras = new JSONObject();
        
        try {
        	
        	for(String s : text){
        		String[] parts = s.split(",");
        		json.put(parts[0], parts[1]);
        	}
        	
        	extras.put("extras", json);
        	System.out.println(extras.toString());
        	
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    

        //CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		HttpClient httpClient = new DefaultHttpClient();

        try {
            HttpPost request = new HttpPost("http://udltest1.cs.ucl.ac.uk/api/rest/dataset/osm-planet");
            StringEntity params = new StringEntity(extras.toString());
            request.addHeader("Authorization", "8073ea5f-ce3a-4806-b3d3-f27286d9d923");
            request.setEntity(params);
            //httpClient.execute((HttpUriRequest) request);
            
            HttpResponse response = httpClient.execute(request);
            int status = response.getStatusLine().getStatusCode();
            System.out.println("status code is :" + status);
            if (status != 401) {
                if (status != 409) {
                    BufferedReader rd = new BufferedReader(
                            new InputStreamReader(response.getEntity()
                                    .getContent()));
                    
                    StringBuilder total = new StringBuilder();

                    String line = null;

                    while ((line = rd.readLine()) != null) {
                       total.append(line);
                    }
                    rd.close();
                    System.out.println(total.toString());     
                }
            }
            
            
            
            
            
        // handle response here...
        } catch (Exception ex) {
            // handle exception here
        } finally {
        	
        }
    }

}
