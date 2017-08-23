package com.udl;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class Downloader {

    public static void main(String[] args) throws IOException {
        URL url = new URL("http://download.geofabrik.de/europe/great-britain/england/greater-london-latest.osm.bz2");
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.connect();
        InputStream connStream = conn.getInputStream();
        
        Configuration configuration = new Configuration();
        FileSystem hdfs;
		try {
			hdfs = FileSystem.get(new URI("hdfs://udltest3.cs.ucl.ac.uk:8020"), configuration);
			
	        Path filePath = new Path(args[0], "greater-london-latest.osm.bz2");

	        FSDataOutputStream outStream = hdfs.create(filePath);
	        IOUtils.copy(connStream, outStream);

	        outStream.close();
	        connStream.close();
	        conn.disconnect();
	        
	        decompress(filePath);
	        delete(filePath);
			
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
    }
    
    private static void decompress(Path filePath){
        Configuration configuration = new Configuration();
        FileSystem hdfs = null;
        
        CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = factory.getCodec(filePath);
        if (codec == null) {
            System.err.println("No codec found for " + filePath.toString());
            System.exit(1);
        }
        String outputUri = CompressionCodecFactory.removeSuffix(filePath.toString(), codec.getDefaultExtension());
        InputStream inStream = null;
        FSDataOutputStream outStream = null;
        try {
        	hdfs = FileSystem.get(new URI("hdfs://udltest3.cs.ucl.ac.uk:8020"), configuration);
            inStream = codec.createInputStream(hdfs.open(filePath));
            outStream = hdfs.create(new Path(outputUri));
            IOUtils.copy(inStream, outStream);
            
            inStream.close();
            outStream.close();
        } catch (Exception e){
        	e.printStackTrace();
        }
    }
    
    private static void delete(Path filePath){
    	FileSystem hdfs;
    	Configuration configuration = new Configuration();
    	
    	try {
			hdfs = FileSystem.get(new URI("hdfs://udltest3.cs.ucl.ac.uk:8020"), configuration);
			
	        hdfs.delete(filePath, true);
    	} catch (Exception e){
    		e.printStackTrace();
    	}
    	
    	
    }
}