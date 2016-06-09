package com.acxiom.ppm.incrementaldataloadvalidation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//Comment added 
public class ValidatorMapper extends Mapper<LongWritable, Text, Text, Text >{
	private String sourceHeaderFile ;
	private String targetHeaderFile ;
	private String targetHiveTableName;
	private static final Map<String, String> headerLocationWithTableNames = new HashMap<String, String>();

	Properties targetDWConfiguration=null;
	Properties sourceDWConfiguration=null;
	static Properties properties=null;
	String currentTablename=null;

	Text KeyOut = new Text();
	Text ValueOut = new Text();
	@Override
	protected void setup(Context context){

		Configuration conf = context.getConfiguration();

		sourceHeaderFile = conf.get("sourcetable.header.configurationfile.path");
		targetHeaderFile = conf.get("targettable.header.configurationfile.path");

		targetDWConfiguration = new Properties();
		sourceDWConfiguration = new Properties();		
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);


			if (sourceHeaderFile != null) {			
				sourceDWConfiguration = ValidatorMapper.loadProperties(sourceHeaderFile, fs);
				for (Map.Entry<Object, Object> e : sourceDWConfiguration.entrySet()) {
					String key = (String) e.getKey();
					String value = (String) e.getValue();				  
					headerLocationWithTableNames.put(key, value);					 
				}
			}

			if (targetHeaderFile != null) {			
				targetDWConfiguration = ValidatorMapper.loadProperties(targetHeaderFile, fs);
				for (Map.Entry<Object, Object> e : targetDWConfiguration.entrySet()) {
					targetHiveTableName = (String) e.getKey();
					String key = (String) e.getKey();
					String value = (String) e.getValue();				  
					headerLocationWithTableNames.put(key, value);					 
				}
			}		

			String inputSplitfileName = ((FileSplit) context.getInputSplit()).getPath().getName();		

			for (String key : headerLocationWithTableNames.keySet()) {
				if(key.contains(inputSplitfileName)){
					currentTablename = key;
				}
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}  


	}

	@Override
	public void map(LongWritable key, Text Value, Context context) throws IOException, InterruptedException{

		String Line1 = Value.toString();


		StringBuilder sb = new StringBuilder();
		String[] keyVeluHolder = Value.toString().split("\\t",2);

		sb.append(currentTablename);
		sb.append(":");
		sb.append(keyVeluHolder[2]);

		KeyOut.set(keyVeluHolder[1]);
		ValueOut.set(sb.toString());
		context.write( KeyOut, ValueOut );

	}


	public static  Properties loadProperties(String propsFile, FileSystem fs) {
		try {
			if (properties == null) {
				properties = new Properties();
				Path inFile = new Path(propsFile);
				FSDataInputStream fis = fs.open(inFile);

				//FileInputStream fis = new FileInputStream(propsFile);
				properties.load(fis);
			}
		} catch (IOException e) {
			String msg = "Error reading the properties file "+propsFile+" make sure the properties file exists "
					+ "with proper permissions and try again";
			e.printStackTrace();
			//log.error(msg);
			//throw new Exception(msg, e);
		}
		return properties;
	}

}

