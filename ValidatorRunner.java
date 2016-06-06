package com.acxiom.ppm.incrementaldataloadvalidation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//Comment added 
public class ValidatorRunner {

	public static void main(String[] args)   {
		// TODO Auto-generated method stub

		Properties driverConfiguration =null;		

		for(String arg: args) {
			System.out.println(arg);
		}

		System.out.println("Starting");
		String[] CoulnmNames = null ;
		String Headerkeys = null ;

		try {

			if(args.length != 1)
			{
				System.out.println("Proper Usage is: java program filename");
				System.exit(0);
			}
			Configuration conf= new Configuration();

			File DriverConfigfile = new File(args[0]);
			FileInputStream fileInput = new FileInputStream(DriverConfigfile);
			driverConfiguration = new Properties();
			driverConfiguration.load(fileInput);
			fileInput.close();

			String soruceDWTablesDirLocation = driverConfiguration.getProperty("sourcetable.data.location");
			String TargetDWTablesDirLocation = driverConfiguration.getProperty("sourcetable.data.location");			
			String soruceDWTablesHeaderConFile = driverConfiguration.getProperty("sourcetable.header.configurationfile.path");
			String TargetDWTablesHeaderConFile = driverConfiguration.getProperty("targettable.header.configurationfile.path");


			conf.set("sourcetable.header.configurationfile.path", soruceDWTablesDirLocation);
			conf.set("targettable.header.configurationfile.path", TargetDWTablesDirLocation);		

			Job job = new Job(conf, "DWValidation");

			job.setJarByClass(ValidatorRunner.class);
			job.setMapperClass(ValidatorMapper.class);
			job.setReducerClass(ValidatorReducer.class);		

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);			
			job.setOutputKeyClass(Text.class);	
			job.setOutputValueClass(Text.class);


			FileInputFormat.setInputPaths(job, new Path(soruceDWTablesDirLocation), new Path(TargetDWTablesDirLocation));
			Path outputPath = new Path(args[1]);
			outputPath.getFileSystem(conf).delete(outputPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			System.out.println("submitting");
			boolean b = job.waitForCompletion(true);
			System.out.println("b is "+b);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

}
