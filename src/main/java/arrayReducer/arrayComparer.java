package arrayReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.*;

import arrayReducer.arrayComparer.similarityValueMapper.similarityOrderReducer;
import mapreducedemo.mapreducedemo.WordCounter;
import mapreducedemo.mapreducedemo.WordCounter.TokenizeMapper;
import mapreducedemo.mapreducedemo.WordCounter.TokenizeMapper.SumReducer;

public class arrayComparer {
	public static class RoutePointObject {
		int lengthRP;
		String longitude;
		String latitude; 
		
		public void RoutepPointObject(int lengthRP, String latitude, String longitude) {
			this.lengthRP= lengthRP; 
			this.longitude= longitude; 
			this.latitude = latitude;
		}
	}
public static class similarityValueMapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String [] item_list = value.toString().split(";");
			String[] userRoute= item_list[0].replace("[","").replace("]","").split(",");
			String[] compareRoute= item_list[1].replace("[","").replace("]","").split(",");
			String id = item_list[2];

			Integer[] ur= new Integer[userRoute.length];
			for(int i = 0 ; i<userRoute.length; i++) {
				ur[i] = new Integer (userRoute[i]);
			}
			int count=0;
			for(int i = 0 ; i<ur.length; i++) {
				if(compareRoute.length>i) {
					Integer cr = new Integer (compareRoute[i]);
					if( cr.compareTo(ur[i])==0) {
						count++;
					}
				}
			}
			
			
			//int []comparedRoute= Arrays.stream(item_list[1].split(",")).mapToInt(Integer::parseInt).toArray();
		////	String id = item_list[2];
			//Double rand = (Double) (Math.random());
			Text wordOut = new Text("Id: "+id); 
			Text one = new Text("Matches: "+((Integer)count).toString());
			context.write(wordOut, one);
			
		}
		
		public static class similarityOrderReducer extends Reducer <Text, Text, Text, Text> {
			public void reduce(Text term, Text ones, Context context) throws IOException, InterruptedException {
				
				context.write(term, ones);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser (conf, args).getRemainingArgs();
		
		if(otherArgs.length!= 2) {
			System.err.println("Usage: ArrComp <input_file> <output_directory>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(arrayComparer.class);
		job.setMapperClass(similarityValueMapper.class);
		job.setReducerClass(similarityOrderReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean status = job.waitForCompletion(true);
		if(status) {
			System.out.println("ok");
			System.exit(0);
		}else {
			System.err.println("npüe");
			System.exit(1);
		
		}
		 String value = "[1,2,3,4,5,6,7,8,9];[0,2];1";
		System.out.println(value.toString().split(";")[0].toString());
		System.out.println(new Text("hihi"));
		
		
	}
}
