package arrayObjReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.*;

import arrayReducer.arrayComparer.similarityValueMapper.similarityOrderReducer;


public class ArrayObjComparer {
	public static class RoutePointObject {
		int lengthRP;
		String longitude;
		String latitude; 
	}
	
	public static class RouteCompareObj {
		RoutePointObject[] userRoute; 
		RoutePointObject[] matchSearchRoute;
		int matchSearchRouteId; 
	}
public static class similarityValueMapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Gson gson = new Gson();
		//	String [] item_list = value.toString().split(";"); 
			try {
			TaskID mapid = context.getTaskAttemptID().getTaskID() ;
			//String s =  "[{'lengthRP': 101, 'longitude': '7.63291', 'latitude': '7.63291'},{'lengthRP': 102, 'longitude': '7.63292', 'latitude': '7.63292'},{'lengthRP': 103, 'longitude': '7.63293', 'latitude': '7.63293'}]";
					
			
			RouteCompareObj rps = gson.fromJson(value.toString(), RouteCompareObj.class);
			/* String[] userRoute= item_list[0].replace("[","").replace("]","").split(",");
			String[] compareRoute= item_list[1].replace("[","").replace("]","").split(",");*/
			// String id = item_list[2];
/*
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
			}*/ 
			
			
			//int []comparedRoute= Arrays.stream(item_list[1].split(",")).mapToInt(Integer::parseInt).toArray();
			int id = rps.matchSearchRouteId;
			//Double rand = (Double) (Math.random());
			Text wordOut = new Text("Id: "+ id + " mapId:" + mapid); 
			Text one = new Text("Length: "+ ((Integer) rps.userRoute[0].lengthRP).toString());
			context.write(wordOut, one);}
			catch(Exception e) {
				Text wordOut = new Text("err"); 
				Text one = new Text(e.toString());
				context.write(wordOut, one);
			}
			
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
			System.err.println("Usage: AOC <input_file> <output_directory>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(ArrayObjComparer.class);
		job.setMapperClass(similarityValueMapper.class);
		job.setReducerClass(similarityOrderReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"222"));
		boolean status = job.waitForCompletion(true);
		if(status) {
			System.out.println("ok");
			System.exit(0);
		}else {
			System.err.println("nope");
			System.exit(1);
		
		}
		 String value = "[1,2,3,4,5,6,7,8,9];[0,2];1";
		System.out.println(value.toString().split(";")[0].toString());
		System.out.println(new Text("hihi"));
		
		
	}
}
