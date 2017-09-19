import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class TempSort
{
	public static class TempMapper extends Mapper<Object, Text, DoubleWritable, Text>
	{
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	    	{
			String str = value.toString();
			String strList = str.substring(25,33);
			
			Double tmp=null;
			int count;
			
			
			if(!str.startsWith("STN---")){			
			try {
			   strList.replaceAll("\\s+","");
			   tmp = Double.parseDouble(strList);
			   //context.write( FloatWritable(tmp), new Text(str));	
				if(tmp >= 0.17)				
				context.write( new DoubleWritable(tmp), new Text(str));				
			} catch (NumberFormatException e) {
    
			}
			}
			    			
    		}
  	}
  
  	public static class TempReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text>
	{
		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			
			for (Text val : values)
			context.write(key, new Text(val));
	    	}
	}


  	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job j1 = new Job(conf);

		Path inputPath = new Path(args[0]);
        	Path stageOutputPath = new Path(args[1]);
        	Path partitionerFile = new Path(args[2]);
        	Path sortedOutputPath = new Path(args[3]);		

		//Path partitionerFile = new Path(args[2]);
		j1.setJobName("Temp Sort job");
		j1.setJarByClass(TempSort.class);

		//Mapper output key and value type
		j1.setMapOutputKeyClass(DoubleWritable.class);
		j1.setMapOutputValueClass(Text.class);

		//Reducer output key and value type
		j1.setOutputKeyClass(DoubleWritable.class);
		j1.setOutputValueClass(Text.class);

 		//j1.setPartitionerClass(TemperaturePartitioner.class);

		//file input and output of the whole program
		j1.setInputFormatClass(TextInputFormat.class);
		j1.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//Set the mapper class
		j1.setMapperClass(TempMapper.class);

		//set the combiner class for custom combiner
		//j1.setCombinerClass(WordReducer.class);

		//Set the reducer class
		//j1.setReducerClass(TemperatureReducer.class);

		//set the number of reducer if it is zero means there is no reducer
		j1.setNumReduceTasks(0);
		
		
		FileOutputFormat.setOutputPath(j1, new Path(args[1]));	

		FileInputFormat.addInputPath(j1, new Path(args[0]));

			
		int code = j1.waitForCompletion(true) ? 0 : 1;

		if(code == 0) {
			
		Job j2 = new Job(conf, "Sorting Job");

		j2.setJarByClass(TempSort.class);
		j2.setInputFormatClass(SequenceFileInputFormat.class);
            	SequenceFileInputFormat.setInputPaths(j2, stageOutputPath);
            	TextOutputFormat.setOutputPath(j2, sortedOutputPath);
		
		//Mapper output key and value type
		j2.setMapOutputKeyClass(DoubleWritable.class);
		j2.setMapOutputValueClass(Text.class);

		//Reducer output key and value type
		j2.setOutputKeyClass(DoubleWritable.class);
		j2.setOutputValueClass(Text.class);

 		//j1.setPartitionerClass(TemperaturePartitioner.class);

		//file input and output of the whole program
		
		j2.setOutputFormatClass(TextOutputFormat.class);

		            //mapper settings
            j2.setMapperClass(TempMapper.class);
            
            
 
            //reducer settings
            j2.setNumReduceTasks(3);
            j2.setReducerClass(TempReducer.class);
		
	    TotalOrderPartitioner.setPartitionFile(j2.getConfiguration(), partitionerFile);
            InputSampler.Sampler<DoubleWritable,Text> inputSampler = new InputSampler.RandomSampler<DoubleWritable,Text>(.01, 10000);
            InputSampler.writePartitionFile(j2, inputSampler);
            j2.setPartitionerClass(TotalOrderPartitioner.class);
            j2.waitForCompletion(true);  
		}
	        //deleting first mapper output and partitioner file
        FileSystem.get(new Configuration()).delete(partitionerFile, false);
        FileSystem.get(new Configuration()).delete(stageOutputPath, true);

 	}
}
