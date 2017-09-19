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

public class TempSort1
{
    public static class TempMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
            {
            //int i=0;                
            String str = value.toString();                        
            if(!str.contains(","))
            {
                //for (String s : strList)
                //i=str.getIndex();                    
                String temp=str.substring(26,32).trim();
                double t=Double.parseDouble(temp);  
		if(t >= 0.17)	              
                context.write(new DoubleWritable(t), new Text(str));
                }
            }
      }
  
      public static class TempReducer extends Reducer<Text, Text, DoubleWritable, Text>
    {
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
                        
            //int count = 0;
            for (Text val : values)
                //count += Integer.parseInt(val.toString());
                context.write(key, new Text(val));
            }
    }

//Partitioner class
    
   public static class TempPartitioner extends Partitioner < DoubleWritable, Text >
   {
      @Override
      public int getPartition(DoubleWritable key, Text value, int numReduceTasks)
      {
         //String[] str = value.toString().split(" ");
         double temp = Double.parseDouble(key.toString());
         
      	 double maxTemp=999.99f;
      	 double minTemp=0.17f;


	 //double maxLoad=(maxTemp-minTemp)/numReduceTasks;
	 
	 //System.out.println("minLoad in partitioner:"+minTemp);
	 //System.out.println("maxtemp in partitioner:"+maxTemp);
	// System.out.println("maxLoad in partitioner:"+maxLoad);
	 if(numReduceTasks == 0)
         {
            return 0;
         }
	 if(temp>=minTemp && temp<(maxTemp/5))
         {
            return 0;
         }
         
         if(temp>= (maxTemp/5) && temp<(maxTemp/3))
         {
            return 0;
         }
         if(temp>=(maxTemp/3) && temp<=(maxTemp))
         {
            return 1;// % numReduceTasks;
         }

	return 0;	
		
      }
   }

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job j1 = new Job(conf);

		j1.setJobName("Temp job");
		j1.setJarByClass(TempSort1.class);

		//Mapper output key and value type
		j1.setMapOutputKeyClass(DoubleWritable.class);
		j1.setMapOutputValueClass(Text.class);

		//Reducer output key and value type
		j1.setOutputKeyClass(Text.class);
		j1.setOutputValueClass(Text.class);

		//file input and output of the whole program
		j1.setInputFormatClass(TextInputFormat.class);
		j1.setOutputFormatClass(DoubleWritable.class);
		
		//Set the mapper class
		j1.setMapperClass(TempMapper.class);
		
		//set the partitioner class for custom partitioner
		j1.setPartitionerClass(TempPartitioner.class);
		//set the combiner class for custom combiner
//		j1.setCombinerClass(LogReducer.class);

		//Set the reducer class
		j1.setReducerClass(TempReducer.class);

		//set the number of reducer if it is zero means there is no reducer
		j1.setNumReduceTasks(2);


 	}
}

