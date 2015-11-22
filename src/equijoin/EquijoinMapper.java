package equijoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EquijoinMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    //hadoop supported data types
   // private final static IntWritable line1 = new IntWritable();
    private String val = new String();
   
    //map method that performs the tokenizer job and framing the initial key value pairs
    // after all lines are converted into key-value pairs, reducer is called.
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
          //taking one line at a time from input file and tokenizing the same
          String line = value.toString();
          String arr[]=line.split(",");
          val=arr[1];
          System.out.println("----------------------"+val);
          output.collect(new Text(val), new Text(line));
     }
}


