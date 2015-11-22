package equijoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Equijoinreducer extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text>
{
    //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
    public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException
    {
        ArrayList<String> table_r = new ArrayList<String>();
        ArrayList<String> table_s = new ArrayList<String>();
      while (values.hasNext())
      {
         //System.out.println("--------------values"+values.next());
    	// System.out.println(res); 
    	  String curr_row = values.next().toString();
    	  System.out.println(curr_row);
    	  String[] tokens = curr_row.split(","); 
		  if(tokens[0].trim().equals("R")){
			  System.out.println("R ==> "+tokens[0].trim());
			  table_r.add(curr_row);}
		  else{
			  System.out.println("s ==> "+tokens[0].trim());
			  table_s.add(curr_row);}
		  
    	 // res += values.next();    
      }
      
      
      for(int i=0; i<table_r.size(); i++){
    	  for (int j=0; j<table_s.size(); j++){
    		  String res = table_r.get(i)+", "+table_s.get(j);
    		  output.collect(NullWritable.get(), new Text(res));
    	  }
      }
      
    }
}
