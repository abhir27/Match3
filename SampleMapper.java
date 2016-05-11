
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SampleMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
			String[] val=value.toString().split("\t");
  	  int v=0;String line;
  	    Path pt1=new Path("hdfs://localhost:9000/citr.txt");
  	    FileSystem fs1 = FileSystem.get(new Configuration());
  	    BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
  	        line=br1.readLine();
  	    while (line != null){
  	    	v=Integer.parseInt(line);
  	            line=br1.readLine();
  	    }
System.out.println("v: "+v);
  	if(Integer.parseInt(val[0])==v)
  	{
  	output.write(new IntWritable(Integer.parseInt(val[0])),new Text(val[1]+"\t"+val[2]+"\t"+val[3]));
  	}
    	  }
}

