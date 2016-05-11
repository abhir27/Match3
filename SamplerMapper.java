
import java.io.IOException;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SamplerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	@Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
		String[] val=value.toString().split("\t");
  	output.write(new IntWritable(Integer.parseInt(val[0])),new Text(val[1]+"\t"+val[2]+"\t"+val[3]));
  	
    	  }
}

