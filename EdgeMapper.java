
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EdgeMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	public static int edges=1;
    @Override
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
    	edges++;
    	String[] val=value.toString().split(",");
    	output.write(new IntWritable(Integer.parseInt(val[0])),new Text(val[1]+"\t"+val[2]));
    	output.write(new IntWritable(Integer.parseInt(val[1])),new Text(val[0]+"\t"+val[2]));
    	
    	//System.out.println(edges);
          }
}

