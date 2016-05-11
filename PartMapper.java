
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	@Override
	
    public void map(LongWritable key, Text value, Context output) throws IOException,
            InterruptedException {
		int itr=1;
		 Path pt=new Path("hdfs://localhost:9000/itr.txt");
	        FileSystem fsx = FileSystem.get(new Configuration());
	        BufferedReader br=new BufferedReader(new InputStreamReader(fsx.open(pt)));
	        String line2;
	        line2=br.readLine();
	        while (line2 != null){
	        	itr=Integer.parseInt(line2);
	                line2=br.readLine();
	        }
	        br.close();
		int size[]=new int[itr+1];
		
			String[] val=value.toString().split("\t");
  	  String line;
  	    Path pt1=new Path("hdfs://localhost:9000/pSize.txt");
  	    FileSystem fs1 = FileSystem.get(new Configuration());
  	    BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
  	        line=br1.readLine();
  	    while (line != null){
  	    	String l[]=line.split("\t");
  	    	size[Integer.parseInt(l[0])]=Integer.parseInt(l[1]);
  	    	line=br1.readLine();
  	    }
   
  	    
  	    
  	    size[Integer.parseInt(val[0])]+=1;
  	    
  	  Path pt4=new Path("hdfs://localhost:9000/pSize.txt");
      FileSystem fs4 = FileSystem.get(new Configuration());
      BufferedWriter br4=new BufferedWriter(new OutputStreamWriter(fs4.create(pt4,true)));
      String ln2="";
      for(int i=1;i<=itr;i++)
      {
      	ln2=i+"\t"+ size[i]+"\n";
      br4.append(ln2);
      }
      br4.close();
  	output.write(new IntWritable(Integer.parseInt(val[0])),new Text(val[1]+"\t"+val[2]+"\t"+val[3]));
  	
    	  }
}

