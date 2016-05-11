import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class EdgeReducer extends Reducer< IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
    	// Configuration conf = output.getConfiguration();
    for(Text value:values)
 {
    String val[]=value.toString().split("\t");
    double wt= Double.parseDouble(val[1]);
    int k=1;
    int t=Double.compare(wt,(Math.pow(2, k)));
    while(t>0)
    {
    	k++;
    	t=Double.compare(wt,(Math.pow(2, k)));
    }
   int old=0;
    Path pt=new Path("hdfs://localhost:9000/itr.txt");
    FileSystem fs = FileSystem.get(new Configuration());
    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
    String line;
    line=br.readLine();
    while (line != null){
    	old=Integer.parseInt(line);
            line=br.readLine();
    }

    if(old<k)
    {
    	Path pt2=new Path("hdfs://localhost:9000/itr.txt");
        FileSystem fs2 = FileSystem.get(new Configuration());
        BufferedWriter br2=new BufferedWriter(new OutputStreamWriter(fs2.create(pt2,true)));
                                   
        String line2;
        line2=Integer.toString(k);
        System.out.println("k written:"+line2);
        br2.write(line2);
        br2.close();
    }
    output.write(new IntWritable(k), new Text(key +"\t"+val[0]+"\t"+val[1]));
    
 } 
    //counting vertices
    int v=0;String line;
    Path pt1=new Path("hdfs://localhost:9000/vert.txt");
    FileSystem fs1 = FileSystem.get(new Configuration());
    BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
        line=br1.readLine();
    while (line != null){
    	v=Integer.parseInt(line);
            line=br1.readLine();
    }
    Path pt2=new Path("hdfs://localhost:9000/vert.txt");
    FileSystem fs2 = FileSystem.get(new Configuration());
    BufferedWriter br2=new BufferedWriter(new OutputStreamWriter(fs2.create(pt2,true)));                              
    String line2;
    line2=Integer.toString(v+1);
   //change this to max of vertex index else it will give problem with discontinued index ranges 
    //System.out.println("k written:"+line2);
    br2.write(line2);
    br2.close();
  
    }
}