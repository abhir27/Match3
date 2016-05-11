import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Match3 extends Configured implements Tool {
	 public static Configuration conf;
	 public static int itr=1; 
	 public static int vert=1; 
	 public static int it=1; 
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("START Matching");
		int res = ToolRunner.run(conf,new Match3(), args);
        System.exit(res);
	}


	public int run(String[] args) throws Exception {
		conf=new Configuration();
		if (args.length != 2) 
		{
			System.out.println("usage: [input] [output].");
			System.exit(-1);
		}
		Path pt2=new Path("hdfs://localhost:9000/itr.txt");
        FileSystem fs2 = FileSystem.get(new Configuration());
        BufferedWriter br2=new BufferedWriter(new OutputStreamWriter(fs2.create(pt2,true)));
        String line;
        line=Integer.toString(1);
       // System.out.println("first k"+line);
        br2.write(line);
        br2.close();
        //vertices
    	Path pt3=new Path("hdfs://localhost:9000/vert.txt");
        FileSystem fs3 = FileSystem.get(new Configuration());
        BufferedWriter br3=new BufferedWriter(new OutputStreamWriter(fs3.create(pt3,true)));
       line=Integer.toString(0);
        //System.out.println("first k"+line);
        br3.write(line);
        br3.close();
		//
		//
		//Sort edges
				Job job = Job.getInstance(conf);
		        job.setOutputKeyClass(IntWritable.class);
		        job.setOutputValueClass(Text.class);
		        job.setMapperClass(EdgeMapper.class);
		        job.setReducerClass(EdgeReducer.class);
		        job.setInputFormatClass(TextInputFormat.class);
		        job.setOutputFormatClass(TextOutputFormat.class);
		        FileInputFormat.setInputPaths(job, new Path(args[0]));
		        FileOutputFormat.setOutputPath(job, new Path(args[1]+"a1"));
		        job.setJarByClass(Match3.class);
		        job.waitForCompletion(true);
		        //
		        Path pt=new Path("hdfs://localhost:9000/itr.txt");
		        FileSystem fsx = FileSystem.get(new Configuration());
		        BufferedReader br=new BufferedReader(new InputStreamReader(fsx.open(pt)));
		        String line2;
		        line2=br.readLine();
		        while (line2 != null){
		        	itr=Integer.parseInt(line2);
		                System.out.println("final k:"+line2);
		                line2=br.readLine();
		        }
		        br.close();
		        
		        //check no. of vertices
		        Path pt1=new Path("hdfs://localhost:9000/vert.txt");
		        FileSystem fs1 = FileSystem.get(new Configuration());
		        BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
		        line2=br1.readLine();
		        while (line2 != null){
		        	vert=Integer.parseInt(line2);
		                System.out.println("final v:"+line2);
		                line2=br1.readLine();
		        }
		        br1.close();
		        Path ptx=new Path("hdfs://localhost:9000/matching.txt");
		        FileSystem fsx2 = FileSystem.get(new Configuration());
		        BufferedWriter brx=new BufferedWriter(new OutputStreamWriter(fsx2.create(ptx,true)));                              
		        String ln="";
		        for(int i=1;i<=vert;i++)
		        {
		        	ln=i+"\t0\t0\n";
		        brx.append(ln);
		        }
		        brx.close();
		        Job job1; Job job2;Job job3;
		        it=itr;
		        for(;it>0;it--)
		        {
		//
		//divide in partitions
				job1 = Job.getInstance(conf);
		        job1.setOutputKeyClass(IntWritable.class);
		        job1.setOutputValueClass(Text.class);
		        job1.setMapperClass(SampleMapper.class);
		      //  job1.setReducerClass(SampleReducer.class);
		        job1.setInputFormatClass(TextInputFormat.class);
		        job1.setOutputFormatClass(TextOutputFormat.class);
		        FileInputFormat.setInputPaths(job1, new Path(args[1]+"a1"));
		        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"b"+it));
		        job1.setJarByClass(Match3.class);
		        job1.waitForCompletion(true);
		        
		        
		        Path pt4=new Path("hdfs://localhost:9000/pSize.txt");
		        FileSystem fs4 = FileSystem.get(new Configuration());
		        BufferedWriter br4=new BufferedWriter(new OutputStreamWriter(fs4.create(pt4,true)));
		        String ln2="";
		        for(int i=1;i<=itr;i++)
		        {
		        	ln2=i+"\t0\n";
		        br4.append(ln2);
		        }
		        br4.close();
		        
		        job3 = Job.getInstance(conf);
		        job3.setOutputKeyClass(IntWritable.class);
		        job3.setOutputValueClass(Text.class);
		        job3.setMapperClass(PartMapper.class);
		        //job3.setReducerClass(PartReducer.class);
		        job3.setInputFormatClass(TextInputFormat.class);
		        job3.setOutputFormatClass(TextOutputFormat.class);
		        FileInputFormat.setInputPaths(job3, new Path(args[1]+"b"+it));
		        FileOutputFormat.setOutputPath(job3, new Path(args[1]+"c"+it));
		        job3.setJarByClass(Match3.class);
		        job3.waitForCompletion(true);
		        
		        
		  
		        
		        job2 = Job.getInstance(conf);
		        job2.setOutputKeyClass(IntWritable.class);
		        job2.setOutputValueClass(Text.class);
		        job2.setMapperClass(SamplerMapper.class);
		        job2.setReducerClass(SampleReducer.class);
		        job2.setInputFormatClass(TextInputFormat.class);
		        job2.setOutputFormatClass(TextOutputFormat.class);
		        FileInputFormat.setInputPaths(job2, new Path(args[1]+"c"+it));
		        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"d"+it));
		        job2.setJarByClass(Match3.class);
		        job2.waitForCompletion(true);
		        
		        }
		        //Read from file.
		        Path pt1x=new Path("hdfs://localhost:9000/matching.txt");
		        FileSystem fs1x = FileSystem.get(new Configuration());
		        BufferedReader br1x=new BufferedReader(new InputStreamReader(fs1x.open(pt1x)));
		            line=br1x.readLine();
		        while (line != null){
		        	System.out.println(line);
		        	line=br1x.readLine();
		        }       
		        System.out.println("STOP");
				return 0;
	}

}
