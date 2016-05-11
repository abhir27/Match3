import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
//import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class SampleReducer extends Reducer< IntWritable, Text, IntWritable, Text> {
    //public static int matching[][]=new int[EdgeReducer.vertices][2];
	public static int matching[][];//=new int[Matching.vert+1][2];
	public static Map<Integer, String> cmat=new TreeMap<Integer, String>();
	public void reduce(IntWritable key, Iterable<Text> values, Context output)
            throws IOException, InterruptedException {
		  Path pt2=new Path("hdfs://localhost:9000/vert.txt");
		    FileSystem fs2 = FileSystem.get(new Configuration());
		    BufferedReader br2=new BufferedReader(new InputStreamReader(fs2.open(pt2)));                              
		    int vert=Integer.parseInt(br2.readLine());
		    br2.close();
		    matching= new int[vert+1][2]; 
		   // cmat= new int[vert+1][2]; 
		String line;
	    Path pt1=new Path("hdfs://localhost:9000/matching.txt");
	    FileSystem fs1 = FileSystem.get(new Configuration());
	    BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
	        line=br1.readLine();
	    while (line != null){
	    	//reading from current matching
	    	String[] l1=line.split("\t");
	    	int u=Integer.parseInt(l1[0]);
	    	matching[u][0]=Integer.parseInt(l1[1]);
	    	matching[u][1]=Integer.parseInt(l1[2]);
	            line=br1.readLine();
	    }
	    br1.close();
	    //read data from matching and store to matrix.
	    
	  /*  
	  
	    */
	     // int curr=0;
	    for(Text value:values)
	    {
	    	//curr++;
    	String[] val=value.toString().split("\t");
    	int u=Integer.parseInt(val[0]);
    	int v=Integer.parseInt(val[1]);
    	int wt=Integer.parseInt(val[2]);
    	String edge=u+"\t"+v;
    	cmat.put(wt,edge);
    	
    	
    	//cmat[u][0]=v;
    	//cmat[u][1]=wt;
    	/*if(matching[u][0]==0 && matching[v][0]==0)
    	{
    	matching[u][0]=v;
    	matching[u][1]=wt;
    	matching[v][0]=u;
    	matching[v][1]=wt;
    	//br2.write(u+"\t"+v+"\t"+wt);
    	//br2.write(v+"\t"+u+"\t"+wt);
    	output.write(new IntWritable(u), new Text(v+"\t"+wt));
    	}
    	else
    	{
    		int wt1=matching[u][1];
    		int wt2=matching[v][1];
    		if(wt>=(wt1+wt2))
    		{
    	    	matching[matching[u][0]][0]=0;
    	    	matching[matching[v][0]][0]=0;
    	    	matching[matching[u][0]][1]=0;
    	    	matching[matching[v][0]][1]=0;
    			matching[u][0]=v;
    	    	matching[u][1]=wt;
    	    	matching[v][0]=u;
    	    	matching[v][1]=wt;    	    	
    	    	output.write(new IntWritable(u), new Text(v+"\t"+wt));
       		}
    		
    	}*/
    	
   
 } 
	    
	    //update matching for partition
	    ArrayList<Integer> keys = new ArrayList<Integer>(cmat.keySet());
        for(int i=keys.size()-1; i>=0;i--){
            //System.out.println(cmat.get(keys.get(i)));
        	int wt1=keys.get(i);
        	String[] ed=cmat.get(keys.get(i)).split("\t");
        	int uu=Integer.parseInt(ed[0]);
        	int vv=Integer.parseInt(ed[1]);
        	if(matching[uu][0]==0 && matching[vv][0]==0)
        	{
        		matching[uu][0]=vv;
        		matching[vv][0]=uu;
        		matching[uu][1]=wt1;
        		matching[vv][1]=wt1;
        	}
        	        	
        }
	    
	    
	    
	    
	    
	    
	    //update according to current threshold
	   /* Path pt=new Path("hdfs://localhost:9000/threshold.txt");
	    FileSystem fs = FileSystem.get(new Configuration());
	    BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt)));    
	    br.write(curr);
	    br.close();*/
	    
	    
	    //update matching.
	    Path ptx=new Path("hdfs://localhost:9000/matching.txt");
	    FileSystem fsx = FileSystem.get(new Configuration());
	    BufferedWriter brx=new BufferedWriter(new OutputStreamWriter(fsx.create(ptx)));    
	    String line2="";
	     for(int i=1;i<=vert;i++)
	     {
	    	 line2=line2+new String(i+"\t"+matching[i][0]+"\t"+matching[i][1]+"\n");
	      }
	     brx.write(line2);
	    brx.close();
	    
    }
}