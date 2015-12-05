package com.laoxiao.mr.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRunJob {

	public static enum Counter {
	    ABC
	}
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try{
			
			Configuration config =new Configuration();
			config.set("fs.defaultFS", "hdfs://node4:8020");
			config.set("yarn.resourcemanager.hostname", "node1");
			config.setInt("pageCount", 4);
			FileSystem fs =FileSystem.get(config);
			Path firstInPath=new Path("/usr/input/pagerank.txt");
			
			double d =0.0001;
			int i =0;
			while(true){
				i++;
				Path outpath =new Path("/usr/output/pr"+i);
				Path inpath =new Path("/usr/output/pr"+(i-1));
				if(i==1){
					inpath=firstInPath;
				}
				config.setInt("run_num", i);
				double result =iter(config,  fs, i, inpath, outpath);
				if(result < d){
					System.out.println(result+"#######################");
					break;
				}
				
			}
			System.out.println(i+"**********************");
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public static double iter(Configuration config,FileSystem fs,int i,Path inpath,Path outpath)throws Exception{
			Job job =Job.getInstance(config);
			job.setJobName("fof"+i);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			//mapreduce 输入文件
			FileInputFormat.setInputPaths(job, inpath);
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			//设置mapreduce的输出文件目录，该目录不能存在，自动创建
			if(fs.exists(outpath)){
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			
			boolean f= job.waitForCompletion(true);
			if(f){
				long sumabc= job.getCounters().findCounter(Counter.ABC).getValue();
				System.out.println(sumabc +"@@@@@@@@@@@@@@@@");
				double avg_abc= sumabc/10000.0/4;
				return avg_abc;
			}
			job=null;
			return 0;

	}
	
	
	//读取原数据
	static class PageRankMapper extends Mapper<Text, Text, Text, Text>{
		
		
		
		protected void map(Text key, Text value,
				Context context)
						throws IOException, InterruptedException {
			int run_num =context.getConfiguration().getInt("run_num", 1);
			String v =value.toString();
			Node page =null;
			if(run_num==1){
				page =Node.fromMR("1.0"+"\t"+v);
			}else{
				page =Node.fromMR(v);
			}
			context.write(key, new Text(page.toString()));//每个页面的初始值输出  A 1.0 B D
			
			if(page.containsAdjacentNodes()){
				String pages[] =page.getAdjacentNodeNames();
				double outValue= page.getPageRank()/Double.valueOf(page.getAdjacentNodeNames().length+"");
				for (int i = 0; i < pages.length; i++) {
					String apage = pages[i];
					Node anode =new Node();
					anode.setPageRank(outValue);
					context.write(new Text(apage), new Text(anode.toString())); // B 0.5
				}
			}
		}
	}
	
	static class PageRankReducer extends Reducer<Text, Text, Text, Text>{
		 
		 
		 static int pagecount =0;
		 
		 //reduce任务启动，只会调用一次setup
		protected void setup(Context context)
				throws IOException, InterruptedException {
			pagecount =context.getConfiguration().getInt("pageCount", 4);
		}
		 
		protected void reduce(Text key, Iterable<Text> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			double sum =0;
			double oldpagerank=0;
			Node sourceNode =null;
			for( Text i :arg1 ){
				Node node =Node.fromMR(i.toString());
				if(node.containsAdjacentNodes()){
					sourceNode =node;
					oldpagerank=node.getPageRank();
//					sum=sum+oldpagerank;
				}else{
					sum=sum+node.getPageRank();
				}
			}
			
			double newpagerank =sum*0.85+(0.15/pagecount);
			sourceNode.setPageRank(newpagerank);
			
			double abc =Math.abs(newpagerank-oldpagerank);
			
			int l_abc=(int) (abc*10000);
			
			arg2.getCounter(Counter.ABC).increment(l_abc);
			arg2.write(key, new Text(sourceNode.toString()));
		}
		
	}
	
	
	
	//计算页面总数，自己去执行
	
	static class FirstMapper extends Mapper<Text, Text, Text, LongWritable>{
		
		protected void map(Text key, Text value,
				Context context)
				throws IOException, InterruptedException {
			context.write(new Text("pagecount"), new LongWritable(1));
		}
	}
	
	
	static class FirstReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		protected void reduce(Text arg0, Iterable<LongWritable> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			long sum=0;
			for(LongWritable i:arg1){
				sum=sum+i.get();
			}
			arg2.write(arg0, new LongWritable(sum));
		}
	}
}
