package org.formation.hadoop.moteur_recherche;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;





public class MapWordPerDoc extends Mapper<LongWritable, Text, Text, CustomValue>{

//	IntWritable intWritable=new IntWritable(1);
	
//	private WordDocKey wordDocKey=new WordDocKey();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		String line = value.toString();
						
		StringTokenizer tokenizer = new StringTokenizer(line);

		String word=tokenizer.nextToken();
		String docname=tokenizer.nextToken();
		int nb=Integer.parseInt(tokenizer.nextToken());
		
		CustomValue custom=new CustomValue(new Text(word), new IntWritable(nb));
				
        context.write(new Text(docname), custom);
        
	}
}
