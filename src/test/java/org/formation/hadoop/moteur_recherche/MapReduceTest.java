package org.formation.hadoop.moteur_recherche;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.testutil.TemporaryPath;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class MapReduceTest {

	MapDriver <LongWritable, Text, WordDocKey, IntWritable> mapDriver;
	ReduceDriver<WordDocKey, IntWritable, WordDocKey, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, WordDocKey, IntWritable, WordDocKey, IntWritable> mapReduceDriver;
	
	/**
	 * Note : les tests ne prennent pas en compte les stopwords qui sont stockés dans un DistributedCache
	 */

	// Initialisation 
	@Before
	public void setUp() {
		MapWordCount mapper = new MapWordCount();
	    Reduce reducer = new Reduce();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("un mot. un autre mot"));
		mapDriver.withInput(new LongWritable(), new Text("une petite.phrase"));

		mapDriver.withOutput(new WordDocKey(new Text("mot"), new Text("somefile")),new IntWritable(1));
		mapDriver.withOutput(new WordDocKey(new Text("autre"), new Text("somefile")),new IntWritable(1));
		mapDriver.withOutput(new WordDocKey(new Text("mot"), new Text("somefile")),new IntWritable(1));
		
		mapDriver.withOutput(new WordDocKey(new Text("une"), new Text("somefile")),new IntWritable(1));
		mapDriver.withOutput(new WordDocKey(new Text("petite"), new Text("somefile")),new IntWritable(1));
		mapDriver.withOutput(new WordDocKey(new Text("phrase"), new Text("somefile")),new IntWritable(1));

		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(2));
	    reduceDriver.withInput(new WordDocKey(new Text("mot"), new Text("somefile")), values);
	    
	    List<IntWritable> values2 = new ArrayList<IntWritable>();
	    values2.add(new IntWritable(4));
	    values2.add(new IntWritable(5));
	    reduceDriver.withInput(new WordDocKey(new Text("autre"), new Text("somefile")), values2);
	    
	    reduceDriver.withOutput(new WordDocKey(new Text("mot"), new Text("somefile")), new IntWritable(3));
	    reduceDriver.withOutput(new WordDocKey(new Text("autre"), new Text("somefile")), new IntWritable(9));
	    reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text("un mot. un autre mot"));
	    mapReduceDriver.withOutput(new WordDocKey(new Text("autre"), new Text("somefile")), new IntWritable(1));
		mapReduceDriver.withOutput(new WordDocKey(new Text("mot"), new Text("somefile")), new IntWritable(2));	
	    mapReduceDriver.runTest();
	  }
	  
}
