package org.formation.hadoop.moteur_recherche;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceWordPerDoc extends Reducer<Text, CustomValue, WordDocKey, WordCountWordPerDoc>{

public void reduce(Text docname, Iterable<CustomValue> values, Context context) throws IOException, InterruptedException {
		
		int sum=0;
		String word=new String();
		WordDocKey wordDocKey=new WordDocKey();
		IntWritable wordcount=new IntWritable(0);
		WordCountWordPerDoc wordCountWordPerDoc=new WordCountWordPerDoc();
		
		// Iteration sur les valeurs
		Iterator<CustomValue> it = values.iterator();
		word=it.next().getWord().toString();
		
		while(it.hasNext()) {
			CustomValue custom=it.next();
			wordcount=custom.getNb();
			word=custom.getWord().toString();
			
			sum+=(it.next().getNb()).get();
		}
		
		wordDocKey.set(word, docname.toString());
		wordCountWordPerDoc.set(wordcount, new IntWritable(sum));
		
		// Écriture dans le contexte
		context.write(wordDocKey, wordCountWordPerDoc);
		
		
	}
	
}
