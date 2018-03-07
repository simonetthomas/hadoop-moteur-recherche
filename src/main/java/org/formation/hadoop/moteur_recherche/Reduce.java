package org.formation.hadoop.moteur_recherche;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<WordDocKey, IntWritable, WordDocKey, IntWritable>{

	public void reduce(WordDocKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum=0;
		
		// Iteration sur les valeurs
		Iterator<IntWritable> it = values.iterator();
		while(it.hasNext()) {
			sum+=it.next().get();
		}
		
		// Écriture dans le contexte de la clef et du nombre total d'occurences
		context.write(key, new IntWritable(sum));
	}
	
}
