package org.formation.hadoop.moteur_recherche;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Compte Le nombre d'occurence d'un mot dans un document
 * @author Thomas Simonet
 *
 */
public class Map extends Mapper<LongWritable, Text, WordDocKey, IntWritable>{

	private IntWritable intWritable = new IntWritable(1);
	private WordDocKey wordDoc = new WordDocKey();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// line contient l'ensemble des mots
		String line = value.toString().toLowerCase();
		
		// Récupération du nom de fichier
		String filePathString = ((FileSplit) context.getInputSplit()).getPath().getName();
				
		// le tokenizer sépare la line en mots, en précisant les caractères séparateurs
		StringTokenizer tokenizer = new StringTokenizer(line, " .,;:-+*!?\"/()[]{}<>|\t_&#0123456789");

		// Tant qu'on rencontre des mots
		while (tokenizer.hasMoreTokens()) {
			Text word=new Text(tokenizer.nextToken());
			// On ne garde que les mots de plus de 2 caractères
			if (word.getLength()>2) {
				wordDoc.set(word.toString(), filePathString);
	            context.write(wordDoc, intWritable);
			}
        }
        
	}
}
