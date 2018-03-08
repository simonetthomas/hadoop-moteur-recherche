package org.formation.hadoop.moteur_recherche;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Compte Le nombre d'occurence d'un mot dans un document
 * @author Thomas Simonet
 *
 */
public class MapWordCount extends Mapper<LongWritable, Text, WordDocKey, IntWritable>{

	private IntWritable intWritable = new IntWritable(1);
	private WordDocKey wordDoc = new WordDocKey();
	private Set<String> stopWords = new HashSet<String>();
	
	@SuppressWarnings("deprecation")
	public void setup(Context context) throws IOException {
		Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
        if (stopWordsFiles != null && stopWordsFiles.length > 0) {
            for (Path stopWordFile : stopWordsFiles) {
            	BufferedReader bufferedReader = new BufferedReader(new FileReader(stopWordFile.toString()));
 	            String stopWord = null;
 	            while((stopWord = bufferedReader.readLine()) != null) {
 	                stopWords.add(stopWord.toLowerCase());
 	            }
            }
        }
		
	}
	
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
			if (word.getLength()>2 && !stopWords.contains(word.toString())) {
				wordDoc.set(word.toString(), filePathString);
	            context.write(wordDoc, intWritable);
			}
        }
        
	}
}
