package org.formation.hadoop.moteur_recherche;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 *
 */
public class MoteurRecherche
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
        System.out.println( "Starting..." );
        
        // Vérification qu'il y a bien 3 paramètres
 		if (args.length != 3) {
             System.out.println("Usage: [input] [output1] [output2]");
             System.exit(-1);
         }
 		
 		
 		// Configuration et instanciation du Job1
 		Configuration config = new Configuration();
 		Job job1 = null;
 		try {
 			job1 = Job.getInstance(config);
 		} catch (IOException e) {
 			e.printStackTrace();
 		}
 		job1.setJobName("job1");
 		
 		// Ajout des stopwords dans le cache du job
 		job1.addCacheFile(new Path("input/stopwords").toUri());
 		
 		// Spécification des formats d'entrée et de sortie
 		job1.setInputFormatClass(TextInputFormat.class);
 		job1.setOutputFormatClass(TextOutputFormat.class);

 		// Récupération des chemins de fichiers d'entrée et de sortie
 		Path inputFilePath = new Path(args[0]);
 		Path outputFilePath = new Path(args[1]);
 	
 		try {
 			FileInputFormat.addInputPath(job1, inputFilePath);
 		} catch (IllegalArgumentException e) {
 			e.printStackTrace();
 		} catch (IOException e) {
 			e.printStackTrace();
 		}
 		FileOutputFormat.setOutputPath(job1, outputFilePath);

 		// Spécification des types de OutputKey et outputValue
 		job1.setOutputKeyClass(WordDocKey.class);
 		job1.setOutputValueClass(IntWritable.class);
 		
 		// Spécification des classes à utiliser comme Map et comme Reduce
 		job1.setMapperClass(MapWordCount.class);
 		job1.setReducerClass(Reduce.class);

 		job1.setJarByClass(MoteurRecherche.class);
 		
 		
 		// Effacement du fichier de sortie précédent
 		FileSystem fs = FileSystem.newInstance(config);
         if (fs.exists(outputFilePath)) {
             fs.delete(outputFilePath, true);
         }
         
 		// Ajout de la soumission du job
 		job1.waitForCompletion(true);
        
 		
 	/*	// Job 2 : Calcul de WordPerDoc : nombre total de mots dans chaque document

 		Job job2 = null;
 		try {
 			job2 = Job.getInstance(config);
 		} catch (IOException e) {
 			e.printStackTrace();
 		}
 		job2.setJobName("job2");
 		
 		// Spécification des formats d'entrée et de sortie
 		job2.setInputFormatClass(TextInputFormat.class);
 		job2.setOutputFormatClass(TextOutputFormat.class);

 		// Récupération des chemins de fichiers d'entrée et de sortie
 		Path inputFilePath2 = outputFilePath;
 		Path outputFilePath2 = new Path(args[2]);
 	
 		try {
 			FileInputFormat.addInputPath(job2, inputFilePath2);
 		} catch (IllegalArgumentException e) {
 			e.printStackTrace();
 		} catch (IOException e) {
 			e.printStackTrace();
 		}
 		FileOutputFormat.setOutputPath(job2, outputFilePath2);

 		// Spécification des types de OutputKey et outputValue
 		job2.setOutputKeyClass(WordDocKey.class);
 		job2.setOutputValueClass(WordCountWordPerDoc.class);
 		
 		// Spécification des classes à utiliser comme Map et comme Reduce
 		job2.setMapperClass(MapWordPerDoc.class);
 		job2.setReducerClass(ReduceWordPerDoc.class);

 		job2.setJarByClass(MoteurRecherche.class);
 		
 		
 		// Effacement du fichier de sortie précédent
 		fs = FileSystem.newInstance(config);
         if (fs.exists(outputFilePath2)) {
             fs.delete(outputFilePath2, true);
         }
         
 		// Ajout de la soumission du job
 		job2.waitForCompletion(true);
        */
    }
}
