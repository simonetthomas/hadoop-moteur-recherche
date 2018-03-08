package org.formation.hadoop.moteur_recherche;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class WordCountWordPerDoc implements WritableComparable<WordCountWordPerDoc>{

	IntWritable wordcount=new IntWritable();
	IntWritable wordperdoc=new IntWritable();
	
	/**
	 * @return the wordcount
	 */
	public IntWritable getWordcount() {
		return wordcount;
	}

	/**
	 * @param wordcount the wordcount to set
	 */
	public void setWordcount(IntWritable wordcount) {
		this.wordcount = wordcount;
	}

	/**
	 * @return the wordperdoc
	 */
	public IntWritable getWordperdoc() {
		return wordperdoc;
	}

	/**
	 * @param wordperdoc the wordperdoc to set
	 */
	public void setWordperdoc(IntWritable wordperdoc) {
		this.wordperdoc = wordperdoc;
	}

	public void set(IntWritable wordcount, IntWritable wordperdoc) {
		this.wordcount = wordcount;
		this.wordperdoc = wordperdoc;
	}
	
	public void readFields(DataInput in) throws IOException {
		wordcount.readFields(in);
		wordperdoc.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		wordcount.write(out);
		wordperdoc.write(out);		
	}

	public int compareTo(WordCountWordPerDoc o) {
		int compare = this.getWordcount().compareTo(o.getWordcount());
		if (compare != 0) {
			return compare;
		}
		compare = this.getWordperdoc().compareTo(o.getWordperdoc());
		return compare;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return wordcount+" "+wordperdoc;
	}

	
}
