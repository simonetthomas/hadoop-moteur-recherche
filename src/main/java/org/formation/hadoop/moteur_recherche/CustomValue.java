package org.formation.hadoop.moteur_recherche;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Contient un mot et un nombre
 * @author Thomas Simonet
 *
 */
public class CustomValue implements Writable {

	private Text word;
	private IntWritable nb;
	
	public CustomValue(Text word, IntWritable nb) {
		super();
		this.word = word;
		this.nb = nb;
	}
	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		nb.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		word.write(out);
		nb.write(out);		
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return word+" "+nb;
	}
	/**
	 * @return the word
	 */
	public Text getWord() {
		return word;
	}
	/**
	 * @param word the word to set
	 */
	public void setWord(Text word) {
		this.word = word;
	}
	/**
	 * @return the nb
	 */
	public IntWritable getNb() {
		return nb;
	}
	/**
	 * @param nb the nb to set
	 */
	public void setNb(IntWritable nb) {
		this.nb = nb;
	}

}
