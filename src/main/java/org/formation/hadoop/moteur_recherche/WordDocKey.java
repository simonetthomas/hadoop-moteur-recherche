package org.formation.hadoop.moteur_recherche;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordDocKey implements WritableComparable<WordDocKey>{

	private Text word=new Text();
	private Text filePathString=new Text();

	public WordDocKey() {
		super();
	}

	public WordDocKey(Text word, Text doc) {
		super();
		this.word = word;
		this.filePathString = doc;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return word+" "+filePathString;
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
	 * @return the filePathString
	 */
	public Text getFilePathString() {
		return filePathString;
	}

	/**
	 * @param filePathString the filePathString to set
	 */
	public void setFilePathString(Text filePathString) {
		this.filePathString = filePathString;
	}

	public void readFields(DataInput in) throws IOException {
		word.readFields(in);
		filePathString.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		word.write(out);
		filePathString.write(out);
	}

	public void set(String word, String filePathString) {
		this.word=new Text(word);
		this.filePathString=new Text(filePathString);
	}

	public int compareTo(WordDocKey o) {
		int compare = this.getFilePathString().compareTo(o.getFilePathString());
		if (compare != 0) {
			return compare;
		}
		return this.getWord().compareTo(o.getWord());
	}

}
