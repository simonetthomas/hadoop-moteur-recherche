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
		compare = this.getWord().compareTo(o.getWord());
		return compare;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((filePathString == null) ? 0 : filePathString.hashCode());
		result = prime * result + ((word == null) ? 0 : word.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WordDocKey other = (WordDocKey) obj;
		if (filePathString == null) {
			if (other.filePathString != null)
				return false;
		} else if (!filePathString.equals(other.filePathString))
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		return true;
	}

}
