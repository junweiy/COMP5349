package ml;

import java.util.List;
import java.util.LinkedList;

import java.io.Serializable;

public class ItemSet implements Serializable {
	private LinkedList<Integer> geneIDs;
	private int numOfTransactions;

	public ItemSet() {
		this.geneIDs = new LinkedList<Integer>();
		this.numOfTransactions = 0;
	}

	public ItemSet(LinkedList<Integer> geneIDs) {
		this.geneIDs = geneIDs;
		this.numOfTransactions = 1;
	}

	public ItemSet(String geneID) {
		this.geneIDs = new LinkedList<Integer>();
		this.geneIDs.add(Integer.parseInt(geneID));
		this.numOfTransactions = 1;
	}

	public boolean filter(int thres) {
		return this.numOfTransactions >= thres;
	}

	public void setNumOfTransactions(int numOfTransactions) {
		this.numOfTransactions = numOfTransactions;
	}

	public int getNumOfTransactions() {
		return this.numOfTransactions;
	}

	public LinkedList<Integer> getGeneIDs() {
		return this.geneIDs;
	}

	public void setGeneIDs(LinkedList<Integer> geneIDs) {
		this.geneIDs = geneIDs;
	}

	@Override
	public String toString() {
		String output = new Integer(numOfTransactions).toString() + "\t";
		for (Integer i: geneIDs) {
			output += i.toString() + "\t";
		}
		return output;
	}

}