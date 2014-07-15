/**
 * NAME	    :   ANKIT SARRAF
 * EMAIL    :   sarrafan@buffalo.edu
 * PURPOSE  :   Implementation of Class Node
 *              Each instance of Node represents the AVD location information
 * @author sarrafan
 */

package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.NoSuchAlgorithmException;

public class Node implements Comparable <Node> {
	// My current Node Number
	private final String myNode;

	// Hash Value of My Node Number
	private final String myHash;

	// Am I alive?
	private boolean isAlive;

	public Node(String myNode) throws NoSuchAlgorithmException {
		this.myNode = myNode;
		this.myHash = SimpleDynamoProvider.genHash(myNode);
		this.setIsAlive(true);
	}

	// Getter Methods
	public String getMyNode() {
		return myNode;
	}

	public String getMyHash() {
		return myHash;
	}

	public boolean getIsAlive() {
		return isAlive;
	}

	// Setter Method
	public void setIsAlive(boolean isAlive) {
		this.isAlive = isAlive;
	}

	@Override
	public int compareTo(Node newNode) {
		return this.getMyHash().compareTo(newNode.getMyHash());
	}
}