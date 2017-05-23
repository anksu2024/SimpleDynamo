/**
 * NAME	                : ANKIT SARRAF
 * EMAIL                : sarrafan@buffalo.edu
 * PROJECT              : IMPLEMENTING SIMPLE DYNAMO
 * ASSUMPTIONS          : 1) NO KEY VALUE PAIR HAS A COLON (:) OR PIPE (|) IN IT
 *                        2) IN THE BEGINNING ALL THE 5 AVDs WILL START
 *                        3) AT MOST 1 NODE CAN FAIL AT A TIME
 *                        4) ALL THE NODE FAILURES ARE TEMPORARY
 * IMPLEMENTATION       : 1) MEMBERSHIP
 *                        2) REQUEST ROUTING
 *                        3) QUORUM REPLICATION
 *                        4) CHAIN REPLICATION
 *                        5) FAILURE HANDLING
 * RESOURCES            : 1) http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
 *                        2) Lecture slides on Amazon Dynamo, Prof. Steve Ko (SUNY Buffalo)
 *                        3) SimpleDht Implementation by Ankit Sarraf (sarrafan@buffalo.edu)
 * DECLARATION          : THIS IS MY ORIGINAL PIECE OF WORK. 
 *                        EVERYONE HAS THE RIGHT TO USE THIS CODE.
 */

package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	// Static TAG for Logging Information 
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();

	// List of all the AVDs in the Dynamo. Added as they come in
	public static ArrayList<Node> allNodes;

	// The Database Helper Class
	DatabaseHelper databaseHelper;

	// Boolean Variable representing whether a Key Value Pair was received or not
	private static volatile boolean receivedReponse;

	// The Key Value Pairs that were received
	private static volatile String remoteCursor;

	// To represent my Consistent State
	public static volatile boolean isConsistent;

	// Global Data String Array
	public static volatile String [] globalData = {"", "", "", ""};
	public static volatile int indexGlobalData = 0;

	// The key I requested
	public static volatile String keyRequested = "";

	// My Recovery 
	public static volatile int numberOfRecoveryMsgs;
	public static volatile String [] recoveryMsgs = {"", "", "", "", ""};

	static {
		System.gc();

		allNodes = new ArrayList<Node> ();

		for(int i = 0 ; i < Constants.REMOTE_PORT.length ; i++) {
			try {
				Node newNode = new Node(Constants.REMOTE_PORT[i]);
				SimpleDynamoProvider.allNodes.add(newNode);
			} catch (NoSuchAlgorithmException e) {
				Log.e("ANKIT", "INSIDE THE EXCEPTION");
			}
		}

		Collections.sort(allNodes);

		receivedReponse = false;
		remoteCursor = "";

		numberOfRecoveryMsgs = 0;
		isConsistent = false;
	}

	@Override
	public synchronized boolean onCreate() {
		// Set the DatabaseHelper for this AVD
		databaseHelper = new DatabaseHelper(getContext());

		// Initialize the Listener ServerTask Thread
		try {
			ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
			new ServerTask(serverSocket);
		} catch(IOException e) {
			// Log.e(TAG, "Can't create a ServerSocket : " + getMyPort());
			return false;
		}

		//Retrieve the Database which this code will work on
		SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

		String columns[] = {DatabaseHelper.KEY, DatabaseHelper.VALUE};
		
		// Delete all my inconsistent data
		myDB.delete(DatabaseHelper.TABLE_NAME, null, null);
		
		columns = new String [] {"key"};
		
		Cursor cursor = myDB.query(DatabaseHelper.DUMMY_TABLE, columns, 
				null, null, null, null, null);

		if(cursor.getCount() == 0) {
			// I am executing for the first time
			Log.d("ONCREATE_1", "MY FIRST EXECUTION");
			ContentValues values = new ContentValues();
			values.put(columns[0], "START");

			myDB.insert(DatabaseHelper.DUMMY_TABLE, null, values);
			
			SimpleDynamoProvider.isConsistent = true;
		} else {
			Log.d("ONCREATE_2", "ON SUBSEQUENT EXECUTION");

			RecoveryThread rt = new RecoveryThread();
			try {
				Log.d("ONCREATE_3", "RECOVERY THREAD STARTED");
				rt.join();
				Log.d("ONCREATE_4", "RECOVERY SUCCESSFUL");
			} catch (InterruptedException e) {
				Log.d("ONCREATE_5", "EXCEPTION IN RECOVERY");
			}
		}

		SimpleDynamoProvider.isConsistent = true;

		return true;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		String destinationNode = findKeyLocation(values.get("key").toString());

		String key = values.getAsString("key");
		String value = values.getAsString("value");

		// Insertion message
		String msgToSend = "insert|" + key + ":" + value;

		// Send this insertion message to the destination Node
		if(destinationNode.equals(getMyPort())) {
			SQLiteDatabase myDB = databaseHelper.getWritableDatabase();
			myDB.replace(DatabaseHelper.TABLE_NAME, null, values);
		} else {
			new ClientTask(msgToSend, destinationNode);
		}

		String [] successorsForDestNode = findMySuccessors(destinationNode);
		for(int i = 0 ; i < successorsForDestNode.length ; i++) {
			if(successorsForDestNode[i].equals(getMyPort())) {
				SQLiteDatabase myDB = databaseHelper.getWritableDatabase();
				myDB.replace(DatabaseHelper.TABLE_NAME, null, values);
			} else {
				new ClientTask(msgToSend, successorsForDestNode[i]);
			}
		}

		return uri;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// Retrieve the Database to work on
		SQLiteDatabase myDB = databaseHelper.getReadableDatabase();
		Cursor cursor = null;

		String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

		selectionArgs = new String [] {selection};

		if(selection.equals("@")) {
			// LDump Search
			cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, null, null,
					null, null, null);

			Log.d("LDUMP", "# of Rows Retrieved : " + cursor.getCount());
		} else if(selection.equals("*")) {
			// GDump Search

			Log.d("UBMAIL", "REACHED QUERY * 1");

			cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, null, null,
					null, null, null);

			Log.d("UBMAIL", "REACHED QUERY * 2" + cursor.getCount());

			// This is global Data store - Adding my data
			globalData[indexGlobalData] = serialize(cursor);
			indexGlobalData++;

			Log.d("UBMAIL", "REACHED QUERY * 3" + globalData[0] + ":" + indexGlobalData);

			String msgToSend = "globalQuery|" + getMyPort();


			// Send the Query Request to All Nodes
			for(String nextNode : Constants.REMOTE_PORT) {
				if(!nextNode.equals(getMyPort())) {
					new ClientTask(msgToSend, nextNode);
				}
			}

			Log.d("UBMAIL", "REACHED QUERY * 5 : sent the query to all the nodes");

			try {
				new Waiter().join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			Log.d("UBMAIL", "REACHED QUERY * 6 : Received query from all");

			String finalGlobalDump ="";

			for(int i = 0 ; i < SimpleDynamoProvider.indexGlobalData ; i++) {
				if(globalData[i] == null || globalData[i].equals(null) || globalData[i].equals("")) {
					continue;
				}

				finalGlobalDump = finalGlobalDump.concat(globalData[i] + " ");
			}

			finalGlobalDump = finalGlobalDump.trim();

			if(finalGlobalDump.equals("")) {
				// No Data in all the content providers
				cursor = null;
			} else {
				// Put all the KV Pairs in the cursor (eliminate the duplicates)
				Map <String, String> starQueryResult = new HashMap<String, String>();

				// Cursor to Store the final return of the Star Query
				MatrixCursor tempCursor = new MatrixCursor(new String[] {"key", "value"});
				for(String starPart: finalGlobalDump.split(" ")) {
					String key = starPart.split(":")[0];
					String value = starPart.split(":")[1];

					if(starQueryResult.containsKey(key)) {
						continue;
					}

					starQueryResult.put(key, value);
					tempCursor.addRow(new String[] {key, value});
				}

				cursor = (Cursor) tempCursor;
				tempCursor.close();
			}

			SimpleDynamoProvider.globalData = new String[] {"", "", "", "", ""};
			SimpleDynamoProvider.indexGlobalData = 0;

			Log.d("GDUMP", "# of Rows Retrieved : " + cursor.getCount());
		} else {
			// Key value pair search

			keyRequested = selection;

			cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, columns[0] + "=?",
					selectionArgs, null, null, null);

			if(cursor == null || cursor.getCount() == 0) {
				// I don't have the key

				// Send message to Correct node for the key
				String correctNode = findKeyLocation(selectionArgs[0]);
				String [] correctSuccessors = findMySuccessors(correctNode);

				// Send the request to the Node
				String queryMessage = "query|" + getMyPort() + "|" + selectionArgs[0];
				new ClientTask(queryMessage, correctNode);
				for(int i = 0 ; i < 2 ; i++) {
					new ClientTask(queryMessage, correctSuccessors[i]);
				}

				try {
					new Waiter().join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				// De-serialize the KeyValue Pairs that were received
				cursor = deserialize(SimpleDynamoProvider.remoteCursor);

				// Reset the remote Cursor Value
				SimpleDynamoProvider.remoteCursor = "";

				// As soon as out of this loop make it false to make it available for new Query
				SimpleDynamoProvider.receivedReponse = false;
			}

			Log.d("KV DUMP", "# of Rows Retrieved : " + cursor.getCount());
		}

		return cursor;
	}

	class Waiter extends Thread {
		Waiter() {
			this.start();
		}

		public void run() {
			while(true) {
				// Waiting for SimpleDhtProvider.recievedReponse = true
				// Here the SimpleDhtProvider.receivedResponse is a shared resource

				if(SimpleDynamoProvider.receivedReponse || 
						SimpleDynamoProvider.indexGlobalData >= 4) {
					Log.d(TAG, "ReceivedNode got set to " + SimpleDynamoProvider.receivedReponse);
					break;
				}
			}			
		}
	}

	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// Retrieve the database name to work on
		SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

		int deletedRows = 0;

		String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

		selectionArgs = new String [] {selection};

		String correctNode = findKeyLocation(selection);


		String msgToSend = "delete|" + getMyPort() + "|" + selection;
		for(String receiver: findMySuccessors(correctNode)) {
			new ClientTask(msgToSend, receiver);
		}

		if(correctNode.equals(getMyPort())) {
			deletedRows = myDB.delete(DatabaseHelper.TABLE_NAME, columns[0] + "=?", selectionArgs);
		} else {
			new ClientTask(msgToSend, correctNode);
		}

		// To indicate how many rows (row with key value as selectionArgs[0]
		/**Log.d(TAG, "Inside delete. Deleted # " + deletedRows + " Row with Key - " + selectionArgs[0]);*/

		// Return 0 if no row deleted
		return deletedRows;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		return 0;
	}

	protected final class DatabaseHelper extends SQLiteOpenHelper {
		// Initialize the Database Name
		private static final String DATABASE_NAME = "Dynamodatabase.db";

		// Initialize the Table Name
		private static final String TABLE_NAME = "DYNAMOTABLE";

		// Dummy Table
		private static final String DUMMY_TABLE = "dummy";

		// Initialize the Database Version
		private static final int DATABASE_VERSION = 1;

		// Columns in DYNAMOTABLE
		// Column Key
		private static final String KEY = "key";
		// Column Value
		private static final String VALUE = "value";

		// Query String for Creating KEYVALUETABLE
		private static final String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (" +
				KEY + " VARCHAR(255) PRIMARY KEY, " +
				VALUE + " VARCHAR(255));";

		private static final String CREATE_DUMMY_TABLE = 
				"CREATE TABLE " + DUMMY_TABLE + " (key TEXT);";

		Context context;

		DatabaseHelper(Context context) {
			// Call the super() class constructor
			super(context, DATABASE_NAME, null, DATABASE_VERSION);

			// Setting the context data-member
			this.context = context;
		}

		@Override
		public void onCreate(SQLiteDatabase myDB) {
			// Go ahead and create the table
			myDB.execSQL(CREATE_TABLE);
			myDB.execSQL(CREATE_DUMMY_TABLE);

			// Log to indicate creation of the table
			Log.i(TAG, "Table created => " + CREATE_TABLE);
		}

		@Override
		public void onUpgrade(SQLiteDatabase myDB, int arg1, int arg2) {

		}
	}

	// Utility Methods

	// Method1: Find the Correct Location of the Key Based on Hash Value
	private String findKeyLocation(String newKey) {
		for(int i = 0 ; i < Constants.MAX ; i++) {
			try {
				if(genHash(newKey).compareTo
						(SimpleDynamoProvider.allNodes.get(i).getMyHash()) < 0) {
					return SimpleDynamoProvider.allNodes.get(i).getMyNode();
				} else if(i == 4) {
					return SimpleDynamoProvider.allNodes.get(0).getMyNode();
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}

		return getMyPort();
	}

	// Get my Port Number
	public String getMyPort() {
		/* Professor's Hack - Taken from SimpleMessenger */
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService
				(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		String myPort = String.valueOf((Integer.parseInt(portStr)));
		return myPort;
	}

	// Get the Hash Value of a String
	public static String genHash(String input) throws NoSuchAlgorithmException {
		/* Professor's Hack - Taken From SimpleDht */
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}

		String hashValue = formatter.toString();
		formatter.close();

		return hashValue;
	}

	// Predecessor Finder
	private String [] findMyPredecessor(String targetNode) {
		String [] predecessors = new String[2];
		for(int i = 0 ; i < Constants.MAX ; i++) {
			if(targetNode.equals(allNodes.get(i).getMyNode())) {
				switch(i) {
				case 0:
					predecessors[0] = allNodes.get(4).getMyNode();
					predecessors[1] = allNodes.get(3).getMyNode();
					break;
				case 1:
					predecessors[0] = allNodes.get(0).getMyNode();
					predecessors[1] = allNodes.get(4).getMyNode();
					break;
				default:
					predecessors[0] = allNodes.get(i - 1).getMyNode();
					predecessors[1] = allNodes.get(i - 2).getMyNode();
					break;
				}

				break;
			}
		}

		return predecessors;
	}

	// Successor Finder
	private String [] findMySuccessors(String targetNode) {
		String [] successors = new String[2];
		for(int i = 0 ; i < Constants.MAX ; i++) {
			if(targetNode.equals(allNodes.get(i).getMyNode())) {
				switch(i) {
				case 3:
					successors[0] = allNodes.get(4).getMyNode();
					successors[1] = allNodes.get(0).getMyNode();
					break;
				case 4:
					successors[0] = allNodes.get(0).getMyNode();
					successors[1] = allNodes.get(1).getMyNode();
					break;
				default:
					successors[0] = allNodes.get(i + 1).getMyNode();
					successors[1] = allNodes.get(i + 2).getMyNode();
					break;
				}

				break;
			}
		}

		return successors;
	}

	// Serializing Method - Converts cursor into KEY:VALUE string separated by _
	private String serialize(Cursor cursor) {
		String serializedCursor = "";
		while(cursor.moveToNext()) {
			serializedCursor += cursor.getString(0) + ":" + cursor.getString(1) + " ";
		}

		return serializedCursor.trim();
	}

	// De-serializing Method - Converts the receivedMessage to a cursor
	private Cursor deserialize(String receivedMesage) {

		String [] keyValPairs = receivedMesage.trim().split(" ");

		MatrixCursor tempCursor = new MatrixCursor(new String[] {"key", "value"});
		for(int i = 0 ; i < keyValPairs.length; i++) {
			// The key Value pair
			String [] keyValRow = keyValPairs[i].split(":");

			tempCursor.addRow(keyValRow);
		}

		return (Cursor)tempCursor;
	}

	// The Clincher - Server Task
	class ServerTask extends Thread {
		// This class acts as a Listener Thread for the individual AVD
		private ServerSocket serverSocket;
		private Socket clientSocket;
		private BufferedReader bufferIn;

		ServerTask(ServerSocket serverSocket) {
			this.serverSocket = serverSocket;
			this.start();
		}

		public void run() {
			// Log.d(TAG, "Inside Server Task");

			while(true) {
				try {
					clientSocket = serverSocket.accept();

					bufferIn = new BufferedReader
							(new InputStreamReader(clientSocket.getInputStream()));

					final String inputLine = bufferIn.readLine();
					if(inputLine.equals(null) || inputLine.equals("") || inputLine == null) {
						//If blank input entered, just break
						// Log.e(TAG, "Blank Input Exiting");
						break;
					}

					/**
					 * SERVER TASK COMMENCES
					 */

					String [] messageParts = inputLine.split("\\|");

					if(messageParts[0].equals("insert")) {
						/*					if(isConsistent == false) {
							// It means that I am in inconsistent state
							continue;
						}
						 */
						// Insert this key Value pair here and send insert request to Successors
						SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

						String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

						String key = messageParts[1].split(":")[0];
						String value = messageParts[1].split(":")[1];

						ContentValues values = new ContentValues();
						values.put(columns[0], key);
						values.put(columns[1], value);

						// Insert the newly received data into self
						Log.d("ANKIT", "MyDB : " + myDB);
						myDB.replace(DatabaseHelper.TABLE_NAME, null, values);
					} else if(messageParts[0].equals("query")) {
						if(messageParts[1].equals(getMyPort())) {
							// If the initiator Node

							if(SimpleDynamoProvider.receivedReponse == true) {
								// Ignore the message which I have received
								// Since I already received the query
								continue;
							}

							if(!messageParts[2].contains(":")) {
								// No such Key Was found
								SimpleDynamoProvider.remoteCursor = new String("");
							} else {
								// Found the Required Key Value Pair
								if(!(messageParts[2].split(":")[0]).equals(keyRequested)) {
									continue;
								}
								SimpleDynamoProvider.remoteCursor = new String(messageParts[2]);
								keyRequested = "";
							}

							SimpleDynamoProvider.receivedReponse = true;
						} else {
							/*if(isConsistent == false) {
								// It means that I am in inconsistent state
								continue;
							}*/

							// The Node which contains that key

							// Table to Query the Key
							SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

							// Declare Columns where insertion is to be made
							String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

							// My Selection Arguments
							String [] selectionArgs = new String [] {messageParts[2]};

							Cursor cursor;

							Log.e("ANKIT", "Current Query : " + DatabaseHelper.TABLE_NAME + ":"
									+ columns[0] + ": SeleArgs - " + selectionArgs[0]);
							cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, 
									columns[0] + "=?", selectionArgs, null, null, null);

							String msgToSend = "";

							if(cursor == null || cursor.getCount() == 0) {
								msgToSend = msgToSend.concat(messageParts[2]);
							} else {
								msgToSend = msgToSend.concat(serialize(cursor));
							}

							msgToSend = "query|" + messageParts[1] + "|" + msgToSend;

							new ClientTask(msgToSend, messageParts[1]);

							cursor.close();
						}
					} else if(messageParts[0].equals("globalQuery")) {
						if(messageParts[1].equals(getMyPort())) {
							// I am the original sender of the message

							if(SimpleDynamoProvider.indexGlobalData >= 4) {
								//Received the expected number of Responses
								continue;
							}

							if(messageParts.length < 3) {
								// Means that no data is stored in any of the AVDs
								SimpleDynamoProvider.globalData[indexGlobalData] = "";
							} else {
								SimpleDynamoProvider.globalData[indexGlobalData] = messageParts[2];
							}

							SimpleDynamoProvider.indexGlobalData++;
						} else {
							// I am one of the participant for Global Query

							/*if(isConsistent == false) {
								// It means that I am in inconsistent state
								continue;
							}*/

							Cursor cursor;

							// Table to Query the Key
							SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

							// Declare Columns where insertion is to be made
							String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

							cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, null, null,
									null, null, null);

							String msgToSend = inputLine + "|";

							if(!(cursor == null || cursor.getCount() == 0)) {
								msgToSend = msgToSend.concat(serialize(cursor));
							}

							msgToSend.trim();

							new ClientTask(msgToSend, messageParts[1]);
						}
					} else if(messageParts[0].equals("delete")) {
						// It's not the source Node
						SQLiteDatabase myDB = databaseHelper.getWritableDatabase();

						String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

						String [] selectionArgs = new String [] {messageParts[2]};

						myDB.delete(DatabaseHelper.TABLE_NAME, columns[0] + "=?", selectionArgs);

					} else if(messageParts[0].equals("recover")) {

						// 0 => Type of message
						// 1 => Requester of the Recovery Message
						// 2 => Responder of Recovery Request
						// 3 => The Recovery Message Kn:Vn Km:Vm

						if(messageParts[1].equals(getMyPort())) {
							// Node Expecting Recovery data
							Log.d("RECOVERYREQ_1", "RECEIVED RECOVERY MSG [" + inputLine + 
									"] AT " + getMyPort());

							int correctIndex = (Integer.parseInt(messageParts[2]) - 5554) / 2;

							if(messageParts.length < 4) {
								// The Recovery Host Had no data stored on it
								recoveryMsgs[correctIndex] = "";
							} else {
								recoveryMsgs[correctIndex] = messageParts[3];
								Log.d("RECOVERYREQ_2", "ADDED AT INDEX : " + correctIndex + 
										"[" + recoveryMsgs[correctIndex] + "]");
							}

							SimpleDynamoProvider.numberOfRecoveryMsgs++;

							Log.d("RECOVERYREQ_3", "# RECOVERY MESSAGE = " + 
									SimpleDynamoProvider.numberOfRecoveryMsgs);
						} else {
							// Node serving Recovery data
							Cursor cursor = null;

							// Table to Query the Key
							SQLiteDatabase myDB = databaseHelper.getReadableDatabase();

							// Declare Columns where insertion is to be made
							String [] columns = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

							// Fetch the Local Dump
							cursor = myDB.query(DatabaseHelper.TABLE_NAME, columns, null, null,
									null, null, null);

							String msgToRecovery = inputLine + "|" + getMyPort() + "|";

							if(!(cursor == null || cursor.getCount() == 0)) {
								msgToRecovery = msgToRecovery.concat(serialize(cursor));
							}

							msgToRecovery = msgToRecovery.trim();

							Log.d("RECOVERYHOST_1", "Message - " + msgToRecovery);

							// Send the Reply of the Recovery Message to The destination
							new ClientTask(msgToRecovery, messageParts[1]);
						}
					}
				} catch(IOException e) {
					Log.d("ANKIT", "" + e.getMessage());
				}
			}
		}
	}

	class RecoveryThread extends Thread {
		private String msgToRecover;

		RecoveryThread() {
			msgToRecover = "recover|" + getMyPort();

			Log.d("RTHREAD_1", "" + Constants.REMOTE_PORT.length);

			for(int i = 0 ; i < Constants.REMOTE_PORT.length ; i++) {
				if(!Constants.REMOTE_PORT[i].equals(getMyPort())) {
					new ClientTask(msgToRecover, Constants.REMOTE_PORT[i]);
					Log.d("RTHREAD_2", "REC MSG " + msgToRecover + " FROM " + 
							getMyPort() + " TO  " + Constants.REMOTE_PORT[i]);
				}
			}
			
			try {
				Thread.sleep(100);
			} catch(Exception e) {}
			
			start();
		}
		public void run() {
			while(SimpleDynamoProvider.numberOfRecoveryMsgs < 4) {
				// This is busy waiting till I do not get Recovery messages from other 4 AVDs
			}

			Log.d("RTHREAD_3", "RECEIVED 4 REC MSG: " + SimpleDynamoProvider.numberOfRecoveryMsgs);

			for(int i = 0 ; i < SimpleDynamoProvider.recoveryMsgs.length ; i++) {
				Log.d("RTHREAD_4", "MESSAGE" + i + "=" + SimpleDynamoProvider.recoveryMsgs[i]);
			}

			Log.d("RTHREAD_5", "MAJOR CHECKPOINT REACHED");
			
			// Read the Recovered Messaged one by one and store in my DB if required
			
			// Create a Map for the purpose storing the KV Pairs received
			
			// I do not need to consider the messages that are on my index
			int myIndex = (Integer.parseInt(getMyPort()) - 5554) / 2;
			
			SQLiteDatabase myDB = databaseHelper.getWritableDatabase();
			
			for(int i = 0 ; i < SimpleDynamoProvider.recoveryMsgs.length ; i++) {
				if(i == myIndex || SimpleDynamoProvider.recoveryMsgs[i].equals("")) {
					// Ignore the messages on my index
					continue;
				}
				
				// The Key Value Pairs are separated by " "
				// The Key and Value is Separated by ":"
				String [] keyValuePairs = recoveryMsgs[i].split(" ");
				
				for(String keyValuePair : keyValuePairs) {
					String key = (keyValuePair.split(":"))[0];
					String val = (keyValuePair.split(":"))[1];
					
					String keyCorrectNode = findKeyLocation(key);
					String [] myPredecessors = findMyPredecessor(getMyPort());
					ContentValues cv = new ContentValues();
					if(keyCorrectNode.equals(getMyPort()) ||
					   keyCorrectNode.equals(myPredecessors[0]) ||
					   keyCorrectNode.equals(myPredecessors[1])) {
						// Add the KV Pair to the Table
						cv.put(DatabaseHelper.KEY, key);
						cv.put(DatabaseHelper.VALUE, val);
						
						myDB.replace(DatabaseHelper.TABLE_NAME, null, cv);
						
						Log.d("RTHREAD_6", "INSERTED KV - [" + key + "," + val + "]");
						
						cv.clear();
					}
				}
			}
			
			SimpleDynamoProvider.isConsistent = true;
		}
	}
}
