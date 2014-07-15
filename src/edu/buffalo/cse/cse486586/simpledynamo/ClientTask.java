/**
 * NAME	    :   ANKIT SARRAF
 * EMAIL    :   sarrafan@buffalo.edu
 * PURPOSE  :   ClientTask is a Thread that is used to send a string over the network.
 *              On the start of the task, it send the message to a specified AVD
 * @author sarrafan
 */

package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

import android.util.Log;

class ClientTask extends Thread {
	// Tag for Logging Activity
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();

	// Message to Send
	private String msgToSend;
	private String receiver;

	ClientTask(String msgToSend, String receiver) {
		this.msgToSend = msgToSend;
		this.receiver = receiver;
		this.start();
	}

	public void run() {
		try {
			// Log entry that ClientTask is entered
			Log.i(TAG, "Inside Client");

			// Log the message to be sent across the socket
			//Log.i(TAG, "Message to Send => " + this.msgToSend + 
				//	" : Receiver => " + this.receiver);

			// Initiate the socket that is used to send the Message
			Socket socket = new Socket(InetAddress.getByAddress(
					new byte[]{10, 0, 2, 2}),
					Integer.parseInt(receiver) * 2);

			// PrintWriter object
			PrintWriter printWriterOut = new PrintWriter(socket.getOutputStream(), true);

			// Write on the Socket
			printWriterOut.write(msgToSend);

			// Close PrintWriter
			printWriterOut.close();

			// Close Socket
			socket.close();
		} catch(NumberFormatException e) {
			Log.e("ANKIT SARRAF", "Number Format Exception");
		} catch(IOException e) {
			Log.e("ANKIT SARRAF", "IO Exception");
		} catch(Exception e) {
			Log.e("ANKIT SARRAF", "SOME EXCEPTION : " + e.getMessage());
		}
	}
}