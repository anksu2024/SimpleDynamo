/**
 * Main Activity class for the Simple Dynamo Application
 * @author Steveko & sarrafan
 */

package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Activity;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

		TextView tv = (TextView) findViewById(R.id.myData);
		tv.setMovementMethod(new ScrollingMovementMethod());

		findViewById(R.id.button3).setOnClickListener(
				new OnTestClickListener(tv, getContentResolver()));

		Button successorButton = (Button) findViewById(R.id.button4);
		successorButton.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
				TextView tt = (TextView) findViewById(R.id.mySuccPre);

				tt.setText("");

				for(Node aNode : SimpleDynamoProvider.allNodes) {
					tt.append(aNode.getMyNode() + "\n");
				}
			}
		});

		findViewById(R.id.button1).setOnClickListener(
				new OnLDumpClickListener(tv, getContentResolver()));

		findViewById(R.id.button2).setOnClickListener(
				new OnGDumpClickListener(tv, getContentResolver()));

		Button clearButton = (Button) findViewById(R.id.button5);
		clearButton.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
				TextView tv;

				tv = (TextView) findViewById(R.id.mySuccPre);
				tv.setText("");

				tv = (TextView) findViewById(R.id.myData);
				tv.setText("");
			}
		});
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
}