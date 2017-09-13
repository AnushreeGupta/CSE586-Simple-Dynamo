package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.w3c.dom.Node;

import static android.content.ContentValues.TAG;
import static edu.buffalo.cse.cse486586.simpledynamo.SQLHelper.TABLE_NAME;

public class SimpleDynamoProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    String[] REMOTE_PORTS = new String[5] ;

    final int initiatorPort = 5554;                          // Send Port i.e. first port
    String myPortID, successor1_ID, successor2_ID, predecessorID;             // All the port IDs associated with a node

    SQLHelper sqlhelper;                                    // SQL Helper class for using database as storage
    SQLiteDatabase mydatabase;                              // Database to store all the key-value pairs

    String queryResult = "";                                // To get value for unique key query

    String globalDumpKeys = "";                             // To get all keys for global query
    String globalDumpValues = "";                           // To get all values for global query
    Boolean gotGlobalDump = false;                          // Flag to check if all results for global query are received

    Boolean keyDeletedFromAll = false;


	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
        //---------------Implementation using SQLite Data Storage Method----------------//

        /* Handling for "@" symbol i.e. local delete from the current AVD */

        if(selection.equals("@")) {

            // For Local Delete from current Node
            int deleteResult = mydatabase.delete(TABLE_NAME, null, null);
            //Log.d("Deleting @ at ", myPortID);

            return deleteResult;

        } else if(selection.equals("*")) {

            // For Global Delete from current Node
            int deleteResult = mydatabase.delete(TABLE_NAME, null, null);

            NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successor1_ID, successor2_ID, "DELETE_ALL");
            node.key = selection;
            node.originator = myPortID;
            String message = node.formMessage();

            Log.d("DELETE ALL Request", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("PROPAGATE DELETE to ::", successor1_ID);

            return deleteResult;

        } else {

            /* Handling to find and delete the unique key from AVD it is present on  */

            String keyValue = sqlhelper.COLUMN_1 +" ='"+selection+"'";
            int deleteResult = mydatabase.delete(TABLE_NAME, keyValue, null);
            //Log.d("DELETE Handler::", "BEFORE RESULT check");

            if (deleteResult > 0) {
                //Log.d(" DELETE RESULT ::", Integer.toString(deleteResult));
                return deleteResult;

            } else {
                NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successor1_ID, successor2_ID, "DELETE_KEY");
                node.key = selection;
                node.originator = myPortID;
                String message = node.formMessage();

                //Log.d("Sending QUERY Request", message);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                //Log.d("PROPAGATE QUERY to ::", successor1_ID);

                while(!keyDeletedFromAll){

                }

                return 0;
            }
        }
	}

    public void deleteHandler(String selection, String originator){

        /* Handler to lookup the key on the current AVD and if not found then propogate to the next one */

        Log.d("DELETE - Selection ", selection);

        String keyValue = sqlhelper.COLUMN_1 +" ='"+selection+"'";
        int deleteResult = mydatabase.delete(TABLE_NAME, keyValue, null);

        Log.d("QUERY Handler::", "Before CURSOR check");
        NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successor1_ID, successor2_ID, "DELETE_FOUND");
        node.key = selection;
        node.originator = originator;
        node.value = Integer.toString(deleteResult);

        if(deleteResult > 0 || successor1_ID.equals(node.originator)) {

            Log.d(" DELETE RESULT ::", Integer.toString(deleteResult));
            String message = node.formMessage();

            Log.d("FOUND KEY", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("SEND CONFIRMATION to ::", originator);

        } else {

            node.msgType = "DELETE_KEY";
            String message = node.formMessage();
            Log.d("Sending DELETE Request", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("PROPAGATE DELETE to ::", successor1_ID);

        }
    }

    public int deleteAllHandler(String originator){

        Log.d("DELETE - ALL ", originator);

        int deleteResult = mydatabase.delete(TABLE_NAME, null, null);

        if(successor1_ID.equals(originator)) {
            return deleteResult;

        } else {

            NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successor1_ID, successor2_ID,  "DELETE_ALL");
            node.originator = originator;
            node.value = Integer.toString(deleteResult);
            node.msgType = "DELETE_ALL";
            String message = node.formMessage();
            Log.d("Sending DELETE Request", message);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
            Log.d("PROPAGATE DELETE to ::", successor1_ID);

        }
        return 0;
    }

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {

        //---------------Implementation using SQLite Data Storage Method----------------//
        try {

            Log.d("INSERT :: ", "Inserting : " + values.getAsString("key") + " " + myPortID + " " + successor1_ID);
            insertHandler(values);

        } catch (Exception e) {
            e.printStackTrace();
        }
        Log.v("insert", values.toString());
        return uri;
    }

    public void insertHandler(ContentValues values){
        try {
            Log.d("Insert", "More than 1 AVDs");
            String[] insertList = findPort(values.getAsString("key"));

            NodeMessage node1 = new NodeMessage(insertList[1], genHash(insertList[1]), insertList[0], insertList[2], insertList[3], "INSERT");
            node1.key = values.getAsString("key");
            node1.value = values.getAsString("value");
            node1.hashID = genHash(values.getAsString("key"));
            node1.time = Long.toString(System.currentTimeMillis());
            String message1 = node1.formMessage();
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message1);

            Log.d("REPLICATION :: ",message1);

        } catch (Exception e) {
            e.printStackTrace();
        }
	}

    public String[] findPort(String key){

        String [] portList = new String[3];

        try {
            for (int i = 0; i < REMOTE_PORTS.length; i++) {

                String port = REMOTE_PORTS[i];
                String pred, succ1, succ2;

                if (i == 0) {
                    pred = REMOTE_PORTS[4];
                } else {
                    pred = REMOTE_PORTS[i - 1];
                }

                if (i == 3) {
                    succ1 = REMOTE_PORTS[i + 1];
                    succ2 = REMOTE_PORTS[0];

                } else if (i == 4) {
                    succ1 = REMOTE_PORTS[0];
                    succ2 = REMOTE_PORTS[1];
                } else {
                    succ1 = REMOTE_PORTS[i + 1];
                    succ2 = REMOTE_PORTS[i + 2];
                }

                //String key = values.getAsString("key");
                String hashKey = genHash(key);
                String hashMyPort = genHash(port);
                String hashPredecessor = genHash(pred);

                /* Checks on the hash of key to verify if new key lies within the partition of current port */

                int check1 = hashKey.compareTo(hashPredecessor);
                int check2 = hashKey.compareTo(hashMyPort);
                int check3 = hashPredecessor.compareTo(hashMyPort);

                    /*Log.d(TAG,"************************************");
                    Log.d(TAG,""+ "KEY :: " + hashKey + "  MY PORT :: " + hashMyPort + "  PREDECESSOR :: "+ hashPredecessor + "  SUCCESSOR :: "+ hashSuccessor);
                    Log.d(TAG," check3 > 0 ::"+(check3 > 0));
                    Log.d(TAG," check1 < 0 && check2 < 0 :: "+(check1 < 0 && check2 < 0));
                    Log.d(TAG," check1 > 0 && check2 > 0 :: "+(check1 > 0 && check2 > 0));
                    Log.d(TAG," check1 > 0 && check2 < 0 :: "+(check1 > 0 && check2 < 0));

                    Log.d(TAG,"************************************");*/

                if (check3 > 0) {

                    // Edge cases : Predecessor is greater than current

                    if (check1 < 0 && check2 < 0) {
                        //Log.d(TAG, "Entering CONDITION 2 :: " + values.getAsString("key") + " " + values.getAsString("value"));
                        portList = new String[]{pred, port, succ1, succ2};
                        break;

                    } else if (check1 > 0 && check2 > 0) {

                        //Log.d(TAG, "Entering CONDITION 3 :: " + values.getAsString("key") + " " + values.getAsString("value"));
                        portList = new String[]{pred, port, succ1, succ2};
                        break;

                    } else {
                        // Pass the value to successor of the current node
                        continue;
                    }

                } else if (check1 > 0 && check2 < 0) {

                    // Ideal Case : Key is within the current partition i.e, greater than my predecessor and less than current

                    //Log.d(TAG, "Entering CONDITION 6 :: " + values.getAsString("key") + " " + values.getAsString("value"));
                    portList = new String[]{pred, port, succ1, succ2};
                    break;

                } else {

                    // Pass the value to successor of the current node
                    continue;

                }
            }
        } catch (Exception e){
            Log.d("FIND PORT ::", "**************** ERROR *****************");
        }
        return portList;
    }

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        /* All port information for the current AVD */
        myPortID = portStr;
        successor1_ID = portStr;
        predecessorID = portStr;

        /* Data storage via SQLite for the current AVD */
        sqlhelper = new SQLHelper(getContext());
        mydatabase = sqlhelper.getWritableDatabase();

        try {

            /* Starting the server for the current AVD */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            ringFormation();

            Log.d("ON CREATE :: ", "***********************RING FORMED***************");


        } catch (Exception e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        String[] columns = {"key", "value"};
        String keyValue = sqlhelper.COLUMN_1 + "=?";

        Cursor cursor = mydatabase.query(sqlhelper.TEST_TABLE, columns, keyValue, new String[]{"INITIAL_START"}, null, null, null);
        if(cursor.getCount() > 0) {
            NodeMessage node = new NodeMessage(myPortID, null, predecessorID, successor1_ID, successor2_ID, "GET_REPLICA");
            node.originator = myPortID;
            String message = node.formMessage();
            new RecoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

        } else {
            ContentValues cValues = new ContentValues();
            cValues.put("key", "INITIAL_START");
            cValues.put("value", "INITIAL_START");
            mydatabase.insert(sqlhelper.TEST_TABLE, null, cValues);

            try {

                if (myPortID.equals("5554")) {
                    TimeUnit.SECONDS.sleep(5);
                }
                if (myPortID.equals("5556")) {
                    TimeUnit.SECONDS.sleep(4);
                }
                if (myPortID.equals("5558")) {
                    TimeUnit.SECONDS.sleep(3);
                }

                if (myPortID.equals("5560")) {
                    TimeUnit.SECONDS.sleep(2);
                }

            } catch (Exception e){

                Log.d(TAG, "INSERT");
            }
        }
        return false;
	}

    public void ringFormation(){

        if (myPortID.equals("5554")){
            REMOTE_PORTS = new String[] {"5554", "5558", "5560", "5562", "5556"};

        } else if (myPortID.equals("5556")){
            REMOTE_PORTS = new String[] {"5556", "5554", "5558", "5560", "5562"};

        } else if (myPortID.equals("5558")){
            REMOTE_PORTS = new String[] {"5558", "5560", "5562", "5556", "5554"};

        } else if (myPortID.equals("5560")){
            REMOTE_PORTS = new String[] {"5560", "5562", "5556", "5554", "5558"};

        } else if (myPortID.equals("5562")){
            REMOTE_PORTS = new String[] {"5562", "5556", "5554", "5558", "5560"};

        }

        successor1_ID = REMOTE_PORTS[1];
        successor2_ID = REMOTE_PORTS[2];
        predecessorID = REMOTE_PORTS[4];

    }

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        //---------------Implementation using SQLite Data Storage Method----------------//

        Log.d("QUERY - Selection ", selection);
        SQLiteDatabase queryDatabase = sqlhelper.getReadableDatabase();

        try{

            if(selection.equals("@")) {
                // For Local Dump from current Node

                String selectQuery = "SELECT  * FROM " + TABLE_NAME;
                Cursor cursor = queryDatabase.rawQuery(selectQuery, null);
                //Log.d("Querying @ at ", myPortID);

                return cursor;

            } else if(selection.equals("*")) {
                // For Global Dump from all the Nodes

                Log.d("Querying * at ", myPortID);

                NodeMessage myDump = globalDumpHandler(myPortID);
                String message = myDump.formMessage();

                //Log.d("GLOBAL DUMP Request", message);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                Log.d("GLOBAL QUERY::", "************PROPAGATE REQUEST to other PORTS**********");

                while(!gotGlobalDump){

                }

                Log.d(TAG,"After the while loop in originator");

                String [] allKeys = globalDumpKeys.split("~~");
                String [] allValues = globalDumpValues.split("~~");

                Log.d(TAG,"In Query of * returned values length: "+allKeys.length+" "+allValues.length);
                MatrixCursor globalCursor = new MatrixCursor(new String[] { "key", "value"});

                for(int i = 0; i < allKeys.length; i++){
                    globalCursor.addRow(new String[] { allKeys[i], allValues[i]});
                }

                globalDumpKeys = "";
                globalDumpValues = "";

                return globalCursor;

            } else {

                // For Unique Selection Keys from current Node

                String[] columns = {"key", "value"};
                String keyValue = sqlhelper.COLUMN_1 + "=?";

                Cursor cursor = queryDatabase.query(TABLE_NAME, columns, keyValue, new String[]{selection}, null, null, null);

                //Log.d("QUERY Handler::", "Before CURSOR check");

                if(cursor != null && cursor.getCount() > 0) {
                    //Log.d(" QUERY", Integer.toString(cursor.getCount()));

                    if (cursor.moveToFirst())
                        //Log.d("CURSOR DATA :: ", cursor.getString(cursor.getColumnIndex("value")));

                        return cursor;

                } else {

                    String [] portList = findPort(selection);

                    NodeMessage node = new NodeMessage(portList[0], null, predecessorID, portList[1], portList[2], "UNIQUE_QUERY");
                    node.key = selection;
                    node.originator = myPortID;
                    String message = node.formMessage();

                    Log.d("UNIQUE QUERY Request", message);
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);

                    while(queryResult.equals("")){

                    }
                    MatrixCursor resultCursor = new MatrixCursor(new String[] { "key", "value"});
                    resultCursor.addRow(new String[] { selection, queryResult});
                    queryResult = "";

                    return resultCursor;
                }

            }

        }catch (Exception e) {
            e.printStackTrace();
        }

        Log.d(TAG, "***************QUERY*************** " + selection);
        return null;
    }

    public String uniqueQueryHandler(String selection, String originator){

        //---------------Implementation using SQLite Data Storage Method----------------//

        /*Log.d("UNIQUE QUERY ::", selection);
        String[] columns = {"key", "value"};
        SQLiteDatabase queryDatabase = sqlhelper.getReadableDatabase();

        String keyValue = sqlhelper.COLUMN_1 + "=?";
        Cursor cursor = queryDatabase.query(TABLE_NAME, columns, keyValue, new String[]{selection}, null, null, null);

        String result = " ";

        Log.d("QUERY Handler::", "Before CURSOR check");

        if(cursor != null && cursor.getCount() > 0) {
            Log.d(" QUERY", Integer.toString(cursor.getCount()));

            if (cursor.moveToFirst()) { // data?
                Log.d("CURSOR DATA :: ", cursor.getString(cursor.getColumnIndex("value")));
            }

            result = cursor.getString(cursor.getColumnIndex("value"));

        }

        return result;*/

        Log.d("UNIQUE QUERY ::", selection);
        String[] columns = {"key", "value", "created_at", "owner"};
        SQLiteDatabase queryDatabase = sqlhelper.getReadableDatabase();

        String keyValue = sqlhelper.COLUMN_1 + "=?";
        Cursor cursor = queryDatabase.query(TABLE_NAME, columns, keyValue, new String[]{selection}, null, null, null);

        String result = "NO_RESULT";

        Log.d("QUERY Handler::", "Before CURSOR check");

        if(cursor != null && cursor.getCount() > 0) {
            Log.d(" QUERY", Integer.toString(cursor.getCount()));

            if (cursor.moveToFirst()) {
                Log.d("CURSOR DATA :: ", cursor.getString(cursor.getColumnIndex("value")));
                Log.d("CURSOR DATA :: ", cursor.getString(cursor.getColumnIndex("created_at")));
            }

            result = cursor.getString(cursor.getColumnIndex("value")) +"~~"+ cursor.getString(cursor.getColumnIndex("created_at"));
        }

        Log.d("RESULT :: ", "**********************  *"+result+"  ***********************");

        return result;

    }

    public NodeMessage globalDumpHandler(String originator){

        SQLiteDatabase queryDatabase = sqlhelper.getReadableDatabase();
        String selectQuery = "SELECT  * FROM " + TABLE_NAME;
        Cursor myCursor = queryDatabase.rawQuery(selectQuery, null);

        String newKeys = "";
        String newValues = "";

        if (myCursor != null) {
            myCursor.moveToFirst();
            int keyIndex = myCursor.getColumnIndex("key");
            int valueIndex = myCursor.getColumnIndex("value");

            while (!myCursor.isAfterLast()) {
                newKeys = newKeys + myCursor.getString(keyIndex) + "~~";
                newValues = newValues + myCursor.getString(valueIndex) + "~~";
                myCursor.moveToNext();

            }
        }

        NodeMessage dump = new NodeMessage(myPortID, null, predecessorID, successor1_ID, successor2_ID, "GLOBAL_QUERY");
        dump.originator = originator;
        dump.key = newKeys;
        dump.value = newValues;

        return dump;

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            Log.d("I am in my Server ", myPortID);

            try {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    Log.d("Server", "Accept Socket");

                    DataInputStream din = new DataInputStream(clientSocket.getInputStream());
                    DataOutputStream dout = new DataOutputStream(clientSocket.getOutputStream());
                    String input1 = "";
                    NodeMessage toSend = new NodeMessage();

                    // Failure Handling for incoming initial input message

                    try {

                        input1 = din.readUTF();

                        Log.d("SERVER ::", "Reading Input " +input1);
                        toSend.breakMessage(input1);

                        dout.writeUTF("MESSAGE_READ");
                        Log.d("Server Msg", input1);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if(toSend.msgType.equals("GET_REPLICA")){

                        SQLiteDatabase queryDatabase = sqlhelper.getReadableDatabase();
                        String selectQuery = "SELECT  * FROM " + TABLE_NAME;
                        Cursor myCursor = queryDatabase.rawQuery(selectQuery, null);

                        String newKeys = "";
                        String newValues = "";
                        String newOwners = "";
                        String newTime = "";

                        if (myCursor != null) {
                            myCursor.moveToFirst();
                            int keyIndex = myCursor.getColumnIndex("key");
                            int valueIndex = myCursor.getColumnIndex("value");
                            int ownerIndex = myCursor.getColumnIndex("owner");
                            int timeIndex = myCursor.getColumnIndex("created_at");

                            while (!myCursor.isAfterLast()) {
                                newKeys = newKeys + myCursor.getString(keyIndex) + "~~";
                                newValues = newValues + myCursor.getString(valueIndex) + "~~";
                                newOwners = newOwners + myCursor.getString(ownerIndex) + "~~";
                                newTime = newTime + myCursor.getString(timeIndex) + "~~";
                                myCursor.moveToNext();

                            }
                        }

                        toSend.key = newKeys;
                        toSend.value = newValues;
                        toSend.originator = newOwners;
                        toSend.time = newTime;
                        String replica = toSend.formMessage();

                        dout.writeUTF(replica);
                        Log.d("SERVER REPLICA ::", replica);

                    }else if(toSend.msgType.equals("INSERT")){

                        Log.d(myPortID, "*****************INSERT Message*********************");

                        /*ContentValues cValues = new ContentValues();
                        cValues.put("key", toSend.key);
                        cValues.put("value", toSend.value);
                        cValues.put("owner", toSend.portID);

                        long row = mydatabase.insert(TABLE_NAME, null, cValues);

                        Log.d(myPortID, " "+Long.toString(row)+" "+toSend.key);*/

                        String[] columns = {"key", "value", "created_at", "owner"};

                        String keyValue = sqlhelper.COLUMN_1 + "=?";
                        Cursor cursor = mydatabase.query(TABLE_NAME, columns, keyValue, new String[]{toSend.key}, null, null, null);

                        ContentValues cValues = new ContentValues();
                        cValues.put("key", toSend.key);
                        cValues.put("value", toSend.value);
                        cValues.put("owner", toSend.portID);
                        cValues.put("created_at", toSend.time);

                        if (cursor != null && cursor.getCount() > 0) {
                            Log.d(" QUERY", Integer.toString(cursor.getCount()));

                            if (cursor.moveToFirst()) {
                                String time = cursor.getString(cursor.getColumnIndex("created_at"));

                                if (Long.parseLong(toSend.time) > Long.parseLong(time)){
                                    long row = mydatabase.update(TABLE_NAME, cValues, "key = '"+toSend.key+"'", null);
                                    Log.d("UPDATED REPLICA :: ", " " + Long.toString(row) + " " + toSend.key + "   " + toSend.value);
                                }
                            }
                        } else {
                            long row = mydatabase.insert(TABLE_NAME, null, cValues);
                            Log.d("INSERTED REPLICA :: ", " " + Long.toString(row) + " " + toSend.key + "   " + toSend.value);
                        }

                        Log.d(myPortID, "*****************INSERT DONE *********************");

                    } else if(toSend.msgType.equals("UNIQUE_QUERY")){

                        String result = uniqueQueryHandler(toSend.key, toSend.originator);
                        dout.writeUTF(result);
                        Log.d("SERVER QUERY ::", result);

                    } else if(toSend.msgType.equals("VALUE_FOUND")){

                        queryResult = toSend.value;

                    } else if(toSend.msgType.equals("GLOBAL_QUERY")){

                        NodeMessage myDump = globalDumpHandler(toSend.originator);
                        String dump = myDump.formMessage();

                        dout.writeUTF(dump);
                        Log.d("SERVER DUMP ::", dump);

                    } else if(toSend.msgType.equals("GLOBAL_FOUND")){

                        /*Log.d(TAG, "************************************************************************");
                        Log.d("FOUND SERVER KEY :: ", Integer.toString(toSend.key.length()) + " PORT :: "+ myPortID);
                        Log.d("FOUND SERVER VALUE :: ", Integer.toString(toSend.value.length()) + " PORT :: "+ myPortID);
                        Log.d(TAG, "************************************************************************");
*/
                        if(toSend.key != null) {
                            globalDumpKeys = toSend.key;
                            globalDumpValues = toSend.value;
                        } else {
                            globalDumpKeys = "test";
                            globalDumpValues = "test";
                        }
                        gotGlobalDump = true;


                    } else if(toSend.msgType.equals("DELETE_KEY")){

                        /*Log.d(TAG, "************************************************************************");
                        Log.d("DELETE SERVER KEY :: ", toSend.key + " PORT :: "+ myPortID);
                        Log.d(TAG, "************************************************************************");

*/                      String keyValue = sqlhelper.COLUMN_1 +" ='"+toSend.key+"'";
                        int deleteResult = mydatabase.delete(TABLE_NAME, keyValue, null);
                        //Log.d("DELETE Handler::", "BEFORE RESULT check");

                        dout.writeUTF("DELETE_KEY_DONE");

                        //deleteHandler(toSend.key, toSend.originator);

                    } else if(toSend.msgType.equals("DELETE_ALL")){

                        /*Log.d(TAG, "************************************************************************");
                        Log.d("FOUND DELETE KEY :: ", toSend.key + " PORT :: "+ myPortID);
                        Log.d("FOUND DELETE VALUE :: ", toSend.value + " PORT :: "+ myPortID);
                        Log.d(TAG, "************************************************************************");
*/
                        deleteAllHandler(toSend.originator);

                    }

                    din.close();;
                    dout.close();
                }

            } catch(Exception e)
            {   e.printStackTrace();
                Log.e(TAG,"Server Socket Error");
            }
            return null;
        }

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... data) {

            try {

                String msgToSend = data[0];
                Log.d(TAG, myPortID + " " + successor1_ID + " " + predecessorID);
                NodeMessage msgDetails = new NodeMessage();
                msgDetails.breakMessage(msgToSend);

                if(msgDetails.msgType.equals("INSERT")) {

                    String [] insertlist = new String[] {msgDetails.portID, msgDetails.successor1, msgDetails.successor2};
                    Log.d("CLIENT INSERT :: ", msgToSend);

                    for(int j = 0; j < insertlist.length; j++){

                        Log.d("INSERT NUMBER :: ", Integer.toString(j) + " " + msgToSend);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(insertlist[j]) * 2);
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        try{
                            Log.d("BEFORE SENDING :: ", Integer.toString(j) + " " + msgToSend);
                            dout.writeUTF(msgToSend);

                        } catch (Exception e){
                            e.printStackTrace();
                        }

                        Log.d("AFTER SENDING :: ", Integer.toString(j) + " " + msgToSend);

                        DataInputStream din = new DataInputStream(socket.getInputStream());
                        String ack;

                        try{
                            ack = din.readUTF();
                            if(ack.equals("MESSAGE_READ")) {
                                dout.close();
                                socket.close();
                            }

                        } catch (Exception e){
                            e.printStackTrace();
                            continue;
                        }

                    }

                } else if(msgDetails.msgType.equals("UNIQUE_QUERY")){

                    /*Log.d("CLIENT UNIQUE :: ", msgToSend);

                    String [] insertlist = new String[] {msgDetails.portID, msgDetails.successor1, msgDetails.successor2};
                    String queryValue = " ";

                    for(int j = 0; j < insertlist.length; j++){

                        Log.d("QUERY NUMBER :: ", Integer.toString(j) + " " + msgToSend);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(insertlist[j]) * 2);
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        try{
                            Log.d("BEFORE SENDING :: ", Integer.toString(j) + " " + msgToSend);
                            dout.writeUTF(msgToSend);

                        } catch (Exception e){
                            e.printStackTrace();
                        }

                        Log.d("AFTER SENDING :: ", Integer.toString(j) + " " + msgToSend);

                        DataInputStream din = new DataInputStream(socket.getInputStream());
                        String ack, result;

                        try{
                            ack = din.readUTF();
                            Log.d("UNIQUE QUERY:: ", ack);

                            result = din.readUTF();
                            Log.d("UNIQUE QUERY:: ", result);

                            queryValue = result;

                            if(!queryValue.equals(" ")){
                                queryResult = queryValue;
                                break;
                            }

                        } catch (Exception e){
                            e.printStackTrace();
                            continue;
                        }

                        dout.close();
                        din.close();
                        socket.close();

                    }*/

                    Log.d("CLIENT UNIQUE :: ", msgToSend);

                    String [] insertlist = new String[] {msgDetails.portID, msgDetails.successor1, msgDetails.successor2};
                    String [] queryValueTime = new String []{"FIRST", "FIRST"};

                    for(int j = 0; j < insertlist.length; j++){

                        Log.d("QUERY NUMBER :: ", Integer.toString(j) + " " + msgToSend);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(insertlist[j]) * 2);
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        try{
                            Log.d("BEFORE SENDING :: ", Integer.toString(j) + " " + msgToSend);
                            dout.writeUTF(msgToSend);

                        } catch (Exception e){
                            e.printStackTrace();
                        }

                        Log.d("AFTER SENDING :: ", Integer.toString(j) + " " + msgToSend);

                        DataInputStream din = new DataInputStream(socket.getInputStream());
                        String ack, result;

                        try{
                            ack = din.readUTF();
                            Log.d("UNIQUE QUERY:: ", ack);

                            result = din.readUTF();
                            Log.d("UNIQUE QUERY:: ", result);

                            String[] temp = result.split("~~");

                            if(!result.equals("NO_RESULT")){
                                if(queryValueTime[0].equals("FIRST")){
                                    queryValueTime[0] = temp[0];
                                    queryValueTime[1] = temp[1];

                                } else {
                                    if (Long.parseLong(temp[1]) > Long.parseLong(queryValueTime[1])) {
                                        queryValueTime[0] = temp[0];
                                        queryValueTime[1] = temp[1];
                                    }
                                }
                            }

                        } catch (Exception e){
                            e.printStackTrace();
                            continue;
                        }

                        dout.close();
                        din.close();
                        socket.close();
                    }
                    queryResult = queryValueTime[0];

                } else if(msgDetails.msgType.equals("VALUE_FOUND")){

                    Log.d("VALUE FOUND :: ", msgToSend);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgDetails.originator) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                } else if(msgDetails.msgType.equals("GLOBAL_QUERY")) {

                    Log.d("GLOBAL QUERY :: ", msgToSend);

                    Log.d(TAG, "************************************************************************");
                    Log.d("QUERY CLIENT KEY :: ", Integer.toString(msgDetails.key.length()) + " PORT :: "+ myPortID+" Successor ID: "+msgDetails.successor1);
                    Log.d("QUERY CLIENT VALUE :: ", Integer.toString(msgDetails.value.length()) + " PORT :: "+ myPortID);
                    Log.d(TAG, "************************************************************************");

                    String dumpKeys = "";
                    String dumpValues = "";

                    for(int j = 1; j < REMOTE_PORTS.length; j++){

                        Log.d("GLOBAL QUERY :: ", Integer.toString(j) + " " + msgToSend);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS[j]) * 2);
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        try{
                            Log.d("BEFORE SENDING :: ", Integer.toString(j) + " " + msgToSend);
                            dout.writeUTF(msgToSend);

                        } catch (Exception e){
                            e.printStackTrace();
                        }

                        Log.d("AFTER SENDING :: ", Integer.toString(j) + " " + msgToSend);

                        DataInputStream din = new DataInputStream(socket.getInputStream());
                        String ack, dump;

                        try{
                            ack = din.readUTF();
                            Log.d("DUMP RECEIVED:: ", ack);

                            dump = din.readUTF();
                            Log.d("DUMP RECEIVED:: ", dump);


                        } catch (Exception e){
                            e.printStackTrace();
                            continue;
                        }

                        NodeMessage temp = new NodeMessage();
                        temp.breakMessage(dump);

                        dumpKeys = dumpKeys + temp.key;
                        dumpValues = dumpValues + temp.value;

                        dout.close();
                        din.close();
                        socket.close();

                    }

                    globalDumpKeys = msgDetails.key + dumpKeys;
                    globalDumpValues = msgDetails.value + dumpValues;
                    gotGlobalDump = true;


                }  else if(msgDetails.msgType.equals("GLOBAL_FOUND")){

                    Log.d("GLOBAL FOUND :: ", msgToSend);

                    Log.d(TAG, "************************************************************************");
                    Log.d("FOUND CLIENT KEY :: ", Integer.toString(msgDetails.key.length()) + " PORT :: "+ myPortID+" Originator ID:"+msgDetails.originator);
                    Log.d("FOUND CLIENT VALUE :: ", Integer.toString(msgDetails.value.length()) + " PORT :: "+ myPortID);
                    Log.d(TAG, "************************************************************************");


                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgDetails.originator) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                } else if(msgDetails.msgType.equals("DELETE_KEY")){

                    //Log.d("DELETE_KEY :: ", msgToSend);

                    /*Log.d(TAG, "************************************************************************");
                    Log.d("DELETE CLIENT KEY :: ", Integer.toString(msgQuery.key.length()) + " PORT :: "+ myPortID+" Successor ID:"+msgQuery.successor);
                    Log.d("DELETE CLIENT VALUE :: ", Integer.toString(msgQuery.value.length()) + " PORT :: "+ myPortID);
                    Log.d(TAG, "************************************************************************");
*/
                    /*Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgDetails.successor1) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }*/

                    for(int j = 1; j < REMOTE_PORTS.length; j++){

                        Log.d("DELETE KEY :: ", Integer.toString(j) + " " + msgToSend);

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS[j]) * 2);
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        try{
                            dout.writeUTF(msgToSend);

                        } catch (Exception e){
                            e.printStackTrace();
                        }

                        DataInputStream din = new DataInputStream(socket.getInputStream());
                        String ack, delete_ack;

                        try{
                            ack = din.readUTF();
                            Log.d("DELETE KEY :: ", ack);

                            delete_ack = din.readUTF();
                            if(delete_ack.equals("DELETE_KEY_DONE")){
                                dout.close();
                                din.close();
                                socket.close();
                            }

                        } catch (Exception e){
                            e.printStackTrace();
                            continue;
                        }

                    }

                    keyDeletedFromAll = true;

                } else if(msgDetails.msgType.equals("DELETE_FOUND")){

                    //Log.d("DELETE_FOUND :: ", msgToSend);

                    /*Log.d(TAG, "************************************************************************");
                    Log.d("FOUND DELETE KEY :: ", Integer.toString(msgQuery.key.length()) + " PORT :: "+ myPortID+" Originator ID:"+msgQuery.originator);
                    Log.d("FOUND DELETE VALUE :: ", Integer.toString(msgQuery.value.length()) + " PORT :: "+ myPortID);
                    Log.d(TAG, "************************************************************************");
*/
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgDetails.originator) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                } else if(msgDetails.msgType.equals("DELETE_ALL")){

                    /*Log.d(TAG, "************************************************************************");
                    Log.d("FOUND DELETE KEY :: ", Integer.toString(msgQuery.key.length()) + " PORT :: "+ myPortID+" Originator ID:"+msgQuery.originator);
                    Log.d("FOUND DELETE VALUE :: ", Integer.toString(msgQuery.value.length()) + " PORT :: "+ myPortID);
                    Log.d(TAG, "************************************************************************");
*/
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgDetails.successor1) * 2);
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

                    try{
                        dout.writeUTF(msgToSend);

                    } catch (Exception e){
                        e.printStackTrace();;
                    }

                    DataInputStream din = new DataInputStream(socket.getInputStream());
                    String ack = din.readUTF();
                    if(ack.equals("MESSAGE_READ")) {
                        dout.close();
                        din.close();
                        socket.close();
                    }

                }

            } catch (Exception e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }
    }

    private class RecoveryTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... data) {

            try {

                String msgToSend = data[0];
                Log.d(TAG, myPortID + " " + successor1_ID + " " + predecessorID);
                NodeMessage msgDetails = new NodeMessage();
                msgDetails.breakMessage(msgToSend);

                if(msgDetails.msgType.equals("GET_REPLICA")) {

                    sqlhelper = new SQLHelper(getContext());
                    mydatabase = sqlhelper.getWritableDatabase();

                    for(int j = 1; j < REMOTE_PORTS.length; j++){

                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS[j]) * 2);
                        DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                        try{
                            dout.writeUTF(msgToSend);
                            Log.d("GET REPLICA :: ", Integer.toString(j) + " " + msgToSend);

                            DataInputStream din = new DataInputStream(socket.getInputStream());
                            String ack = din.readUTF();
                            Log.d("RECOVERY :: ", ack);


                            String replica = din.readUTF();
                            Log.d("REPLICA RECEIVED:: ", replica);

                            NodeMessage temp = new NodeMessage();
                            temp.breakMessage(replica);

                            Log.d("GET REPLICA :: ", Integer.toString(j) + " ");

                            String [] allKeys = temp.key.split("~~");
                            String [] allValues = temp.value.split("~~");
                            String [] allOwners = temp.originator.split("~~");
                            String [] allTime = temp.time.split("~~");

                            /*for(int i = 0; i < allKeys.length; i++){

                                if(allOwners[i].equals(myPortID) || allOwners[i].equals(REMOTE_PORTS[3]) || allOwners[i].equals(REMOTE_PORTS[4])){

                                    ContentValues cValues = new ContentValues();
                                    cValues.put("key", allKeys[i]);
                                    cValues.put("value", allValues[i]);
                                    cValues.put("owner", allOwners[i]);

                                    long row = mydatabase.insert(sqlhelper.TABLE_NAME, null, cValues);

                                    Log.d("INSERT REPLICA :: ", " " +Long.toString(row) +" "+allKeys[i] +"   " +allValues[i]);
                                }

                            }*/

                            for (int i = 0; i < allKeys.length; i++) {

                                if (allOwners[i].equals(myPortID) || allOwners[i].equals(REMOTE_PORTS[3]) || allOwners[i].equals(REMOTE_PORTS[4])) {

                                    String[] columns = {"key", "value", "created_at", "owner"};

                                    String keyValue = sqlhelper.COLUMN_1 + "=?";
                                    Cursor cursor = mydatabase.query(TABLE_NAME, columns, keyValue, new String[]{allKeys[i]}, null, null, null);

                                    ContentValues cValues = new ContentValues();
                                    cValues.put("key", allKeys[i]);
                                    cValues.put("value", allValues[i]);
                                    cValues.put("owner", allOwners[i]);
                                    cValues.put("created_at", allTime[i]);

                                    if (cursor != null && cursor.getCount() > 0) {
                                        Log.d(" QUERY", Integer.toString(cursor.getCount()));

                                        if (cursor.moveToFirst()) {
                                            String time = cursor.getString(cursor.getColumnIndex("created_at"));

                                            if (Long.parseLong(allTime[i]) < Long.parseLong(time)){
                                                continue;
                                            } else {
                                                long row = mydatabase.update(TABLE_NAME, cValues, "key = '"+allKeys[i]+"'", null);
                                                Log.d("UPDATED REPLICA :: ", " " + Long.toString(row) + " " + allKeys[i] + "   " + allValues[i]);
                                            }

                                        }

                                    } else {
                                        long row = mydatabase.insert(TABLE_NAME, null, cValues);
                                        Log.d("INSERTED REPLICA :: ", " " + Long.toString(row) + " " + allKeys[i] + "   " + allValues[i]);
                                    }
                                }
                            }

                        } catch (Exception e){
                            e.printStackTrace();
                            continue;
                        }

                    }

                }

            } catch (Exception e) {
                Log.e(TAG, "RecoveryTask socket IOException");
                e.printStackTrace();
            }

            return null;
        }
    }
}