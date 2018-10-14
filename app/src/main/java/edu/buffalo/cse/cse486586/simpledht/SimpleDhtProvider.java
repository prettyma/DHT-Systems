package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;


//Reference:  Adding cursors : https://developer.android.com/reference/android/database/MatrixCursor.html
//Reference:  Sending arrays in JSONObject  https://developer.android.com/reference/org/json/JSONArray.html
public class SimpleDhtProvider extends ContentProvider {

    public static enum MESSAGE_ENUM {
        JOIN, UPDATE_NODE, INSERT, INSERT_ALL, QUERY, QUERY_BACK, QUERY_ALL, DELETE_ALL, DELETE
    }

    public static enum UPDATE_NODE_ENUM {
        NEW_SUCC, NEW_PREV, NEW_CURR
    }

    ChordDBHelper chordDBHelper;
    SQLiteDatabase sqLiteDatabase;
    static final String REMOTE_PORT0 = "11108";
    static final int SERVER_PORT = 10000;
    static String myPort;
    static String myPredecessor;
    static String mySuccessor;

    static final String CONTENT_KEY = "key";
    static final String CONTENT_VALUE = "value";


    //MESSAGE KEYS FOR JSON OBJECT
    static final String MESSAGE_TYPE = "type";
    static final String JOINING_PORT = "myPort";
    static final String CURRENT_PORT = "myPort";
    static final String SUCCESSOR_PORT = "mySuccessor";
    static final String PREDECESSOR_PORT = "myPredecessor";
    static final String UPDATE_NODE = "updateNode";
    static final String SELECTION = "selection";
    static final String QUERY_PORT = "queryPort";
    static final String DELETE_PORT = "deletePort";

    TreeMap<String, String> activeNodeMap;

    Comparator<String> chordRingComparator;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        //TODO: DELETE based on the selection string
        //TODO: handle multiple selection arguments
        if(selection.compareTo("@") == 0) {
            chordDBHelper.deleteDBHelperAllKey(sqLiteDatabase);
        } else if(selection.compareTo("*")==0) {
            chordDBHelper.deleteDBHelperAllKey(sqLiteDatabase);
            //Log.v("QUERY_ALL","with selection" +selection);
            if(myPredecessor==null || mySuccessor==null){
                return 1;
            } else {
                try{
                    if(selectionArgs!=null){
                        //Log.v("DELETE_ALL","not first time with delete port");
                        //query all
                        String deletePort = selectionArgs[0];
                        if(deletePort.compareTo(mySuccessor)==0){
                            //Log.v("DELETE_ALL","found my Successor = delete port");
                            return 1;
                        } else {
                            //Log.v("DELETE_ALL","Did not find my Successor = delete port, so moving to my successor");
                            String msgToSend = createJsonMsgForDeleteAllToSuccessor(selection,deletePort);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend,mySuccessor);
                            return 1;
                        }
                    } else {
                        //query all with actual query port
                        //Log.v("DELETE_ALL","first time with delete port:"+myPort);
                        String msgToSend = createJsonMsgForDeleteAllToSuccessor(selection,myPort);
                        //msgReceived = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend,mySuccessor).get().toString();
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend,mySuccessor);
                        //Log.v("DELETE_ALL","Received all delete back");
                        return 1;
                    }
                } catch (Exception e){
                    e.printStackTrace();
                    //Log.v("DELETE_ALL","error occurred while sending delete All message to Successor");
                }
            }
        } else {
            String genKey = null, predID = null, myID = null;
            if(myPredecessor!=null || mySuccessor!=null){
                try {
                    genKey = genHash(selection);
                    predID = getPortHash(myPredecessor);
                    myID = getPortHash(myPort);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                if ((predID.compareTo(myID) > 0 && (genKey.compareTo(predID) > 0 || genKey.compareTo(myID) < 0)) || (genKey.compareTo(predID) > 0 && genKey.compareTo(myID) <= 0)) {
                    chordDBHelper.deleteDBHelperKey(sqLiteDatabase, selection);
                    //Log.v("DELETE","deleted data from DB in query()");

                } else {
                    //Log.v("DELETE", "Not correct node passing to successor port:" + mySuccessor);
                    String msgToSend = createJsonMsgForDeleteToSuccessor(selection, myPort);
                    //String msgReceived = sendMessageAndWait(msgToSend, mySuccessor);
                    //String msgReceived = null;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend,mySuccessor);
                    //Log.v("DELETE", "Msg sent for deletion to successor ");
                    //TODO: Extract cursor and return
                }
            } else {
                chordDBHelper.deleteDBHelperKey(sqLiteDatabase, selection);
                return 1;
            }
        }
        //chordDBHelper.deleteDBHelperKey(sqLiteDatabase, "");

        return 1;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        //TODO: hash the key and insert to the right node
        String key = values.get(CONTENT_KEY).toString();
        String genKey = "";
        try {
            genKey = genHash(key);


            if(myPredecessor==null || mySuccessor==null){
                chordDBHelper.insertDBHelper(sqLiteDatabase, values);
                //Log.v("INSERT:", "found port to insert at :" + myPort);
            }
            String predID = getPortHash(myPredecessor);
            String myID = getPortHash(myPort);
            if ((predID.compareTo(myID) > 0 && (genKey.compareTo(predID) > 0 || genKey.compareTo(myID) < 0)) || (genKey.compareTo(predID) > 0 && genKey.compareTo(myID) <= 0)) {
                //case 1: if myPort hash is the first in the chord ring then the predecessor port will be greater
                //      a) when the key is lesser than myID
                //      b) when the key is greater than myID
                //case 2: if key is between predecessor and my port
                chordDBHelper.insertDBHelper(sqLiteDatabase, values);
                //Log.v("INSERT:", "found port to insert at :" + myPort);
            } else {
                // send to the successor the content values
                //Log.v("INSERT", "Not correct node passing to successor port:" + mySuccessor);
                String msgToSend = createJsonMsgForInsertToSuccessor(key, values.get(CONTENT_VALUE).toString());
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, mySuccessor);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        chordDBHelper = new ChordDBHelper(getContext(), null, null, 0);
        sqLiteDatabase = chordDBHelper.getWritableDatabase();

        chordRingComparator = new Comparator<String>() {
            public int compare(String hash1, String hash2) {
                return hash1.compareTo(hash2);
            }
        };
        activeNodeMap = new TreeMap<String, String>(chordRingComparator);

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        //Test RE-BALANCING when node join
        /*if(myPort==REMOTE_PORT0){
            testJoin();
        }*/

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.d("On Create", "Can't create a ServerSocket");
            return false;
        }

        //Log.v("OnCreate", "Port: " + myPort);
        String joinMsgToSend = createJSONMsgForJoin(myPort).toString();
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, joinMsgToSend, REMOTE_PORT0);
        //Log.v("OnCreate","Port: "+myPort+" | Sent join message: "+ joinMsgToSend);
        return true;
    }

    private void testJoin() {
        int TEST_CNT =20;
        ContentValues[] cv = new ContentValues[TEST_CNT];
        for (int i = 0; i < TEST_CNT; i++) {
            cv[i] = new ContentValues();
            cv[i].put(CONTENT_KEY, "key" + Integer.toString(i));
            cv[i].put(CONTENT_VALUE, "val" + Integer.toString(i));
        }
        try {
            for (int i = 0; i < TEST_CNT; i++) {
                insert(null, cv[i]);
            }
        } catch (Exception e) {
            Log.e("TEST JOIN", e.toString());
            return;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        //Log.v("QUERY_ALL","Reached Query with " +selection);
        Cursor cursorQuery = chordDBHelper.queryDBHelperAllKey(sqLiteDatabase);
        if (selection.compareTo("@") == 0) {
            return cursorQuery;
        } else if (selection.compareTo("*") == 0) {
            //Start of *
            //Log.v("QUERY_ALL","with selection" +selection);
            if(myPredecessor==null || mySuccessor==null){
                return cursorQuery;
            } else {
                try{
                    String msgReceived=null;
                    if(selectionArgs!=null){
                        //Log.v("QUERY_ALL","not first time with query port");
                        //query all
                        String queryP = selectionArgs[0];
                        if(queryP.compareTo(mySuccessor)==0){
                            //Log.v("QUERY_ALL","found my Successor = query port");
                            return cursorQuery;
                        } else {
                            //Log.v("QUERY_ALL","Did not find my Successor = query port, so moving to my successor");
                            String msgToSend = createJsonMsgForQueryAllToSuccessor(selection,queryP);
                            Object obj = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend,mySuccessor).get();
                            msgReceived = obj!=null?obj.toString():null;
                            MatrixCursor cursorFinal;
                            if(msgReceived!=null){
                                cursorFinal = getCursorFromJSONArray(msgReceived);
                            } else {

                                cursorFinal = new MatrixCursor(new String[] {CONTENT_KEY,CONTENT_VALUE});
                            }
                            cursorQuery.moveToFirst();
                            while (!cursorQuery.isAfterLast()) {
                                Object[] values = {cursorQuery.getString(cursorQuery.getColumnIndex(CONTENT_KEY)), cursorQuery.getString(cursorQuery.getColumnIndex(CONTENT_VALUE))};
                                cursorFinal.addRow(values);
                                cursorQuery.moveToNext();
                            }
                            return cursorFinal;

                        }
                    } else {
                        //query all with actual query port
                        //Log.v("QUERY_ALL","first time with query port:"+myPort);
                        String msgToSend = createJsonMsgForQueryAllToSuccessor(selection,myPort);
                        //msgReceived = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend,mySuccessor).get().toString();
                        Object obj = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend,mySuccessor).get();
                        msgReceived = obj!=null?obj.toString():null;
                        //Log.v("QUERY_ALL","Received all queries back");
                        MatrixCursor cursorFinal;
                        if(msgReceived!=null){
                            cursorFinal = getCursorFromJSONArray(msgReceived);
                            //Log.v("QUERY_ALL","cursor received from: "+mySuccessor+" "+ DatabaseUtils.dumpCursorToString(cursorFinal));
                        } else {
                            cursorFinal = new MatrixCursor(new String[] {CONTENT_KEY,CONTENT_VALUE});
                        }
                        cursorQuery.moveToFirst();
                        while (!cursorQuery.isAfterLast()) {
                            Object[] values = {cursorQuery.getString(0), cursorQuery.getString(1)};
                            cursorFinal.addRow(values);
                            cursorQuery.moveToNext();
                        }
                        return cursorFinal;
                    }
                } catch (InterruptedException e){
                    e.printStackTrace();
                    //Log.v("Query all","error occurred while sending Query All message to Successor");
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
            //end of *
        } else {
            //Single key
            String genKey = null, predID = null, myID = null;
            if(myPredecessor!=null || mySuccessor!=null){
                try {
                    genKey = genHash(selection);
                    predID = getPortHash(myPredecessor);
                    myID = getPortHash(myPort);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                if ((predID.compareTo(myID) > 0 && (genKey.compareTo(predID) > 0 || genKey.compareTo(myID) < 0)) || (genKey.compareTo(predID) > 0 && genKey.compareTo(myID) <= 0)) {
                    cursorQuery = chordDBHelper.queryDBHelperSingleKey(sqLiteDatabase, selection);
                    //Log.v("QUERY","retreived data from DB in query()");
                } else {
                    //Log.v("QUERY", "Not correct node passing to successor port:" + mySuccessor);
                    String msgToSend = createJsonMsgForQueryToSuccessor(selection, myPort);
                    //String msgReceived = sendMessageAndWait(msgToSend, mySuccessor);
                    String msgReceived = null;
                    try {
                        msgReceived = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgToSend,mySuccessor).get().toString();
                        MatrixCursor cursorFinal = getCursorFromJSONArray(msgReceived);
                        //Log.v("QUERY", "Msg received after sending and waiting: "+msgReceived);
                        return cursorFinal;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                cursorQuery = chordDBHelper.queryDBHelperSingleKey(sqLiteDatabase, selection);
            }
        }
        return cursorQuery;
    }

    private MatrixCursor getCursorFromJSONArray(String msgReceived) throws JSONException {
        JSONObject jsonReceived = new JSONObject(msgReceived);
        JSONArray keyArray = jsonReceived.getJSONArray(CONTENT_KEY);
        JSONArray valueArray = jsonReceived.getJSONArray(CONTENT_VALUE);
        MatrixCursor cursorFinal = new MatrixCursor(new String[] {CONTENT_KEY,CONTENT_VALUE});
        int i=0;
        while(i<keyArray.length()){
            cursorFinal.addRow(new Object[]{keyArray.getString(i), valueArray.getString(i)});
            i++;
        }
        cursorFinal.close();
        return cursorFinal;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, Void, Void> {

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {
            ServerSocket serverSocket = serverSockets[0];
            Socket socket;
            try {
                while (true) {
                    socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String s = in.readLine();
                    if (s != null) {
                        //Log.v("Server Task", "Message received " + s);
                        JSONObject msg = new JSONObject(s);
                        MESSAGE_ENUM message_type = MESSAGE_ENUM.valueOf(msg.getString(MESSAGE_TYPE));
                        switch (message_type) {
                            case JOIN:
                                String joiningPort = msg.getString(JOINING_PORT);
                                //Log.v("Server Task",Integer.toString(Integer.parseInt(joiningPort)/2));
                                String joinPortHash = getPortHash(joiningPort);
                                activeNodeMap.put(joinPortHash, joiningPort);
                                int ringSize = activeNodeMap.size();
                                //Log.v("Server Task", "Ring size is " + ringSize);
                                if (ringSize > 1) {
                                    findSuccPredNodes(joinPortHash);
                                }
                                break;
                            case UPDATE_NODE:
                                //Log.v("Server Task", "UPDATE NODE received");
                                UPDATE_NODE_ENUM updateType = UPDATE_NODE_ENUM.valueOf(msg.getString(UPDATE_NODE));
                                //Log.v("Server Task", "UPDATE NODE received with type"+updateType);
                                switch (updateType) {
                                    case NEW_SUCC:
                                        mySuccessor = msg.getString(SUCCESSOR_PORT);
                                        break;
                                    case NEW_PREV:
                                        myPredecessor = msg.getString(PREDECESSOR_PORT);
                                        int predPort = Integer.parseInt(myPredecessor)/2,i=0;
                                        String myPredHash = genHash(Integer.toString(predPort));
                                        Cursor cursor = query(null,null,"@",null,null);
                                        JSONArray keyArray = new JSONArray();
                                        JSONArray valueArray = new JSONArray();
                                        while (cursor.moveToNext()) {
                                            String key = cursor.getString(cursor.getColumnIndex(CONTENT_KEY));
                                            String hashKey = genHash(key);
                                            if(hashKey.compareTo(myPredHash)<=0){
                                                keyArray.put(i,cursor.getString(cursor.getColumnIndex(CONTENT_KEY)));
                                                valueArray.put(i,cursor.getString(cursor.getColumnIndex(CONTENT_VALUE)));
                                                delete(null,cursor.getString(cursor.getColumnIndex(CONTENT_KEY)),null);
                                            }
                                        }

                                        JSONObject msgToSend = new JSONObject();
                                        //msg
                                        msgToSend.put(MESSAGE_TYPE,MESSAGE_ENUM.INSERT_ALL);
                                        msgToSend.put(CONTENT_KEY,keyArray);
                                        msgToSend.put(CONTENT_VALUE,valueArray);
                                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend.toString(), myPredecessor);
                                        break;
                                    case NEW_CURR:
                                        mySuccessor = msg.getString(SUCCESSOR_PORT);
                                        myPredecessor = msg.getString(PREDECESSOR_PORT);
                                        ////Log.v("Update nodes","myPredecessor "+myPredecessor+" myPort "+myPort+" mySuccessor "+mySuccessor);
                                }
                                //Log.v("Server Task", "End of nodes update pred:" + myPredecessor + " curr:" + myPort + " succ:" + mySuccessor);
                                break;
                            case INSERT:
                                //Log.v("Server Task", "Received insert query");
                                String key = msg.getString(CONTENT_KEY);
                                String value = msg.getString(CONTENT_VALUE);
                                ContentValues values = new ContentValues();
                                values.put(CONTENT_KEY, key);
                                values.put(CONTENT_VALUE, value);
                                insert(null, values);
                                //Log.v("Server Task", "Insert msg passing to content provider's insert");
                                break;
                            case QUERY:
                                //Log.v("Server Task", "Received query msg");
                                String selection = msg.getString(SELECTION);
                                Cursor cursor = query(null, null, selection, null, null);

                                JSONArray keyArray = new JSONArray();
                                JSONArray valueArray = new JSONArray();
                                cursor.moveToFirst();
                                int index = 0;
                                while (!cursor.isAfterLast()) {
                                    keyArray.put(index, cursor.getString(cursor.getColumnIndex(CONTENT_KEY)));
                                    valueArray.put(index, cursor.getString(cursor.getColumnIndex(CONTENT_VALUE)));
                                    index++;
                                    cursor.moveToNext();
                                    //Log.v("Server Task","Query result received from DB");
                                }

                                JSONObject jsonMsg = new JSONObject();
                                jsonMsg.put(CONTENT_KEY, keyArray);
                                jsonMsg.put(CONTENT_VALUE, valueArray);
                                jsonMsg.put(MESSAGE_TYPE, MESSAGE_ENUM.QUERY_BACK);
                                //sendMessage(jsonMsg.toString(), myPredecessor);
                                //Log.v("Server Task","before sending query back");
                                socket.close();
                                socket = serverSocket.accept();
                                BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                                out.write(jsonMsg.toString());
                                out.flush();
                                out.close();
                                cursor.close();
                                break;
                            case QUERY_ALL:
                                //socket.close();
                                //Start query all
                                //Log.v("Server Task", "Received query msg");
                                String select = msg.getString(SELECTION);
                                String queryPort = msg.getString(QUERY_PORT);
                                //Log.v("Server Task","QUERY_ALL "+select+" from query port:"+queryPort);
                                Cursor cursorAll = query(null, null, select, new String[]{queryPort}, null);
                                JSONObject jsonMsgAll = new JSONObject();
                                JSONArray keyArrayAll = new JSONArray();
                                JSONArray valueArrayAll = new JSONArray();
                                cursorAll.moveToFirst();
                                int j = 0;
                                while (!cursorAll.isAfterLast()) {
                                    keyArrayAll.put(j, cursorAll.getString(cursorAll.getColumnIndex(CONTENT_KEY)));
                                    valueArrayAll.put(j, cursorAll.getString(cursorAll.getColumnIndex(CONTENT_VALUE)));
                                    j++;
                                    cursorAll.moveToNext();
                                }
                                //Log.v("Server Task","Query result received from DB");
                                jsonMsgAll.put(CONTENT_KEY, keyArrayAll);
                                jsonMsgAll.put(CONTENT_VALUE, valueArrayAll);
                                jsonMsgAll.put(MESSAGE_TYPE, MESSAGE_ENUM.QUERY_BACK);
                                //sendMessage(jsonMsg.toString(), myPredecessor);
                                //Log.v("Server Task","before sending query back");
                                socket.close();
                                socket = serverSocket.accept();
                                BufferedWriter outAll = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                                outAll.write(jsonMsgAll.toString());
                                outAll.flush();
                                outAll.close();
                                cursorAll.close();
                                //End query all
                                break;
                            case DELETE:
                                String deleteSelect = msg.getString(SELECTION);
                                delete(null, deleteSelect,null);
                                break;
                            case DELETE_ALL:
                                String deleteSelectAll = msg.getString(SELECTION);
                                delete(null, deleteSelectAll,new String[]{msg.getString(DELETE_PORT)});
                                break;
                            case INSERT_ALL:
                                //Log.v("Server Task", "Received insert query");
                                JSONArray insertKeyArray = msg.getJSONArray(CONTENT_KEY);
                                JSONArray insertValueArray = msg.getJSONArray(CONTENT_VALUE);
                                //int i=0;
                                for(int i=0;i<insertKeyArray.length();i++){
                                    key = insertKeyArray.get(i).toString();
                                    value = insertValueArray.get(i).toString();
                                    ContentValues insertAllValues = new ContentValues();
                                    insertAllValues.put(CONTENT_KEY, key);
                                    insertAllValues.put(CONTENT_VALUE, value);
                                    insert(null, insertAllValues);
                                }
                                //String value = msg.getString(CONTENT_VALUE);
                                //Log.v("Server Task", "Insert msg passing to content provider's insert");
                                break;
                        }
                    }
                    socket.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                Log.e("Server Task", "Outmost catch Exception | " + String.valueOf(e));
            }
            return null;
        }
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

    public JSONObject createJSONMsgForJoin(String myPort) {
        JSONObject jsonSend = new JSONObject();
        try {
            jsonSend.put(MESSAGE_TYPE, MESSAGE_ENUM.JOIN);
            jsonSend.put(JOINING_PORT, myPort);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonSend;
    }

    private String createJsonMsgForQueryToSuccessor(String selection, String myPort) {
        JSONObject jsonSend = new JSONObject();
        try {
            jsonSend.put(MESSAGE_TYPE, MESSAGE_ENUM.QUERY);
            jsonSend.put(SELECTION, selection);
            jsonSend.put(QUERY_PORT, myPort);
        } catch (JSONException e) {
            e.printStackTrace();
            //Log.v("QUERY EXCEPTION", String.valueOf(e));
        }
        return jsonSend.toString();
    }

    private String createJsonMsgForQueryAllToSuccessor(String selection, String myPort) {
        JSONObject jsonSend = new JSONObject();
        try {
            jsonSend.put(MESSAGE_TYPE,MESSAGE_ENUM.QUERY_ALL);
            jsonSend.put(SELECTION,selection);
            jsonSend.put(QUERY_PORT,myPort);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonSend.toString();
    }

    private class ClientTask extends AsyncTask<String, Void, String> {
        @Override
        protected String doInBackground(String... msgs) {
            return sendMessage(msgs[0], msgs[1]);
            //return null;
        }
    }

    private String sendMessage(String msg, String destination) {
        try {

            //Log.v("Client Task","Sending msg: "+msg+" | Port:"+destination+" IntegerparseInt: "+ Integer.parseInt(destination));
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destination));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            out.write(msg);
            out.flush();
            out.close();
            String type = new JSONObject(msg).getString(MESSAGE_TYPE);
            if(MESSAGE_ENUM.valueOf(type) == MESSAGE_ENUM.QUERY){
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destination));
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String s = in.readLine();
                while(s==null) {
                    s=in.readLine();
                }

                //Log.v("QUERY BACK",s);
                socket.close();
                return s;
            } else if(MESSAGE_ENUM.valueOf(type) == MESSAGE_ENUM.QUERY_ALL){
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destination));
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String s = in.readLine();
                while(s==null) {
                    s=in.readLine();
                }
                //Log.v("QUERY BACK",s);
                socket.close();
                return s;
            } else {
                out.close();
                socket.close();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
            Log.d("SendMessage", "ClientTask UnknownHostException");
        } catch (IOException e) {
            e.printStackTrace();
            Log.d("SendMessage", "ClientTask socket IOException");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getPortHash(String joiningPort) {
        try {
            return genHash(Integer.toString(Integer.parseInt(joiningPort)/2));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            //Log.v("DHT Provider","No such algorithm exception for port");
            return null;
        }
    }

    private void findSuccPredNodes(String joinPortHash) {
        String prevNode = activeNodeMap.firstKey();
        String currNode = null;
        String succNode = null;

        //Log.v("Server Task","Printing the current node list");
        for(String key: activeNodeMap.keySet()){
            currNode = key;
            //Log.v("Nodes ", "entry "+": "+activeNodeMap.get(key));
            try{
                if(joinPortHash.compareTo(currNode) == 0){
                    if (currNode == activeNodeMap.lastKey()) {
                        succNode = activeNodeMap.firstKey();
                    } else if (currNode == activeNodeMap.firstKey()){
                        prevNode = activeNodeMap.lastKey();
                        succNode = activeNodeMap.higherKey(currNode);
                    } else {
                        succNode = activeNodeMap.higherKey(currNode);
                    }
                    break;
                }
            } catch (Exception e){
                e.printStackTrace();
                //Log.v("Error","occured at selecting pred and succ");
            }
            prevNode = currNode;
        }
        String msgToSend=null;
        //Tell the prevNode about its new successor
        msgToSend = createJSONMsgForUpdatePrevSuccNodes(null, activeNodeMap.get(prevNode),activeNodeMap.get(currNode), UPDATE_NODE_ENUM.NEW_SUCC);
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, activeNodeMap.get(prevNode));
        //Log.v("Server Task","Update Succ msg to Pred :"+msgToSend);
        //Tell the currNode about its prevNode and succNode
        msgToSend = createJSONMsgForUpdatePrevSuccNodes(activeNodeMap.get(prevNode),activeNodeMap.get(currNode),activeNodeMap.get(succNode), UPDATE_NODE_ENUM.NEW_CURR);
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, activeNodeMap.get(currNode));
        //Log.v("Server Task","Update Succ and Pred msg to Curr :"+msgToSend);
        //Tell the succNode about its prevNode
        msgToSend = createJSONMsgForUpdatePrevSuccNodes(activeNodeMap.get(currNode),activeNodeMap.get(succNode), null, UPDATE_NODE_ENUM.NEW_PREV);
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend, activeNodeMap.get(succNode));
        //Log.v("Server Task","Update Pred msg to Succ :"+msgToSend);
        //Log.v("Server Task","Created Client tasks to send new join node updates");
    }

    private String createJSONMsgForUpdatePrevSuccNodes(String s1, String s2, String s3, UPDATE_NODE_ENUM update) {
        JSONObject msg = new JSONObject();
        try{
            msg.put(MESSAGE_TYPE,MESSAGE_ENUM.UPDATE_NODE);
            msg.put(PREDECESSOR_PORT,s1);
            msg.put(CURRENT_PORT,s2);
            msg.put(SUCCESSOR_PORT,s3);
            msg.put(UPDATE_NODE, update);
        } catch (JSONException e){
            e.printStackTrace();
            Log.e("createJSONMsg","ForUpdatePrevSuccNodes");
        }

        return msg.toString();
    }

    private String createJsonMsgForInsertToSuccessor(String key, String value) {
        JSONObject jsonMsg = new JSONObject();
        try{
            jsonMsg.put(MESSAGE_TYPE, MESSAGE_ENUM.INSERT);
            jsonMsg.put(CONTENT_KEY,key);
            jsonMsg.put(CONTENT_VALUE, value);
        }catch (JSONException e) {
            e.printStackTrace();
            //Log.v("Insert","Exception occurred while creating json msg for insert");
        }
        return jsonMsg.toString();
    }

    private String createJsonMsgForDeleteToSuccessor(String selection, String myPort) {
        JSONObject jsonSend = new JSONObject();
        try {
            jsonSend.put(MESSAGE_TYPE,MESSAGE_ENUM.DELETE);
            jsonSend.put(SELECTION,selection);
            jsonSend.put(DELETE_PORT,myPort);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonSend.toString();
    }

    private String createJsonMsgForDeleteAllToSuccessor(String selection, String myPort) {
        JSONObject jsonSend = new JSONObject();
        try {
            jsonSend.put(MESSAGE_TYPE,MESSAGE_ENUM.DELETE_ALL);
            jsonSend.put(SELECTION,selection);
            jsonSend.put(DELETE_PORT,myPort);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonSend.toString();
    }

}
