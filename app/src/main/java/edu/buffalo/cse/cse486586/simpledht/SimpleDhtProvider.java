package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    private String successor;
    private String predecessor;
    private String myPort;
    private String myNodeId;
    private ArrayList<String> nodeList = new ArrayList<>();
    private HashMap<String,String> queryMap = new HashMap<>();
    private MatrixCursor queryAllCursor;
    ReentrantLock lock = new ReentrantLock();
    Condition condition = lock.newCondition();
    Condition qCondition = lock.newCondition();

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        try {
            switch (selection) {
                case "\"*\"":
                    deleteAll();
                    break;
                case "\"@\"":
                    deleteLocal();
                    break;
                default:
                    boolean isMine = checkIfInMyPartition(selection);
                    if (isMine){
                        getContext().deleteFile(selection);
                    }else{
                        String msgString = "D-"+selection;
                        Message deleteMsg = new Message(myPort+";"+ Message.MessageType.MSG+";"+msgString);
                        sendMessageSynch(deleteMsg.stringify(), Integer.parseInt(successor));
                    }
                    break;
            }

            Log.v("delete",selection);
        } catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
    public void deleteLocal(){
        File parent = getContext().getFilesDir();
        File []files = parent.listFiles();
        for (File file:files){
            String key = file.getName();
            getContext().deleteFile(key);
            Log.d("Filename", key);
        }
    }
    public void deleteAll() throws IOException {
        deleteLocal();
        if (successor==null){
            return;
        }
        String msgString = "D-*-"+myPort;
        Message queryAllMsg = new Message(myPort+";"+ Message.MessageType.MSG.name()+";"+msgString);
        sendMessageSynch(queryAllMsg.stringify(),Integer.parseInt(successor));
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString("key");
        try {
            Boolean isMine = checkIfInMyPartition(key);
            if (isMine || successor == null){
                FileOutputStream outputStream = getContext().openFileOutput( key, Context.MODE_PRIVATE);
                outputStream.write(values.getAsString("value").getBytes());
                outputStream.close();
                Log.v("insert", values.toString());
            }else{
                String msgStr = "I-"+key+"@"+values.getAsString("value");
                Message insertMsgObj = new Message(myPort+";"+
                        Message.MessageType.MSG.name()+";"+msgStr);
                sendMessageSynch(insertMsgObj.stringify(),Integer.parseInt(successor));
                Log.d("insert", "Sent to " + successor + ": " + values.toString());

            }
        } catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        try {
            String [] columns =  new String[2];
            columns[0] = "key";
            columns[1] = "value";
            MatrixCursor cursor = new MatrixCursor(columns);
            switch (selection) {
                case "\"*\"":
                    queryAll(cursor);
                    break;
                case "\"@\"":
                    queryLocal(cursor);
                    break;
                default:
                    querySingleValue(selection, cursor);
                    break;
            }

            Log.v("query", selection);
            return cursor;
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }


    }

    public void querySingleValue(String selection, MatrixCursor cursor)
            throws NoSuchAlgorithmException, IOException, InterruptedException {
        String value;
        boolean isMine = checkIfInMyPartition(selection);
        if (isMine) {
            value = queryFile(selection);
        } else {
            value = queryFromOther(selection);
        }
        String columnsArray[] = {selection,value};
        cursor.addRow(columnsArray);
    }
    public String queryFile(String key) throws IOException {
        FileInputStream inputStream;
        inputStream = getContext().openFileInput(key);
        byte[] buffer = new byte[inputStream.available()];
        int length = inputStream.read(buffer);
        inputStream.close();
        return new String(buffer, 0, length, StandardCharsets.UTF_8);
    }
    public void queryLocal(MatrixCursor cursor) throws IOException {

        File parent = getContext().getFilesDir();
        File []files = parent.listFiles();
        for (File file:files){
            String key = file.getName();
            Log.d("Filename",key);
            String value = queryFile(key);
            String [] columns = {key,value};
            cursor.addRow(columns);
        }
    }
    public String queryFromOther(String key) throws InterruptedException, IOException {
        String msgStr = "Q-"+key+"@"+myPort;
        Message msgObj = new Message(myPort+";"+ Message.MessageType.MSG.name()+";"+msgStr);
        sendMessageSynch(msgObj.stringify(),Integer.parseInt(successor));
        lock.lock();
        while(queryMap.get(key)==null){
            Log.d("Conc","Waiting");
            condition.await();
        }
        lock.unlock();
        return queryMap.get(key);
    }
    public void queryAll(MatrixCursor cursor) throws IOException, InterruptedException {
        queryLocal(cursor);
        if (successor==null){
            return;
        }
        queryAllCursor = cursor;
        Message queryAllMsg = new Message(myPort+";"+ Message.MessageType.QALL.name()+";"+myPort);
        sendMessageSynch(queryAllMsg.stringify(),Integer.parseInt(successor));
        lock.lock();
        qCondition.await();
        lock.unlock();
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().
                getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        int portInt = Integer.parseInt(portStr);
        myPort = String.valueOf(portInt * 2);
        int emId = Integer.parseInt(myPort)/2;
        nodeList.add(Integer.toString(emId));

        try {
            ServerSocket socket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,socket);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            myNodeId = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (!(myPort.equalsIgnoreCase("11108"))){
            Message joinObj = new Message(myPort+";"+ Message.MessageType.JOIN.name()+";"+myNodeId);
            new ClientSendTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,joinObj.stringify(),
                    "11108");
        }

        return false;
    }

    private class ServerTask extends AsyncTask<ServerSocket,String,Void>{

        @SuppressWarnings("InfiniteLoopStatement")
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while(true){
                try {
                    Socket clientSocket = serverSocket.accept();
                    InputStreamReader isw = new InputStreamReader(clientSocket.getInputStream());
                    BufferedReader bw = new BufferedReader(isw);
                    String msgStr = bw.readLine();
                    Message rcvObj = new Message(msgStr);
                    Log.d("received",rcvObj.stringify());
                    switch (rcvObj.getType()){
                        case JOIN:
                            addToNodeList(rcvObj.getSentBy());
                            break;
                        case MSG:
                            dispatchMessage(rcvObj);
                            break;
                        case SIB:
                            assignSiblings(rcvObj);
                            break;
                        case REPLY:
                            signalQueryReply(rcvObj);
                            break;
                        case QALL:
                            handleGlobalQuery(rcvObj);
                            break;
                        case QALLREPLY:
                            addToQAllCursor(rcvObj);
                            break;
                        default:
                            throw new NoSuchElementException();
                    }

                } catch (IOException e) {
                    Log.d("IOEXC", e.getMessage());
                    e.printStackTrace();
                } catch (NoSuchElementException e) {
                    Log.d("NOELEM", e.getMessage());
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

        }

        public void addToQAllCursor(Message rcvObj) {
            String message = rcvObj.getMessage();
            if (message != null){
                String[] keyValPairs = rcvObj.getMessage().split(",");
                for (String keyVal:keyValPairs){
                    if (keyVal.contains("-")){
                        String[] keyVals = keyVal.split("-");
                        Log.d("QALLADD", Arrays.toString(keyVals));
                        queryAllCursor.addRow(keyVals);
                    }
                }
            }

        }

        public void handleGlobalQuery(Message rcvObj) throws IOException {
            if (rcvObj.getMessage().equals(myPort)){
                lock.lock();
                qCondition.signalAll();
                lock.unlock();
            }else{
                String [] columns =  new String[2];
                columns[0] = "key";
                columns[1] = "value";
                String qRplyString = "";
                MatrixCursor cursor = new MatrixCursor(columns);
                queryLocal(cursor);
                while(cursor.moveToNext()){
                    String key = cursor.getString(0);
                    String value = cursor.getString(1);
                    qRplyString += key+"-"+value+",";
                }
                cursor.close();
                Log.d("QALL",qRplyString+"End");
                if (qRplyString.length() > 1){
                    qRplyString = qRplyString.substring(0,qRplyString.length()-1);
                }
                Message qAllReplyObj = new Message(myPort+";"+
                        Message.MessageType.QALLREPLY.name()+";"+qRplyString);
                sendMessageSynch(qAllReplyObj.stringify(),
                        Integer.parseInt(rcvObj.getMessage()));
                sendMessageSynch(rcvObj.stringify(), Integer.parseInt(successor));
            }
        }

        public void signalQueryReply(Message rcvObj) {
            String[] keyVal = rcvObj.getMessage().split("@");
            lock.lock();
            queryMap.put(keyVal[0],keyVal[1]);
            condition.signalAll();
            lock.unlock();
        }

        public void assignSiblings(Message rcvObj) {
            String[] siblingStr = rcvObj.getMessage().split("-");
            if (siblingStr[0].equals("S")){
                successor = siblingStr[1];
                publishProgress("Pre: "+successor);
            }else if(siblingStr[0].equals("P")){
                predecessor = siblingStr[1];
                publishProgress("Suc: "+predecessor);
            }
        }

        @Override
        protected void onProgressUpdate(String... values) {
            SimpleDhtActivity activity = SimpleDhtActivity.activity;
            if (activity != null){
                TextView tv = (TextView) activity.findViewById(R.id.textView1);
                tv.append(values[0].trim()+"\n");
            }
        }
    }

    private class ClientSendTask extends AsyncTask<String,Void,Void>{

        @Override
        protected Void doInBackground(String... params) {
            String msgString = params[0];
            int port = Integer.parseInt(params[1]);
            try {
                sendMessageSynch(msgString, port);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    private class HashComparator implements Comparator<String> {

        @Override
        public int compare(String lhs, String rhs) {
            try {
                return genHash(lhs).compareTo(genHash(rhs));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
                return 0;
            }
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

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public void addToNodeList(Integer port) {
        String emStr = Integer.toString(port/2);
        nodeList.add(emStr);
        Collections.sort(nodeList,new HashComparator());

        int index = nodeList.indexOf(emStr);

        int preIndex = index-1;
        int sucIndex = index+1;

        if (preIndex < 0){
            preIndex = nodeList.size()-1;
        }
        if (sucIndex>nodeList.size()-1){
            sucIndex = 0;
        }

        Log.d("Join",nodeList.toString());

        String preEmId = nodeList.get(preIndex);
        String prePort = Integer.toString(Integer.parseInt(preEmId)*2);

        String sucEmId = nodeList.get(sucIndex);
        String sucPort = Integer.toString(Integer.parseInt(sucEmId)*2);


        String msgToPre = "S-"+Integer.toString(port);
        Message msgObjToPre = new Message(myPort+";"+Message.MessageType.SIB.name()+";"+msgToPre);
        new ClientSendTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgObjToPre.stringify(),prePort);

        String msgToSuc = "P-"+Integer.toString(port);
        Message msgObjToSuc = new Message(myPort+";"+Message.MessageType.SIB.name()+";"+msgToSuc);
        new ClientSendTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,msgObjToSuc.stringify(),sucPort);

        String preMsgToNode = "P-"+prePort;
        Message preMsgObjToNode = new Message(myPort+";"+Message.MessageType.SIB.name()
                +";"+preMsgToNode);
        new ClientSendTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,preMsgObjToNode.stringify()
                ,Integer.toString(port));


        String sucMsgToNode = "S-"+sucPort;
        Message sucMsgObjToNode = new Message(myPort+";"+Message.MessageType.SIB.name()
                +";"+sucMsgToNode);
        new ClientSendTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,sucMsgObjToNode.stringify()
                ,Integer.toString(port));

    }

    public void dispatchMessage(Message msgObj) throws NoSuchAlgorithmException, IOException {
        String[] opMessage = msgObj.getMessage().split("-");
        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        switch (opMessage[0]) {
            case "I": {
                String[] keyVal = opMessage[1].split("@");
                Boolean isMine = checkIfInMyPartition(keyVal[0]);
                if (isMine){
                    ContentValues cv = new ContentValues();
                    cv.put("key", keyVal[0]);
                    cv.put("value", keyVal[1]);
                    getContext().getContentResolver().insert(uri, cv);
                }else{
                    sendMessageSynch(msgObj.stringify(), Integer.parseInt(successor));
                }
                break;
            }
            case "Q": {
                String[] keyAndPort = opMessage[1].split("@");
                String key = keyAndPort[0];
                try {
                    Boolean isMine = checkIfInMyPartition(key);
                    if (isMine){
                        String value = queryFile(key);
                        Message replyObj = new Message(myPort+";"+ Message.MessageType.REPLY.name()
                                +";"+key+"@"+value);
                        new ClientSendTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                replyObj.stringify(),keyAndPort[1]);
                    }else{
                        new ClientSendTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                                msgObj.stringify(),successor);
                    }
                } catch (NoSuchAlgorithmException | IOException e) {
                    e.printStackTrace();
                }
                break;
            }
            case "D":
                switch (opMessage[1]) {
                    case "\"*\"":
                        if (opMessage[2].equals(myPort)){
                            break;
                        }
                        deleteLocal();
                        sendMessageSynch(msgObj.stringify(), Integer.parseInt(successor));
                        break;
                    default:
                        Boolean isMine = checkIfInMyPartition(opMessage[1]);
                        if (isMine){
                            getContext().getContentResolver().delete(uri, opMessage[1],null);
                        }else{
                            sendMessageSynch(msgObj.stringify(), Integer.parseInt(successor));
                        }
                        break;
                }

                break;
        }

    }

    public void sendMessageSynch(String msgString, int port) throws IOException {
        Log.d("Send", "Sending :" + msgString);
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),port);
        OutputStreamWriter osw = new OutputStreamWriter(socket.getOutputStream());
        BufferedWriter bw = new BufferedWriter(osw);
        bw.write(msgString);
        bw.newLine();
        bw.flush();
        socket.close();
    }

    public Boolean checkIfInMyPartition(String key) throws NoSuchAlgorithmException {
        if (predecessor == null){
            return true;
        }
        String keyHash = genHash(key);
        //handle corner case at head
        int preEmId = Integer.parseInt(predecessor)/2;

        String preHash = genHash(Integer.toString(preEmId));
        if (preHash.compareTo(myNodeId) > 0){
            if (keyHash.compareTo(preHash)>0){
                return true;
            }else if(keyHash.compareTo(myNodeId) <=0 ){
                return true;
            }
        }else {
            if (keyHash.compareTo(preHash) > 0 && keyHash.compareTo(myNodeId) <= 0) {
                return true;
            }
        }
        return false;

    }

}
