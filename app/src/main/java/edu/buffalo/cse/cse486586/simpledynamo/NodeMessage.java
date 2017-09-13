package edu.buffalo.cse.cse486586.simpledynamo;

import static android.R.attr.type;

/**
 * Created by anushree on 5/2/17.
 */


public class NodeMessage implements Comparable<NodeMessage>{

    String portID;
    String hashID;
    String predecessor;
    String successor1;
    String successor2;
    String msgType;
    String key;
    String value;
    String originator;
    String time;

    public NodeMessage(){
        portID = null ;
        hashID = null;
        predecessor = null;
        successor1 = null;
        successor2 = null;
        msgType = "DUMMY";
        key = "";
        value = "";
        originator = null;
        time = null;
    }

    public NodeMessage(String ID, String hashID, String predecessor, String successor1, String successor2, String type){
        this.portID = ID;
        this.msgType = type;
        this.hashID = hashID;
        this.predecessor = predecessor;
        this.successor1 = successor1;
        this.successor2 = successor2;
        this.key = "";
        this.value = "";
        this.originator = null;
        this.time = null;
    }

    public String formMessage(){

        String str = portID+"~#~"+hashID+"~#~"+predecessor+"~#~"+successor1+"~#~"+successor2+"~#~"+msgType+"~#~"+key+"~#~"+value+"~#~"+originator+"~#~"+time;

        return str;
    }

    public void breakMessage(String str){

        String[] msg = str.split("~#~");

        this.portID = msg[0];
        this.hashID = msg[1];
        this.predecessor = msg[2];
        this.successor1 = msg[3];
        this.successor2 = msg[4];
        this.msgType = msg[5];
        this.key =  msg[6];
        this.value = msg[7];
        this.originator = msg[8];
        this.time = msg[9];

    }

    @Override
    public int compareTo(NodeMessage node) {
        String hash = node.hashID;
        return this.hashID.compareTo(hash);
    }
}
