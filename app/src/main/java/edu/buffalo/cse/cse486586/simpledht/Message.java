package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by rakesh on 3/31/15.
 */
public class Message {
    private MessageType type;
    private Integer sentBy;
    private String message;

    @Override
    public String toString() {
        return sentBy+": "+message;
    }

    public String stringify(){
        return this.sentBy+";"+this.type.name()+";"+this.message;
    }

    public Message(String stringified) {
        String[] members = stringified.split(";");
        this.sentBy = Integer.parseInt(members[0]);
        this.type = MessageType.valueOf(members[1]);
        if (members.length >2) this.message = members[2];

    }

    public Message(MessageType type, Integer sentBy, String message) {
        this.type = type;
        this.sentBy = sentBy;
        this.message = message;
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public Integer getSentBy() {
        return sentBy;
    }

    public void setSentBy(Integer sentBy) {
        this.sentBy = sentBy;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public  enum MessageType {
        MSG, JOIN, SIB, REPLY, QALL, QALLREPLY
    }


}
