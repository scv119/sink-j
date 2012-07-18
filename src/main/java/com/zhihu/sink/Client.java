package com.zhihu.sink;

import java.util.List;

import static com.zhihu.sink.Protocol.Command.*;

/**
 * Created with IntelliJ IDEA.
 * User: shenchen
 * Date: 7/13/12
 * Time: 3:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class Client extends Connection{
    private String     session;

    public Client(final String host){
        super(host);
    }

    public Client(final String host, int port){
        super(host, port);
    }

    public void connect(){
        super.connect();
        if(session == null)
            mksession();
        else
            ldsession();
    }

    public void publish(String topic, String message){
        super.connect();
        sendCommand(PUBLISH,topic,message);
        List<String> reply = getStringReply();
        if(reply.get(0).equals("ERR"))
            throw new RuntimeException("ERR");;
    }

    private void mksession(){
        sendCommand(MKSESSION);
        List<String> reply = getStringReply();
        if(reply.get(0).equals("ERR"))
            throw new RuntimeException("ERR");
        session = reply.get(1);
    }

    private void ldsession(){
        sendCommand(LDSESSION,session);
        List<String> reply = getStringReply();
        if(reply.get(0).equals("ERR"))
            throw new RuntimeException("ERR");
    }
    public PubSub subscribe(String topic){
        connect();
        sendCommand(SUBSCRIBE,topic);
        List<String> reply = getStringReply();
        if(reply.get(0).equals("ERR") || !reply.get(1).equals(topic))
            throw new RuntimeException("ERR");
        return new CPubSub(this, topic);
    }

    static class CPubSub implements PubSub{
        private final Client client;
        private final String topic;

        CPubSub(Client client, String topic) {
            this.client = client;
            this.topic = topic;
        }

        @Override
        public String next() {
            if(!client.isConnected()){
                client.connect();
                client.sendCommand(SUBSCRIBE,topic);
                List<String> reply = client.getStringReply();
                if(reply.get(0).equals("ERR") || !reply.get(1).equals(topic))
                    throw new RuntimeException("ERR");
            }

            client.sendCommand(NEXT);
            client.setTimeoutInfinite();
            try{
                List<String> reply = client.getStringReply();
                if(reply.get(0).equals("ERR") || !reply.get(2).equals(topic) )
                    throw new RuntimeException("ERR");
                return reply.get(3);
            }
            finally {
                client.rollbackTimeout();
            }
        }

    }

}
