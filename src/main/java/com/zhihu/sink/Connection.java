package com.zhihu.sink;

import com.zhihu.sink.util.SafeEncoder;
import com.zhihu.sink.util.SinkInputStream;
import com.zhihu.sink.util.SinkOutputStream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: shenchen
 * Date: 7/13/12
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class Connection {
    private String host;
    private int port = Protocol.DEFAULT_PORT;
    private Socket socket;
    private Protocol protocol = new Protocol();
    private SinkOutputStream outputStream;
    private SinkInputStream inputStream;
    private int timeout = Protocol.DEFAULT_TIMEOUT;

    public Socket getSocket() {
        return socket;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(final int timeout) {
        this.timeout = timeout;
    }

    public void setTimeoutInfinite() {
        try {
            socket.setSoTimeout(0);
        } catch (SocketException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void rollbackTimeout() {
        try {
            socket.setSoTimeout(timeout);
        } catch (SocketException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Connection(final String host) {
        super();
        this.host = host;
    }

    protected void flush() {
        try {
            outputStream.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected Connection sendCommand(final Protocol.Command cmd, final String... args) {
        final byte[][] bargs = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            bargs[i] = SafeEncoder.encode(args[i]);
        }
        return sendCommand(cmd, bargs);
    }

    protected Connection sendCommand(final Protocol.Command cmd, final byte[]... args) {
        connect();
        protocol.sendCommand(outputStream, cmd, args);
        return this;
    }

    protected Connection sendCommand(final Protocol.Command cmd) {
        connect();
        protocol.sendCommand(outputStream, cmd, new byte[0][]);
        return this;
    }

    public Connection(final String host, final int port) {
        super();
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    public Connection() {
    }

    public void connect() {
        if (!isConnected()) {
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), timeout);
                socket.setSoTimeout(timeout);
                outputStream = new SinkOutputStream(socket.getOutputStream());
                inputStream = new SinkInputStream(socket.getInputStream());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void disconnect() {
        if (isConnected()) {
            try {
                inputStream.close();
                outputStream.close();
                if (!socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public boolean isConnected() {
        return socket != null && socket.isBound() && !socket.isClosed()
                && socket.isConnected() && !socket.isInputShutdown()
                && !socket.isOutputShutdown();
    }



    public List<byte[]> getBinaryReply() {
        flush();
        return protocol.readResponse(inputStream);
    }

    public List<String> getStringReply() {
        List<byte[]> reply = getBinaryReply();
        List<String> ret = new ArrayList<String>(reply.size());
        for(byte[] arr:reply){
            if(arr == null)
                ret.add("");
            else
                ret.add(SafeEncoder.encode(arr));
        }
        return ret;
    }

}
