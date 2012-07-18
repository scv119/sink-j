package com.zhihu.sink;

import com.zhihu.sink.util.SafeEncoder;
import com.zhihu.sink.util.SinkInputStream;
import com.zhihu.sink.util.SinkOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: shenchen
 * Date: 7/13/12
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class Protocol {
    public static final int DEFAULT_PORT = 3399;
    public static final int DEFAULT_TIMEOUT = 2000;
    public static final String CHARSET = "UTF-8";

    public void sendCommand(final SinkOutputStream os, final Command command, final byte[]...args){
        sendCommand(os, command.raw, args);
    }

    private void sendCommand(final SinkOutputStream os, final byte[] command, final byte[]...args){
        try{
            os.writeIntCrLf(command.length);
            os.write(command);
            os.writeCrLf();
            for(final byte[] arg : args){
                os.writeIntCrLf(arg.length);
                os.write(arg);
                os.writeCrLf();
            }
            os.writeCrLf();
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    public List<byte[]> readResponse(final SinkInputStream is){
        List<byte[]> list = new ArrayList<byte[]>();
        byte[] line;
        while((line = processLine(is))!=null){
            list.add(line);
        }
        return list;
    }

    private byte[] processLine(final SinkInputStream is){
        String line = is.readLine();
        if(line == null)
            return null;
        int len = Integer.parseInt(line);
        if (len == -1) {
            return null;
        }
        byte[] read = new byte[len];
        int offset = 0;
        try {
            while (offset < len) {
                offset += is.read(read, offset, (len - offset));
            }
            // read 2 more bytes for the command delimiter
            is.readByte();
            is.readByte();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return read;
    }



    public static enum Command{
        QUIT, SUBSCRIBE, MKSESSION, LDSESSION, NEXT,PUBLISH;

        public final byte[] raw;

        Command(){
            raw = SafeEncoder.encode(this.name());
        }
    }
}
