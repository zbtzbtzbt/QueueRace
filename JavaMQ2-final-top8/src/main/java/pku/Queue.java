package pku;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Queue {

    public static final int BUF_SIZE = 2 << 10;
    private volatile boolean isFirstGet = true;
    private ByteBuffer buffer = ByteBuffer.allocateDirect(BUF_SIZE);
    private int curMsgNum = 0;  // 当前该buffer写入磁盘的数量
    private int bufMsgNum = 0;   // 当前该buffer中缓存的数量
    private Index index = new Index();

    private FileChannel channel;
    private AtomicLong wrotePosition;

    public Queue(FileChannel channel, AtomicLong wrotePosition){
        this.channel = channel;
        this.wrotePosition = wrotePosition;
    }
    public void put(byte[] message) {
        if (message.length + 2 > buffer.remaining()) {
            writeToDisk();
            buffer.clear();
        }
        buffer.putShort((short) message.length);
        buffer.put(message);
        curMsgNum++;
        bufMsgNum++;
    }

    private void writeToDisk() {
        buffer.position(0);
        long curBufIndex = wrotePosition.getAndIncrement();
        index.add(curBufIndex * BUF_SIZE , curMsgNum-bufMsgNum, bufMsgNum);
        try {
            channel.write(buffer,curBufIndex * BUF_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        buffer.clear();
        bufMsgNum = 0;
    }

    public Collection<byte[]> get(long offset, long num) {
        //将当前队列缓存写道磁盘
        if (isFirstGet) {
            writeToDisk();
            isFirstGet = false;
        }
        List<byte[]> result = new ArrayList<>();
        ArrayList<IndexItem> bufIndexs = index.getBufIndexs(offset, num);
        if (bufIndexs == null || bufIndexs.size() == 0) return DefaultQueueStoreImpl.EMPTY;
        int leftNum = (int)num;
        boolean isFirst = true;
        try {
            ByteBuffer buffer = ByteBuffer.allocate(BUF_SIZE);
            for (IndexItem bufIndex : bufIndexs) {
                buffer.clear();
                channel.read(buffer,bufIndex.offset);
                buffer.position(0);
                int startIndex = 0;
                if (isFirst) {
                    startIndex = (int) offset - bufIndex.startMsgIndex;
                    isFirst = false;
                }
                int canReadNum = bufIndex.msgNum - startIndex;
                int realNum = leftNum < canReadNum ? leftNum : canReadNum;
                for (int i = 0; i < startIndex; i++) {
                    short msgLen = buffer.getShort();
                    int oldPos = buffer.position();
                    buffer.position(oldPos + msgLen);
                }
                for (int i = 0; i < realNum; i++) {
                    short msgLen = buffer.getShort();
                    byte[] message = new byte[msgLen];
                    buffer.get(message);
                    result.add(message);
                }
                leftNum -= realNum;
            }
            buffer = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
