package pku;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
//100w queue = 100w buffer = 100w bufferManager对象
public class BufferManager {
    //8个bufIndex
    //第i个文件写到的位置[下次应该从那个位置开始写]
    //bufIndex[2]=8K 表示文件3下次应该从8K位置开始写
    public static AtomicLong bufIndex[] = new AtomicLong[DiskManager.FILE_NUM];
    private static final int BUF_SIZE = 8 << 10;//buffer大小是8K
    private volatile boolean isFirstGet = true;
    private ByteBuffer buffer = ByteBuffer.allocateDirect(BUF_SIZE);
    private int currentMsgNum = 0;  // 当前该buffer写入磁盘的数量
    private int bufferMsgNum = 0;   // 当前该buffer中缓存的数量
    private Index index = new Index();
    private String queueName; // 为了获得写入的文件

    public BufferManager (String queueName){
        for(int i = 0;i< DiskManager.FILE_NUM; i++){
            bufIndex[i] = new AtomicLong(0);
        }
       this.queueName = queueName;
    }
    public void put(byte[] message) {
        //缓存区加不下这个message了,需要写入磁盘
        //消息本身的长度(short)是2字节
        if (message.length + 2 > buffer.remaining()) {
            writeToDisk();
            buffer.clear();
        }
        buffer.putShort((short) message.length);
        buffer.put(message);
        currentMsgNum++;
        bufferMsgNum++;
    }

    private void writeToDisk() {
        buffer.position(0);
        //返回的是原值
        long curBufIndex = bufIndex[(queueName.hashCode() & (DiskManager.FILE_NUM-1))].getAndIncrement();
        index.add(curBufIndex * BUF_SIZE, currentMsgNum-bufferMsgNum, bufferMsgNum);
        DiskManager.writeToFile(buffer, curBufIndex * BUF_SIZE, queueName);
        buffer.clear();
        bufferMsgNum = 0;//当前buffer缓冲区msg数目
    }

    //读之前先把遗留的放入磁盘
    //得到对应文件的 从offset位置开始的num条msg
    //offset 【第几条数据】
    //每次读和写都是BUF_SIZE 字节
    public synchronized Collection<byte[]> get(long offset, long num) {
        //将当前队列缓存写到磁盘
        if (isFirstGet) {
            writeToDisk();
            isFirstGet = false;
        }

        List<byte[]> result = new ArrayList<>();
        ArrayList<IndexItem> bufIndexs = index.getBufIndexs(offset, num);
        if (bufIndexs == null || bufIndexs.size() == 0) return DefaultQueueStoreImpl.EMPTY;
        int leftNum = (int)num;
        boolean isFirst = true;
        for (IndexItem bufIndex : bufIndexs) {
            buffer.clear();
            //数据从磁盘file读到buffer
            DiskManager.readAbBuf(buffer, bufIndex.offset,queueName);
            buffer.position(0);
            int startIndex = 0;
            if (isFirst) {
                //比如想读第68条msg,读出来的是buffer[50,72],那么startIndex就是68-50=18
                //表示buffer里面的第18条
                startIndex = (int) offset - bufIndex.startMsgIndex;
                isFirst = false;
            }
            int canReadNum = bufIndex.msgNum - startIndex;
            int realNum = leftNum < canReadNum ? leftNum : canReadNum;
            skipNMsgInBuf(startIndex);
            for (int i = 0; i < realNum; i++) {
                byte[] nextMsgInBuf = getNextMsgInBuf();
                result.add(nextMsgInBuf);
            }
            leftNum -= realNum;
        }
        return result;
    }

    public void skipNMsgInBuf(int n) {
        for (int i = 0; i < n; i++) {
            short msgLen = buffer.getShort();
            int oldPos = buffer.position();
            buffer.position(oldPos + msgLen);
        }
    }

    public byte[] getNextMsgInBuf() {
        short msgLen = buffer.getShort();
        byte[] message = new byte[msgLen];
        buffer.get(message);
        return message;
    }
}
