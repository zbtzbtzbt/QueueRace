package pku;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultQueueStoreImpl extends QueueStore {

    public static Collection<byte[]> EMPTY = new ArrayList<>();
    private ConcurrentHashMap<String, Queue> queues;
    private static final int FILE_SIZE = 128;
    public static final String dir = "data/";
    private FileChannel[] channels;
    private AtomicLong[] wrotePositions;
    public DefaultQueueStoreImpl() {
        queues = new ConcurrentHashMap<>();
        channels = new FileChannel[FILE_SIZE];
        wrotePositions = new AtomicLong[FILE_SIZE];//写入磁盘的位置
        try {
            RandomAccessFile raf = null;
            for (int i = 0; i < FILE_SIZE; i++) {
                raf = new RandomAccessFile(dir + "data" + i , "rw");
                this.channels[i] = raf.getChannel();
                this.wrotePositions[i] = new AtomicLong(0L);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * 把一条消息写入一个队列；
     * 这个接口需要是线程安全的，也即评测程序会并发调用该接口进行put；
     * 每个queue中的内容，按发送顺序存储消息（可以理解为Java中的List），同时每个消息会有一个索引，索引从0开始；
     * 不同queue中的内容，相互独立，互不影响；
     *
     * @param queueName 代表queue名字，如果是第一次put，则自动生产一个queue
     * @param message   message，代表消息的内容，评测时内容会随机产生，大部分长度在64字节左右，会有少量消息在1k左右
     */

    /*
    * 【put函数】放入key值是队列名queueName，
    * value是消息打算存入哪个缓冲区【具体为哪个channel，打算写入磁盘的哪个文件、以及文件中的哪个位置(offset)】
    * */
    @Override
    public void put(String queueName, byte[] message) {
        if (!queues.containsKey(queueName)) {
            int fileNo = Integer.valueOf(queueName.split("-")[1]) & (FILE_SIZE-1);
            queues.put(queueName, new Queue(channels[fileNo],wrotePositions[fileNo]));
        }
        Queue buffer = queues.get(queueName);
        buffer.put(message);
    }

    /**
     * 从一个队列中读出一批消息，读出的消息要按照发送顺序来；
     * 这个接口需要是线程安全的，也即评测程序会并发调用该接口进行get；
     * 返回的Collection会被并发读，但不涉及写，因此只需要是线程读安全就可以了；
     *
     * @param queueName 代表队列的名字
     * @param offset    代表消息的在这个队列中的起始消息索引
     * @param num       代表读取的消息的条数，如果消息足够，则返回num条，否则只返回已有的消息即可;没有消息了，则返回一个空的集合
     */
    @Override
    public Collection<byte[]> get(String queueName, long offset, long num) {
        Queue queue = queues.get(queueName);
        if (queue == null) {
            return EMPTY;
        }
        return queue.get(offset, num);
    }

}
