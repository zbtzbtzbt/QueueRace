package pku;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

//buffer本质上是一块内存区,可以用来写入数据,稍后取出来
//buffer flip 从[写模式]切换到[读模式]
public class DiskManager {

    public static int FILE_NUM = 8;
    //写入磁盘的八个文件
    private static RandomAccessFile randomAccessFile[];
    //长度为8的randomAccessFile 数组
    static {
        try {
            randomAccessFile = new RandomAccessFile[FILE_NUM];
            for (int i = 0; i < FILE_NUM; i++) {
                randomAccessFile[i] = new RandomAccessFile("data/" + ("data" + i), "rw");
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    //就是WriteToDisk
    //position是写入文件那个位置
    //buffer数据写入磁盘
    public static void writeToFile(ByteBuffer buffer, long position,String queueName) {
        //queueName.hashCode() & (FILE_NUM-1) 存在的问题:分布【不均匀】
        //改进:queueName_i i%8 需要截取后面的字符串转数字 会很慢
        synchronized (randomAccessFile[queueName.hashCode() & (FILE_NUM-1)]) {
            FileChannel channel = randomAccessFile[queueName.hashCode() & (FILE_NUM-1)].getChannel();
            try {
                channel.write(buffer, position);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //从file磁盘读到buffer
    public static void readAbBuf(ByteBuffer buffer, long position,String queueName) {
//        if (!readFileChannels.keySet().contains(Thread.currentThread())) {
//            readFileChannels.put(Thread.currentThread(), randomAccessFile.getChannel());
//        }

        FileChannel channel = randomAccessFile[queueName.hashCode() & (FILE_NUM-1)].getChannel();
//                readFileChannels.get(Thread.currentThread());
        try {
            channel.read(buffer, position);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
