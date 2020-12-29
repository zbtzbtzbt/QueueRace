package pku;

//索引项【存储信息和buffer相关】
//buffer数据写入磁盘的记录信息
//写入filename个文件(8)
//buffer对应一个i=IndexItem链表
class IndexItem {
    public long offset;//buffer从【文件中】下次哪个位置开始 BUF_SIZE(8K)的倍数
    public int startMsgIndex;//下次msg开始的下标，也可以理解为已经写入消息的数目（比如第一个buffer已经写入60条msg）下次从61开始
    public int msgNum;//当前有多少条消息

    public IndexItem(long offset, int startMsgIndex, int msgNum) {
        this.offset = offset;
        this.startMsgIndex = startMsgIndex;
        this.msgNum = msgNum;
    }
}
