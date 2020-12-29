package pku;

class IndexItem {
    public long offset;
    public int startMsgIndex;
    public int msgNum;

    public IndexItem(long offset, int startMsgIndex, int msgNum) {
        this.offset = offset;
        this.startMsgIndex = startMsgIndex;
        this.msgNum = msgNum;
    }

}
