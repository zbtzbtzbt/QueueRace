package pku;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

class Index {
    //索引项 链表
    private List<IndexItem> index = new LinkedList<>();

    public synchronized void add(long offset, int startMsg, int msgNum) {
        index.add(new IndexItem(offset, startMsg, msgNum));
    }

    //从offset位置开始 得到num条msg消息
    public ArrayList<IndexItem> getBufIndexs(long offset, long num) {
        ArrayList<IndexItem> results = new ArrayList<>();
        boolean hasFind = false;  //找到offset所在index，如果找到之后，num数字大于0，那么则取下一个item的值
        for (int i = 0; i < index.size(); i++) {
            for (IndexItem indexItem : index) {
                //比如 写入file 61~100消息,offset=98 num=100
                if (offset >= indexItem.startMsgIndex
                        && offset < indexItem.startMsgIndex + indexItem.msgNum) {
                    results.add(indexItem);
                    num -= (indexItem.msgNum - (offset - indexItem.startMsgIndex));
                    hasFind = true;
                    continue;
                }
                if (hasFind && num > 0) {
                    results.add(indexItem);
                    num -= indexItem.msgNum;
                    continue;
                }
                if (num <= 0)
                    return results;
            }
        }
        return null;
    }
}
