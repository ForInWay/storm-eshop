package com.morgan.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.morgan.storm.config.ZookeeperSession;
import com.morgan.storm.constant.GlobalConstants;
import com.morgan.storm.utils.Tools;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Description 热门商品访问数量统计
 * @Author Morgan
 * @Date 2020/12/15 17:15
 **/
public class ProductCountBolt extends BaseRichBolt {

    private static final long serialVersionUID = 6665177101793035662L;

    private ZookeeperSession zkSession;
    private LRUMap<Long,Long> productCountMap = new LRUMap<>(1000);
    private int taskId;
    private final static String TASK_LIST_LOCK = "task-list-lock";
    private final static String TASK_ID_LIST = "task-id-list";
    private final static String PRODUCT_LIST_PREFIX = "task-hot-product-list-";

    private class ProductCountThread implements Runnable{
        @Override
        public void run() {
            // 每隔1分钟统计，统计最热的商品top3
            List<Map.Entry<Long,Long>> topThreeProductList = new ArrayList<>();
            while (true){
                topThreeProductList.clear();
                int topN = 3;
                if (productCountMap.size() == 0){
                    Utils.sleep(1000);
                    continue;
                }
                for (Map.Entry<Long,Long> productCountEntry: productCountMap.entrySet()){
                    boolean hasMoved = false;
                    // 具体算法逻辑
                    if (topThreeProductList.size() == GlobalConstants.DIGITAL.ZERO){
                        topThreeProductList.add(productCountEntry);
                    }else{
                        for (int i = 0,size=topThreeProductList.size(); i < size; i++){
                            Map.Entry<Long, Long> topThreeProduct = topThreeProductList.get(i);
                            if (productCountEntry.getValue() >= topThreeProduct.getValue()){
                                int lastIndex = topThreeProductList.size() <= topN ? topThreeProductList.size() - 1 : topThreeProductList.size() -2;
                                for (int j = lastIndex; j > i; j--){
                                    topThreeProductList.set(j+1,topThreeProductList.get(j));
                                }
                                topThreeProductList.set(i,productCountEntry);
                                hasMoved = true;
                                break;
                            }
                        }
                    }
                    if (!hasMoved && topThreeProductList.size() <= topN){
                        topThreeProductList.add(productCountEntry);
                    }
                }

                String topThreeProductListString = JSONArray.toJSONString(topThreeProductList);
                zkSession.setNodeData(GlobalConstants.SpecialChar.FORWARD_SLASH + PRODUCT_LIST_PREFIX + taskId,topThreeProductListString);
                Utils.sleep(60000);
            }
        }
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.zkSession = ZookeeperSession.getInstance();
        this.taskId = context.getThisTaskId();
        new Thread(new ProductCountThread()).start();
        initTaskId(taskId);
    }

    /**
     * 每个任务启动时得把taskId存储到zookeeper节点
     * @param taskId 任务id
     */
    private void initTaskId(int taskId) {
        zkSession.acquireDistributedLock(GlobalConstants.SpecialChar.FORWARD_SLASH + TASK_LIST_LOCK);
        String taskIds = zkSession.getNodeData(GlobalConstants.SpecialChar.FORWARD_SLASH + TASK_ID_LIST);
        if (Tools.isNotEmpty(taskIds)){
            if (!taskIds.contains(String.valueOf(taskId))){
                taskIds += "," + taskId;
            }
        }else {
            taskIds += taskId;
        }
        zkSession.setNodeData(GlobalConstants.SpecialChar.FORWARD_SLASH + TASK_ID_LIST,taskIds);
        zkSession.releaseDistributedLock(GlobalConstants.SpecialChar.FORWARD_SLASH + TASK_LIST_LOCK);
    }

    @Override
    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");
        Long count = productCountMap.get(productId);
        if (Tools.isEmpty(count)){
            count = 0L;
        }
        count++;
        productCountMap.put(productId,count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
