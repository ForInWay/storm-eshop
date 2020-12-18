package com.morgan.storm.bolt;

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

    private class ProductCountThread implements Runnable{
        @Override
        public void run() {
            // 每隔1分钟统计，统计最热的商品top3
            List<Map.Entry<Long,Long>> topThreeProductList = new ArrayList<>();
            while (true){
                topThreeProductList.clear();
                int topN = 3;
                if (productCountMap.size() == 0){
                    Utils.sleep(100);
                    continue;
                }
                for (Map.Entry<Long,Long> productCountEntry: productCountMap.entrySet()){
                    // 具体算法逻辑
                    if (topThreeProductList.size() == GlobalConstants.DIGITAL.ZERO){
                        topThreeProductList.add(productCountEntry);
                    }
                    for (Map.Entry<Long,Long> product: topThreeProductList){

                    }
                }
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
