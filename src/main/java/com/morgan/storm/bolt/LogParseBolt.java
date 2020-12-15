package com.morgan.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * @Description 日志解析的bolt
 * @Author Morgan
 * @Date 2020/12/15 17:02
 **/
public class LogParseBolt extends BaseRichBolt {

    private static final long serialVersionUID = 7008756471342721919L;

    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String accessLogJson = tuple.getStringByField("accessLogList");
        List<String> accessLogList = JSONArray.parseArray(accessLogJson, String.class);
        accessLogList.forEach(accessLog -> {
            JSONObject accessLogObject = JSONObject.parseObject(accessLog);
            Long productId = accessLogObject.getLong("productId");
            if (productId != null){
                collector.emit(new Values(productId));
            }
        });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("declarer"));
    }
}
