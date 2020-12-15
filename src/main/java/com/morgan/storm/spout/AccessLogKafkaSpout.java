package com.morgan.storm.spout;

import com.alibaba.fastjson.JSON;
import com.morgan.storm.config.KafkaProperties;
import com.morgan.storm.context.SpringContextHolder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Description 从kafka数据源获取数据
 * @Author Morgan
 * @Date 2020/12/14 15:41
 **/
public class AccessLogKafkaSpout extends BaseRichSpout {

    private static final long serialVersionUID = -5878989225965137703L;

    private SpoutOutputCollector collector;
    private KafkaConsumer<String, String> consumer;
    private ConsumerRecords<String,String> accessLogs;
    private KafkaProperties kafkaProperties;
    private final static String topicName = "HotProductTopology";

    /**
     * 预处理工作
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        kafkaProperties = SpringContextHolder.getBean(KafkaProperties.class);
        initKafka();
    }

    /**
     * 核心业务逻辑处理
     */
    @Override
    public void nextTuple() {
        try {
            accessLogs = consumer.poll(Duration.ofMillis(100));
            if (accessLogs!=null && !accessLogs.isEmpty()){
                List<String> logs = new ArrayList<>();
                for (ConsumerRecord<String,String> record:accessLogs){
                    String value = record.value();
                    logs.add(value);
                }
                System.out.println("AccessLogKafkaSpout发送的数据" + logs.toString());
                collector.emit(new Values(JSON.toJSONString(logs)));
                consumer.commitSync();
            }else {
                TimeUnit.SECONDS.sleep(3);
            }
        } catch (InterruptedException e) {
            System.out.println("线程中断...");
            e.printStackTrace();
        }
    }

    /**
     * 流分组命名
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("accessLogList"));
    }

    private void initKafka() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaProperties.getServers());
        props.setProperty("enable.auto.commit",kafkaProperties.getEnableAutoCommit());
        props.setProperty("group.id",kafkaProperties.getGroupId());
        props.setProperty("auto.offset.reset",kafkaProperties.getAutoOffsetReset());
        props.setProperty("key.deserializer",kafkaProperties.getKeyDeserializer());
        props.setProperty("value.deserializer",kafkaProperties.getValueDeserializer());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        System.out.println("消息队列" + topicName + "初始化....");
    }
}
