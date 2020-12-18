package com.morgan.storm.config;

import com.morgan.storm.constant.GlobalConstants;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @Description Zookeeper连接管理
 * @Author Morgan
 * @Date 2020/11/2 10:57
 **/
public class ZookeeperSession {

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private ZooKeeper zooKeeper;

    @Value("${zookeeper.connectString}")
    private String zookeeperLink;

    public ZookeeperSession() {
        // 去连接Zookeeper Server，创建会话的时候，是异步进行的
        // 要给一个监听器，监听Zookeeper端返回的响应信息
        try {
            this.zooKeeper = new ZooKeeper(zookeeperLink, 50, new ZookeeperWatcher());
            System.out.println(zooKeeper.getState());
            countDownLatch.await();
            System.out.println("Zookeeper session established.....");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void acquireDistributedLock(String path){
        try {
            zooKeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("success to acquire lock for " + path);
        } catch (KeeperException e) {
            int count = 0;
            while (true){
                try {
                    Utils.sleep(1000);
                    zooKeeper.create(path,"".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                } catch (KeeperException ex) {
                    count++;
                    continue;
                }
                System.out.println("success to acquire lock for " + path +" after " + count + "times try....");
                break;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void releaseDistributedLock(String path){
        try {
            zooKeeper.delete(path,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public String getNodeData(String path){
        try {
            return new String(zooKeeper.getData(path,false,new Stat()));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return GlobalConstants.SpecialChar.BLANK;
    }

    public void setNodeData(String path,String data){
        try {
            zooKeeper.setData(path,data.getBytes(),-1);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * zookeeper连接监听器
     */
    private class ZookeeperWatcher implements Watcher{
        @Override
        public void process(WatchedEvent event) {
            System.out.println("Receive watched event：" + event.getState());
            if (Event.KeeperState.SyncConnected == event.getState()){
                countDownLatch.countDown();
            }
        }
    }

    /**
     * 静态内部类实现单例
     */
    private static class Singleton{

        private static ZookeeperSession zookeeperSession;

        static {
            zookeeperSession = new ZookeeperSession();
        }

        private static ZookeeperSession getInstance(){
            return zookeeperSession;
        }
    }

    /**
     * 获取单例
     * @return
     */
    public static ZookeeperSession getInstance(){
        return Singleton.getInstance();
    }

    public static void init(){
        getInstance();
    }
}
