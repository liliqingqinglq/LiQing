package cn.itcast.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperDemo {
    public static void main(String[] args)
            throws IOException, InterruptedException, KeeperException {
      deleteNode();
      List<String> childNode = getChildNode();
      for (String child : childNode){
          System.out.println("持久结点zkapi的子结点："+childNode);
      }
    }


    public static ZooKeeper getConnect()
            throws IOException, InterruptedException {
        String zkServers = "hadoop3:2181";
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ZooKeeper zooKeeper = new ZooKeeper(zkServers, 3000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("通知状态：" + watchedEvent.getState() + "\t"
                        + "事件类型：" + watchedEvent.getType() + "\t"
                        + "结点路径：" + watchedEvent.getPath());
                if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                    countDownLatch.countDown();
                }

            }
        });
        countDownLatch.await();
        return zooKeeper;
    }

    public static void createNode()
            throws IOException, InterruptedException, KeeperException {
        ZooKeeper connect = getConnect();
        connect.create(
                "/zkapi",
                "fruit".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        connect.create(
                "/zkapi/zkChild",
                "apple".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

    }
    public static Stat existsNode()
        throws IOException, InterruptedException, KeeperException {
        ZooKeeper connect = getConnect();
        Stat exists = connect.exists("/zkapi", true);
        return exists;
    }
    public static byte[] getNode()
        throws IOException, InterruptedException, KeeperException {
        ZooKeeper connect = getConnect();
        byte[] data = connect.getData("/zkapi", true, null);
        return data;
    }
    public static Stat updateNode()
        throws IOException, InterruptedException, KeeperException {
        ZooKeeper connect = getConnect();
        Stat stat = connect.setData(
                "/zkapi", "fruit_new".getBytes(),
                -1);
        return stat;
    }
    public static List<String>getChildNode()
        throws IOException, InterruptedException, KeeperException {
        ZooKeeper connect = getConnect();
        List<String> nodeList = connect.getChildren("/zkapi", false);
        return nodeList;
    }
    public static void deleteNode()
        throws IOException, InterruptedException, KeeperException {
        ZooKeeper connect = getConnect();
        connect.delete("/zkapi/zkChild", -1);
    }
}