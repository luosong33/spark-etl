package org.yylc.xfjr.util;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaConsumerSimple implements Runnable {
    public String title;
    public KafkaStream<byte[], byte[]> stream;
//    int count = 0;

    public KafkaConsumerSimple(String title, KafkaStream<byte[], byte[]> stream) {
        this.title = title;
        this.stream = stream;
    }

    @Override
    public void run() {
        System.out.println("开始运行 " + title);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        /**
         * 不停地从stream读取新到来的消息，在等待新的消息时，hasNext()会阻塞
         * 如果调用 `ConsumerConnector#shutdown`，那么`hasNext`会返回false
         * */
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> data = it.next();
            String topic = data.topic();
            int partition = data.partition();
            long offset = data.offset();
            String msg = new String(data.message());
            /*System.out.println(String.format(
                    "Consumer: [%s],  Topic: [%s],  PartitionId: [%d], Offset: [%d], msg: [%s]",
                    title, topic, partition, offset, msg));*/
//            int count = 0;
            String s = msg;
            List<Map<String, String>> listMap = JsonUtil.json2listMap(s);  //  解析json
            HBaseBasic02.putBach("communication:contact_list", "c", listMap);
//            count += listMap.size();
        }
//        System.out.println("count==================" + count);
//        System.out.println(String.format("Consumer: [%s] exiting ...", title));
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("group.id", "group_test_contact_list");
        props.put("zookeeper.connect", "node1:2181,node2:2181,node3:2181,node4:2181,node5:2181");
        props.put("auto.offset.reset", "largest");
        props.put("auto.commit.interval.ms", "1000");
        props.put("partition.assignment.strategy", "roundrobin");
        ConsumerConfig config = new ConsumerConfig(props);
        String topic1 = "test_contact_list";
        String topic2 = "paymentMq";
        //只要ConsumerConnector还在的话，consumer会一直等待新消息，不会自己退出
        ConsumerConnector consumerConn = Consumer.createJavaConsumerConnector(config);
        //定义一个map
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic1, 3);
        //Map<String, List<KafkaStream<byte[], byte[]>> 中String是topic， List<KafkaStream<byte[], byte[]>是对应的流
        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamsMap = consumerConn.createMessageStreams(topicCountMap);
        //取出 `kafkaTest` 对应的 streams
        List<KafkaStream<byte[], byte[]>> streams = topicStreamsMap.get(topic1);
        //创建一个容量为4的线程池
        ExecutorService executor = Executors.newFixedThreadPool(3);
        //创建20个consumer threads
        for (int i = 0; i < streams.size(); i++)
            executor.execute(new KafkaConsumerSimple("消费者" + (i + 1), streams.get(i)));

        //关闭consumer
        /*try {
            Thread.sleep(1000*3600*24*30);  //  单位毫秒
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (consumerConn != null) {
            consumerConn.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }*/
    }
}