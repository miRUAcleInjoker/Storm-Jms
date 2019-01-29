package com.neo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.jms.bolt.JmsBolt;
import org.apache.storm.jms.spout.JmsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.utils.Utils;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * @author wf
 * @Description ExampleJmsTopology
 * @Date 2019/1/29 15:21
 */
public class ExampleJmsTopology {
    private static final String JMS_QUEUE_SPOUT = "JMS_QUEUE_SPOUT";
    private static final String INTERMEDIATE_BOLT = "INTERMEDIATE_BOLT";
    private static final String FINAL_BOLT = "FINAL_BOLT";
    private static final String JMS_TOPIC_BOLT = "JMS_TOPIC_BOLT";
    private static final String JMS_TOPIC_SPOUT = "JMS_TOPIC_SPOUT";
    private static final String ANOTHER_BOLT = "ANOTHER_BOLT";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // JMS Queue Provider
        JmsProvider jmsQueueProvider = new SpringJmsProvider(
                "jms-activemq.xml",
                "jmsConnectionFactory",
                "notificationQueue");

        // JMS Topic provider
        JmsProvider jmsTopicProvider = new SpringJmsProvider(
                "jms-activemq.xml",
                "jmsConnectionFactory",
                "notificationTopic");

        // JMS Producer
        JmsTupleProducer producer = new JsonTupleProducer();

        // JMS Queue Spout
        JmsSpout queueSpout = new JmsSpout();
        queueSpout.setJmsProvider(jmsQueueProvider);
        queueSpout.setJmsTupleProducer(producer);
        queueSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);

        //Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(JMS_QUEUE_SPOUT, queueSpout, 5);
        // intermediate bolt, subscribes to jms spout, anchors on tuples, and auto-acks
        builder.setBolt(INTERMEDIATE_BOLT,
                new GenericBolt(true, true, new Fields("json"), "INTERMEDIATE_BOLT"), 3)
                .shuffleGrouping(JMS_QUEUE_SPOUT);
        //Final bolt that subscribes to the intermediate bolt, and auto-acks
        builder.setBolt(FINAL_BOLT,
                new GenericBolt("FINAL_BOLT", true, true), 3)
                .shuffleGrouping(INTERMEDIATE_BOLT);

        // bolt that subscribes to the intermediate bolt, and publishes to a JMS Topic
        JmsBolt jmsBolt = new JmsBolt();
        jmsBolt.setJmsProvider(jmsTopicProvider);
        jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
            public Message toMessage(Session session, ITuple input) throws JMSException {
                System.out.println("Sending JMS Message:" + input.toString());
                return session.createTextMessage(input.toString());
            }
        });
        builder.setBolt(JMS_TOPIC_BOLT, jmsBolt).shuffleGrouping(INTERMEDIATE_BOLT);

        // JMS Topic spout
        JmsSpout topicSpout = new JmsSpout();
        topicSpout.setJmsProvider(jmsTopicProvider);
        topicSpout.setJmsTupleProducer(producer);
        topicSpout.setJmsAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        topicSpout.setDistributed(false);

        builder.setSpout(JMS_TOPIC_SPOUT, topicSpout);

        builder.setBolt(ANOTHER_BOLT, new GenericBolt("ANOTHER_BOLT", true, true), 1).shuffleGrouping(
                JMS_TOPIC_SPOUT);

        Config config = new Config();
        config.setDebug(false);

        if (args != null && args.length > 0) {
            //集群模式
            config.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            //本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-jms-example", config, builder.createTopology());
            //300s后 shutdown
            Utils.sleep(300000);
            cluster.killTopology("storm-jms-example");
            cluster.shutdown();
        }

    }


}
