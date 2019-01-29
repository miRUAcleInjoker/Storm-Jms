package com.neo;

import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * @author wf
 * @Description JsonTupleProducer
 * @Date 2019/1/29 15:06
 */
public class JsonTupleProducer implements JmsTupleProducer {

    public Values toTuple(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            String json = ((TextMessage) message).getText();
            return new Values(json);
        } else {
            return null;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("json"));
    }
}
