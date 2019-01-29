package com.neo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author wf
 * @Description GenericBolt
 * @Date 2019/1/29 15:14
 */
public class GenericBolt extends BaseRichBolt {
    private OutputCollector collector;
    private boolean autoAck = false;
    private boolean autoAnchor = false;
    private Fields declaredFields;
    private String name;

    public GenericBolt(boolean autoAck, boolean autoAnchor, Fields declaredFields, String name) {
        this.autoAck = autoAck;
        this.autoAnchor = autoAnchor;
        this.declaredFields = declaredFields;
        this.name = name;
    }

    public GenericBolt(String name, boolean autoAck, boolean autoAnchor) {
        this(autoAck, autoAnchor, null, name);
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        System.out.println("[" + this.name + "] Received message: " + input);

        // only emit if we have declared fields.
        if (this.declaredFields != null) {
            System.out.println("[" + this.name + "] emitting: " + input);
            if (this.autoAnchor) {
                this.collector.emit(input, input.getValues());
            } else {
                this.collector.emit(input.getValues());
            }
        }

        if (this.autoAck) {
            System.out.println("[" + this.name + "] ACKing tuple: " + input);
            this.collector.ack(input);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (this.declaredFields != null) {
            declarer.declare(this.declaredFields);
        }
    }
}
