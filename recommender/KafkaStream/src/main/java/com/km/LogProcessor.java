package com.km;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[],byte[]> {
    public static final String PREFIX_MSG = "abc:";

    //上下文信息
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        String ratingValue = new String(value);
        if (ratingValue.contains(PREFIX_MSG)) {
            String bValue = ratingValue.split(PREFIX_MSG)[1];
            context.forward("log".getBytes(),bValue.getBytes());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
