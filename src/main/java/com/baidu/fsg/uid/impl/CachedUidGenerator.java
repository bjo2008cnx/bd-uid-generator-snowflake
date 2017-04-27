package com.baidu.fsg.uid.impl;

import com.baidu.fsg.uid.BitsAllocator;
import com.baidu.fsg.uid.buffer.*;
import com.baidu.fsg.uid.exception.UidGenerateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

public class CachedUidGenerator extends DefaultUidGenerator implements DisposableBean {
    private static Logger log = LoggerFactory.getLogger(CachedUidGenerator.class);
    private static final int DEFAULT_BOOST_POWER = 3; //?

    /**
     * Spring 属性
     */
    private int boostPower = DEFAULT_BOOST_POWER;
    private int paddingFactor = RingBuffer.DEFAULT_PADDING_PERCENT;
    private Long scheduleInterval;

    private RejectedPutBufferHandler rejectedPutBufferHandler;
    private RejectedTakeBufferHandler rejectedTakeBufferHandler;

    /**
     * RingBuffer
     */
    private RingBuffer ringBuffer;
    private BufferPaddingExecutor bufferPaddingExecutor;

    @Override
    public void afterPropertiesSet() {
        // 初始化workerId & bitsAllocator
        super.afterPropertiesSet();

        // 初始化 RingBuffer & RingBufferPaddingExecutor
        this.initRingBuffer();
        log.info("Initialized RingBuffer successfully.");
    }

    @Override
    public long getUID() {
        try {
            return ringBuffer.take();
        } catch (Exception e) {
            log.error("Generate unique id exception. ", e);
            throw new UidGenerateException(e);
        }
    }

    @Override
    public String parseUID(long uid) {
        return super.parseUID(uid);
    }

    @Override
    public void destroy() {
        bufferPaddingExecutor.shutdown();
    }

    /**
     * 取同一秒下的id
     *
     * @param currentSecond
     * @return UID list, size of {@link BitsAllocator#getMaxSequence()} + 1
     */
    protected List<Long> nextIdsForOneSecond(long currentSecond) {
        // 初始化列表
        int listSize = (int) bitsAllocator.getMaxSequence() + 1;
        List<Long> uidList = new ArrayList<>(listSize);

        //分配第一个Uid,其余可根据位移计算出来
        long firstSeqUid = bitsAllocator.allocate(currentSecond - epochSeconds, workerId, 0L);
        for (int offset = 0; offset < listSize; offset++) {
            uidList.add(firstSeqUid + offset);
        }
        return uidList;
    }

    /**
     * 初始化 RingBuffer & RingBufferPaddingExecutor
     */
    private void initRingBuffer() {
        // 初始化 RingBuffer
        int bufferSize = ((int) bitsAllocator.getMaxSequence() + 1) << boostPower;
        this.ringBuffer = new RingBuffer(bufferSize, paddingFactor);
        log.info("Initialized ring buffer size:{}, paddingFactor:{}", bufferSize, paddingFactor);

        // initialize RingBufferPaddingExecutor
        boolean usingSchedule = (scheduleInterval != null);

        //jdk 1.8-->1.7
        //this.bufferPaddingExecutor = new BufferPaddingExecutor(ringBuffer, this::nextIdsForOneSecond, usingSchedule);
        BufferedUidProvider uidProvider = new BufferedUidProvider() {
            @Override
            public List<Long> provide(long momentInSecond) {
                return nextIdsForOneSecond(momentInSecond);
            }
        };

        this.bufferPaddingExecutor = new BufferPaddingExecutor(ringBuffer, uidProvider, usingSchedule);
        if (usingSchedule) {
            bufferPaddingExecutor.setScheduleInterval(scheduleInterval);
        }

        log.info("Initialized BufferPaddingExecutor. Using schdule:{}, interval:{}", usingSchedule, scheduleInterval);

        // set rejected put/take handle policy
        this.ringBuffer.setBufferPaddingExecutor(bufferPaddingExecutor);
        if (rejectedPutBufferHandler != null) {
            this.ringBuffer.setRejectedPutHandler(rejectedPutBufferHandler);
        }
        if (rejectedTakeBufferHandler != null) {
            this.ringBuffer.setRejectedTakeHandler(rejectedTakeBufferHandler);
        }

        // fill in all slots of the RingBuffer
        bufferPaddingExecutor.paddingBuffer();

        // start buffer padding threads
        bufferPaddingExecutor.start();
    }

    /**
     * Setters for spring property
     */
    public void setBoostPower(int boostPower) {
        Assert.isTrue(boostPower > 0, "Boost power must be positive!");
        this.boostPower = boostPower;
    }

    public void setRejectedPutBufferHandler(RejectedPutBufferHandler rejectedPutBufferHandler) {
        Assert.notNull(rejectedPutBufferHandler, "RejectedPutBufferHandler can't be null!");
        this.rejectedPutBufferHandler = rejectedPutBufferHandler;
    }

    public void setRejectedTakeBufferHandler(RejectedTakeBufferHandler rejectedTakeBufferHandler) {
        Assert.notNull(rejectedTakeBufferHandler, "RejectedTakeBufferHandler can't be null!");
        this.rejectedTakeBufferHandler = rejectedTakeBufferHandler;
    }

    public void setScheduleInterval(long scheduleInterval) {
        Assert.isTrue(scheduleInterval > 0, "Schedule interval must positive!");
        this.scheduleInterval = scheduleInterval;
    }

}
