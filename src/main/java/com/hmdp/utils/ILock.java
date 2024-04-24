package com.hmdp.utils;

public interface ILock {
    /**
     * 尝试获取锁
     *
     * @param timeoutSec 持有锁的超时时间，过期自动释放
     * @return
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unLock();
}
