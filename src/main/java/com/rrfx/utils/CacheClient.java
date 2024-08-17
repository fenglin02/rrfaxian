package com.rrfx.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.rrfx.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long expireTime, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),expireTime,unit);
    }

    public void setWithLogicalExpire(String key, Object value,Long expireTime,TimeUnit unit){
        RedisData redisData=new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(expireTime)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long expireTime, TimeUnit unit) {
        String key=keyPrefix+id;
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断是否为空
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        //不等于null就是空字符串
        if (json != null) {
            return null;
        }
        R r =dbFallback.apply(id);
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", expireTime, unit);
            return null;
        }
        this.set(key,r,expireTime,unit);
        return r;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);
    public <R,ID> R queryWithLogicExpire(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit) {
        String key=keyPrefix+id;
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断是否为空
        if (StrUtil.isBlank(json)) {
            return null;
        }
        //存在获取过期时间
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        //判断是否过期 过期时间是不是在当前时间之后
        if(expireTime.isAfter(LocalDateTime.now())){
            return r;
        }
        //如果过期,缓存重建
        String localKey= LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(localKey);
        //判断是否获取锁成功
        if (isLock) {
            String shopJson1 = stringRedisTemplate.opsForValue().get(key);
            //判断是否为空
            if (StrUtil.isBlank(shopJson1)) {
                return null;
            }
            //存在 获取过期时间
            RedisData redisData1 = JSONUtil.toBean(shopJson1, RedisData.class);
            LocalDateTime expireTime1 = redisData1.getExpireTime();
            R r1 = JSONUtil.toBean((JSONObject) redisData.getData(), type);
            if(expireTime1.isAfter(LocalDateTime.now())){
                return r1;
            }
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    R apply = dbFallback.apply(id);
                    RedisData redisData2=new RedisData();
                    redisData2.setData(apply);
                    redisData2.setExpireTime(LocalDateTime.now().plusMinutes(unit.toMinutes(time)));
                    stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData2));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unLock(localKey);
                }
            });
        }
        return r;
    }


    private boolean tryLock(String key) {
        Boolean inuse = stringRedisTemplate.opsForValue().setIfAbsent(key, "inuse", CACHE_SHOP_TTL, TimeUnit.SECONDS);
        //因为直接返回会有拆箱的操作 所以有可能会返回空值
        return BooleanUtil.isTrue(inuse);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }

}
