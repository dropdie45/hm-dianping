package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.service.impl.ShopServiceImpl.CACHE_REBUILD_EXECUTOR;

@Component
@Slf4j
public class CacheClient {
    private static final AbstractExecutorService CACHE_REBUILD_EXECUTOR = (AbstractExecutorService) java.util.concurrent.Executors.newFixedThreadPool(10);
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, value.toString(), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 使用缓存空值的方式查询店铺，防止缓存穿透。
     * 优先读取缓存，未命中时查询数据库，并按查询结果缓存店铺信息或空字符串。
     *
     * @param id 店铺 ID
     * @return 店铺信息；命中空值缓存或数据库中不存在时返回 null
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1.查询redis缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //3.存在，直接返回
            R r = JSONUtil.toBean(json, type);
            return r;
        }
        //3.判断命中的是否是空值
        if (json != null) {
            //命中的是空值，返回错误信息
            return null;
        }

        //4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        //5.不存在，返回错误
        if (r==null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //6.存在，写入redis缓存
        this.set(key, JSONUtil.toJsonStr(r),time, unit);
        //7.返回
        return r;
    }

    /**
     * 使用逻辑过期策略查询店铺，适用于已预热的热点数据。
     * 缓存未命中时返回 null；逻辑过期后尝试获取互斥锁，异步重建缓存，
     * 当前请求仍返回旧数据，避免等待数据库查询。
     *
     * @param id 店铺 ID
     * @return 缓存中的店铺信息；缓存未命中时返回 null
     */
    public <R> R queryWithLoginExpire(String ketPrefix,Long id,Class<R> type,Function<Long,R> dbFallback,Long time,TimeUnit unit) {
        String key = ketPrefix + id;
        //1.查询redis缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isBlank(json)) {
            //3.缓存未命中，直接返回；此策略依赖提前预热数据
            return null;
        }
        //5.命中，需要先将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //6.判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            //7.未过期，直接返回店铺信息
            return r;
        }

        //8.已过期，需要缓存重建
        //9.获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //10.判断是否获取成功
        if (isLock){
            //11.成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    //重建缓存
                    R r1 = dbFallback.apply(id);
                    //模拟重建延时
                    Thread.sleep(200);
                    //写入redis
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }

        //12.无论是否获取到锁，当前请求都返回旧的店铺信息
        return shop;
    }

    /**
     * 通过 Redis SET NX 尝试获取互斥锁，并设置锁的自动过期时间。
     *
     * @param key 锁在 Redis 中的键
     * @return 获取成功返回 true，锁已存在或 Redis 返回 null 时返回 false
     */
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 删除指定的 Redis 锁键，释放互斥锁。
     *
     * @param key 锁在 Redis 中的键
     */
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
