package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.apache.catalina.LifecycleListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    /**
     * 根据店铺 ID 查询详情，当前采用逻辑过期缓存策略。
     *
     * @param id 店铺 ID
     * @return 查询成功时返回店铺信息，缓存未命中时返回店铺不存在
     */
    @Override
    public Result queryById(Long id) {
        Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById,
                RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //Shop shop = queryWithPassThrough(id);
        //Shop shop = queryWithLoginExpire(id);
        if (shop==null) {
            return Result.fail("店铺不存在");
        }
        //7.返回
        return Result.ok(shop);
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR
            = java.util.concurrent.Executors.newFixedThreadPool(10);



    /**
     * 使用互斥锁重建店铺缓存，缓解热点缓存失效引起的缓存击穿。
     * 缓存未命中时尝试获取锁，失败后休眠并重试；查库无结果时缓存空值，
     * 防止对不存在的店铺反复查询数据库。
     *
     * @param id 店铺 ID
     * @return 店铺信息；命中空值缓存或数据库中不存在时返回 null
     */
    public Shop queryWithMutex(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //1.查询redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //3.判断命中的是否是空值
        if (shopJson != null) {
            //命中的是空值，返回错误信息
            return null;
        }

        //4.实现缓存重建
        //4.1获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取成功
            if (!isLock) {
                //4.3失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.4成功，根据id查询数据库
            shop = getById(id);
            //模拟重建延时
            Thread.sleep(200);
            //5.不存在，返回错误
            if (shop==null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            //6.存在，写入redis缓存
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop)
                    ,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放互斥锁
            unlock(lockKey);
        }
        //8.返回
        return shop;
    }
    /**
     * 使用缓存空值的方式查询店铺，防止缓存穿透。
     * 优先读取缓存，未命中时查询数据库，并按查询结果缓存店铺信息或空字符串。
     *
     * @param id 店铺 ID
     * @return 店铺信息；命中空值缓存或数据库中不存在时返回 null
     */
    /* public Shop queryWithPassThrough(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        //1.查询redis缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //3.判断命中的是否是空值
        if (shopJson != null) {
            //命中的是空值，返回错误信息
            return null;
        }

        //4.不存在，根据id查询数据库
        Shop shop = getById(id);
        //5.不存在，返回错误
        if (shop==null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //6.存在，写入redis缓存
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop)
                ,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //7.返回
        return shop;
    }
    */



    /**
     * 查询数据库并写入带逻辑过期时间的店铺缓存，用于缓存预热或重建。
     * Redis 键本身不设置过期时间，由读取方检查数据中的逻辑过期时间。
     *
     * @param id 店铺 ID
     * @param expireSeconds 从当前时间起计算的逻辑有效期，单位为秒
     * @throws InterruptedException 模拟重建延时的休眠被中断时抛出
     */
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
    /**
     * 更新店铺信息，先更新数据库，再删除对应缓存。
     * 数据库更新操作由 Spring 事务管理。
     *
     * @param shop 待更新的店铺信息，必须包含店铺 ID
     * @return ID 为空时返回失败，否则返回成功
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id==null) {
            return Result.fail("店铺id不能为空");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
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
