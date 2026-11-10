package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    private static final String SHOP_TYPE_LIST_KEY = "cache:shopType:list";

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        // 1. 查询 Redis 缓存
        String cachedJson = stringRedisTemplate.opsForValue().get(SHOP_TYPE_LIST_KEY);

        // 2. 命中缓存时，将 JSON 转为店铺类型列表
        if (StrUtil.isNotBlank(cachedJson)) {
            List<ShopType> shopTypes = JSONUtil.toList(cachedJson, ShopType.class);
            return Result.ok(shopTypes);
        }

        // 3. 缓存未命中时，按 sort 升序查询数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();

        // 4. 将列表序列化为 JSON 并写入 Redis
        stringRedisTemplate.opsForValue().set(
                SHOP_TYPE_LIST_KEY, JSONUtil.toJsonStr(shopTypes));

        return Result.ok(shopTypes);
    }
}
