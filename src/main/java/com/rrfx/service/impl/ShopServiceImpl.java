package com.rrfx.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.rrfx.dto.Result;
import com.rrfx.entity.Shop;
import com.rrfx.mapper.ShopMapper;
import com.rrfx.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.rrfx.utils.CacheClient;
import com.rrfx.utils.RedisData;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.rrfx.utils.RedisConstants.*;
import static com.rrfx.utils.SystemConstants.DEFAULT_PAGE_SIZE;


@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

  /*  private boolean tryLock(String key) {
        Boolean inuse = stringRedisTemplate.opsForValue().setIfAbsent(key, "inuse", CACHE_SHOP_TTL, TimeUnit.SECONDS);
        //因为直接返回会有拆箱的操作 所以有可能会返回空值
        return BooleanUtil.isTrue(inuse);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }*/

    @Override
    public Result queryById(Long id) {
        //缓存穿透
//         Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById,CACHE_SHOP_TTL,TimeUnit.MINUTES);
        //互斥解决缓存击穿
//        Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("商铺信息不存在！");
        }
        //互斥锁解决缓存击穿
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
   /* public Shop queryWithLogicExpire(Long id) {
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //判断是否为空
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }
        //存在获取过期时间
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        //判断是否过期 过期时间是不是在当前时间之后
        if(expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }
        //如果过期,缓存重建
        String localKey= LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(localKey);
        //判断是否获取锁成功
        if (isLock) {
            String shopJson1 = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
            //判断是否为空
            if (StrUtil.isBlank(shopJson1)) {
                return null;
            }
            //存在获取过期时间
            RedisData redisData1 = JSONUtil.toBean(shopJson1, RedisData.class);
            LocalDateTime expireTime1 = redisData1.getExpireTime();
            if(expireTime1.isAfter(LocalDateTime.now())){
                return shop;
            }
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    this.saveShop2Redis(id,1800L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    unLock(localKey);
                }
            });
        }
        return shop;
    }

    public Shop queryWithMutex(Long id) {
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //判断是否为空
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //不等于null就是空字符串
        if (shopJson != null) {
            return null;
        }

        Shop shop = null;
        try {
            boolean isLock = tryLock(LOCK_SHOP_KEY + id);
            //判断是否获取成功
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //再次判断是否redis缓存是否为空
            String shopJson1 = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
            //判断是否为空
            if (StrUtil.isNotBlank(shopJson1)) {
                return JSONUtil.toBean(shopJson1, Shop.class);
            }
            //不等于null就是空字符串
            if (shopJson1 != null) {
                return null;
            }
            shop = getById(id);
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            unLock(LOCK_SHOP_KEY + id);
        }
        return shop;
    }

    public Shop queryWithPassThrough(Long id) {
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //判断是否为空
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //不等于null就是空字符串
        if (shopJson != null) {
            return null;
        }
        Shop shop = getById(id);
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }*/

    public void saveShop2Redis(Long id, Long expireTime) {
        RedisData redisData = new RedisData();
        Shop shop = getById(id);
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        updateById(shop);
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据id查询
        if (x == null || y == null) {
            Page<Shop> shopPage = new Page<>(current, DEFAULT_PAGE_SIZE);
            Page<Shop> page = query().eq("type_id", typeId).page(shopPage);
            return Result.ok(page.getRecords());
        }
        //2.计算分页参数
        int from = (current - 1) * DEFAULT_PAGE_SIZE;
        int end = current * DEFAULT_PAGE_SIZE;
        //3.查询redis，按照距离排序，分页  结果：shopId，distance  GEOSEARCH key BYLONLAT x y BYRADIUS ? WITHDISTANCE
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(
                key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().limit(end).includeDistance().sortAscending()
        );
        //4.解析出id
        if(results==null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size()<=from){
            return Result.ok(Collections.emptyList());
        }
        //截取from-end的部分
        List<Long> ids=new ArrayList<>(list.size());
        Map<String,Distance> distanceMap=new HashMap<>(list.size());
        list.stream().skip(from).forEach(result->{
            //获取店铺id和对应到目标点的距离
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));

            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);
        });
        //5.根据id查询店铺
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for(Shop shop:shops){
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6.返回数据
        return Result.ok(shops);
    }
}
