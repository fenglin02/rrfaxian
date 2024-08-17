package com.rrfx;

import cn.hutool.json.JSONUtil;
import com.rrfx.entity.Shop;
import com.rrfx.service.IShopService;
import com.rrfx.utils.RedisData;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.rrfx.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.rrfx.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class RRfaxianApplicationTests {
    @Resource
    private IShopService service;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private ExecutorService es = Executors.newFixedThreadPool(500);
    private RedissonClient redissonClient;
    private RedissonClient redissonClient1;
    private RedissonClient redissonClient2;
    private RLock rLock;

    @Test
    void redisSession() {
        //redis集群
        RLock order = redissonClient.getLock("order");
        RLock order1 = redissonClient1.getLock("order");
        RLock order2 = redissonClient2.getLock("order");
        //创建multioLock
        rLock = redissonClient.getMultiLock(order, order1, order2);
    }

    @Test
    void testAdd() throws InterruptedException {
//
        this.saveShop2Redis(3L, 30L);
    }

    public void saveShop2Redis(Long id, Long expireTime) {
        RedisData redisData = new RedisData();
        Shop shop = service.getById(id);
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Test
    public void loadShopData() {
        List<Shop> shopList = service.list();
        Map<Long, List<Shop>> collect = shopList.stream().collect(Collectors.groupingBy(shop -> shop.getTypeId()));
        for (Map.Entry<Long, List<Shop>> entry : collect.entrySet()) {
            Long typeID = entry.getKey();
            String key = SHOP_GEO_KEY + typeID;
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(collect.size());
            for (Shop shop : value) {
                //GEOADD key x y member
                RedisGeoCommands.GeoLocation<String> location = new RedisGeoCommands.GeoLocation<String>(shop.getId().toString(), new Point(shop.getX(), shop.getY()));
                locations.add(location);
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }

    }

}

