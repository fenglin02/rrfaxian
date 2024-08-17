package com.rrfx.service.impl;

import cn.hutool.json.JSONUtil;
import com.rrfx.dto.Result;
import com.rrfx.entity.ShopType;
import com.rrfx.mapper.ShopTypeMapper;
import com.rrfx.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;
import java.util.stream.Collectors;

import static com.rrfx.utils.RedisConstants.CACHE_SHOPTYPES;


@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result getShopTypes() {
        List<String> range = stringRedisTemplate.opsForList().range(CACHE_SHOPTYPES, 0, -1);
        if (range.size()>0) {
            List<ShopType> shopTypes = range.stream().map(types -> JSONUtil.toBean(types, ShopType.class)).collect(Collectors.toList());
            return Result.ok(shopTypes);
        }
        List<ShopType> sort = query().orderByAsc("sort").list();
        if(sort.size()>0){
            List<String> collect = sort.stream().map(types -> JSONUtil.toJsonStr(types)).collect(Collectors.toList());
            stringRedisTemplate.opsForList().rightPushAll(CACHE_SHOPTYPES,collect);
        }
        return Result.ok(sort);
    }
}
