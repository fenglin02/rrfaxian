package com.rrfx.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.rrfx.dto.Result;
import com.rrfx.dto.UserDTO;
import com.rrfx.entity.Follow;
import com.rrfx.entity.User;
import com.rrfx.mapper.FollowMapper;
import com.rrfx.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.rrfx.service.IUserService;
import com.rrfx.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;

    @Override
    public Result follow(Long id, boolean isFollow) {
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        //1,判断是关注还是取关
        if (isFollow) {
            //2,关注 增加数据
            Follow follow = new Follow();
            follow.setFollowUserId(id);
            follow.setUserId(userId);
            boolean success = save(follow);
            if (success) {
                //把关注的用户id,放入redis的set集合 sadd userId followUserId
                String key = "follows:" + userId;
                stringRedisTemplate.opsForSet().add(key, id.toString());
            }
        } else {
            //3,取关 删除数据 delete from tb_follow where userId=? and follow_user_id=?
            LambdaQueryWrapper<Follow> lambdaQueryWrapper = new LambdaQueryWrapper<>();
            lambdaQueryWrapper.eq(Follow::getUserId, userId).eq(Follow::getFollowUserId, id);
            boolean success = remove(lambdaQueryWrapper);
            if (success) {
                //把取关的用户id从set集合中移除
                String key = "follows:" + userId;
                stringRedisTemplate.opsForSet().remove(key, id.toString());
            }
        }


        return Result.ok();
    }

    @Override
    public Result isFollow(Long id) {
        //1.查询是否关注
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        LambdaQueryWrapper<Follow> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(Follow::getUserId, userId).eq(Follow::getFollowUserId, id);
        int count = count(queryWrapper);
        return Result.ok(count > 0 ? true : false);
    }

    @Override
    public Result followCommons(Long id) {
        //1,获取当前用户
        Long userId = UserHolder.getUser().getId();
        //求交集
        String key = "follows:" + userId;
        String followCommonsKey="follows:"+id;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, followCommonsKey);
        if(intersect==null||intersect.isEmpty()){
            return Result.ok();
        }
        //解析id 转换为Long
        List<Long> userIds = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //查询用户
        List<User> users = userService.listByIds(userIds);
        List<UserDTO>  userDTOS = users.stream().map(user -> {
           return BeanUtil.copyProperties(user, UserDTO.class);
        }).collect(Collectors.toList());
        return Result.ok(userDTOS);
    }
}
