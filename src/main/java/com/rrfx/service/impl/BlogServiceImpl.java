package com.rrfx.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.rrfx.dto.Result;
import com.rrfx.dto.ScrollResult;
import com.rrfx.dto.UserDTO;
import com.rrfx.entity.Blog;
import com.rrfx.entity.Follow;
import com.rrfx.entity.User;
import com.rrfx.mapper.BlogMapper;
import com.rrfx.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.rrfx.service.IFollowService;
import com.rrfx.service.IUserService;
import com.rrfx.utils.SystemConstants;
import com.rrfx.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.rrfx.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.rrfx.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IFollowService iFollowService;

    @Override
    public Result queryHotBlog(Integer current) {

        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.isBlogLiked(blog);
            this.queryBlogUser(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        //1查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记记录不存在");
        }
        //查询用户所在id
        queryBlogUser(blog);
        //查询blog是否为当前用户点过赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        //获取用户id
        UserDTO userDTO = UserHolder.getUser();
        if(userDTO==null){
            //用户为空，无需查询是否点赞
            return;
        }
        Long userId = userDTO.getId();
        //判断当前用户是否已经点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score==null?false:true);
    }

    @Override
    public Result likeBlog(Long id) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //判断当前用户是否已经点赞
        String key = BLOG_LIKED_KEY + id;
        Double isMember = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (isMember==null) {
            //没点过赞，可以点赞
            //数据库点赞数+1
            boolean isSuccess = update().setSql("liked=liked+1").eq("id", id).update();
            if (isSuccess) {
                //存入redis zadd key value score
                stringRedisTemplate.opsForZSet().add(key, userId.toString(),  System.currentTimeMillis());
            }

        } else {
            //已经点过赞
            //数据库数量-1
            boolean success = update().setSql("liked=liked-1").eq("id", id).update();
            if (success) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        //查询笔记点赞用户的id 查询得分前五的id
        Set<String> top5Id = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if(top5Id==null || top5Id.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //根据查询到的用户 查询用户
        List<Long> ids = top5Id.stream().map(Long::valueOf).collect(Collectors.toList());
        //根根据id查询用户信息where id in（n1,n2） order by Field(id,n1,n2）
        String idStr= StrUtil.join(",",ids);
        List<UserDTO> userDTOS = userService.query()
                .in("id",ids).last("order by field(id,"+idStr+")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result queryBlogByUserId(Integer current, Long id) {
        Page<Blog> blogPage = new Page<>(current,SystemConstants.MAX_PAGE_SIZE);
        Page<Blog> page = query().eq("user_id", id).page(blogPage);
        //获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.ok("新增笔记失败!");
        }
        //查询笔记作者的所有粉丝 select * from tb_follow where follow_user_id=?
        LambdaQueryWrapper<Follow> queryWrapper = new LambdaQueryWrapper<>();
        queryWrapper.eq(Follow::getFollowUserId,user.getId());
        List<Follow> follows = iFollowService.list(queryWrapper);
        //推送笔记id给所有分数
        for (Follow follow:follows){
            Long userId = follow.getUserId();
            //推送
            String key=FEED_KEY+userId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2.查询收件箱 ZREVRANGEBYSCORE key Max Min WITHSCORES LIMIT offset count
        String key=FEED_KEY+userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if(typedTuples==null||typedTuples.isEmpty()){
            return Result.ok();
        }
        //3.解析数据:blogId,minTime(时间戳),offset
        List<Long> ids=new ArrayList<>(typedTuples.size());
        long minTime=0;
        int num=1;
         for(ZSetOperations.TypedTuple<String> tuple:typedTuples){
             //获取分数
             long time = tuple.getScore().longValue();
             //获取blogId
             ids.add(Long.valueOf(tuple.getValue()));
             if(time==minTime){
                 num++;
             }else{
                 minTime=time;
                 num=1;
             }
         }
        //4根据blogId获取blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("order by field(id," + idStr + ")").list();
        blogs.forEach(blog -> {
            //查询笔记用户信息
            queryBlogUser(blog);
            //查询blog是否被当前用户点过赞
            isBlogLiked(blog);
        });
        //5.封装
        ScrollResult result = new ScrollResult();
        result.setList(blogs);
        result.setOffset(num);
        result.setMinTime(minTime);
        return Result.ok(result);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
