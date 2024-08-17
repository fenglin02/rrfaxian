package com.rrfx.service;

import com.rrfx.dto.Result;
import com.rrfx.entity.Follow;
import com.baomidou.mybatisplus.extension.service.IService;


public interface IFollowService extends IService<Follow> {

    Result follow(Long id, boolean isFollow);

    Result isFollow(Long id);

    Result followCommons(Long id);
}
