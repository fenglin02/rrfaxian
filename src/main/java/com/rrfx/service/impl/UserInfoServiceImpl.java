package com.rrfx.service.impl;

import com.rrfx.entity.UserInfo;
import com.rrfx.mapper.UserInfoMapper;
import com.rrfx.service.IUserInfoService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;


@Service
public class UserInfoServiceImpl extends ServiceImpl<UserInfoMapper, UserInfo> implements IUserInfoService {

}
