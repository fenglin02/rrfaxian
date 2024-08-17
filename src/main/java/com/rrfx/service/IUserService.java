package com.rrfx.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.rrfx.dto.LoginFormDTO;
import com.rrfx.dto.Result;
import com.rrfx.entity.User;

import javax.servlet.http.HttpSession;


public interface IUserService extends IService<User> {

    Result sendCode(String phone, HttpSession session);

    Result login(LoginFormDTO loginForm, HttpSession session);

    Result queryUserById(Long userId);

    Result sign();

    Result signCount();
}
