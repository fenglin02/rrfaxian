package com.rrfx.controller;


import com.rrfx.dto.Result;
import com.rrfx.service.IFollowService;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;


@RestController
@RequestMapping("/follow")
public class FollowController {
    @Resource
    private IFollowService service;

    @PutMapping("{id}/{isFollow}")
    public Result follow(@PathVariable("id") Long id, @PathVariable("isFollow") boolean isFollow) {
        return service.follow(id, isFollow);
    }

    @GetMapping("/or/not/{id}")
    public Result isFollow(@PathVariable("id") Long id) {
        return service.isFollow(id);
    }
    @GetMapping ("/common/{id}")
    public Result followCommons(@PathVariable("id")Long id){
        return service.followCommons(id);
    }


}
