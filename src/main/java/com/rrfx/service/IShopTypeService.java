package com.rrfx.service;

import com.rrfx.dto.Result;
import com.rrfx.entity.ShopType;
import com.baomidou.mybatisplus.extension.service.IService;


public interface IShopTypeService extends IService<ShopType> {

    Result getShopTypes();
}
