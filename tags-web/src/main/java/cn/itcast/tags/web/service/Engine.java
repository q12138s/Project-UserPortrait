package cn.itcast.tags.web.service;


import cn.itcast.tags.web.bean.dto.ModelDto;

public interface Engine {

    void startModel(ModelDto modelDto);
    void stopModel(ModelDto modelDto);
}
