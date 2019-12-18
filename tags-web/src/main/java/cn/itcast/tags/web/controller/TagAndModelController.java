package cn.itcast.tags.web.controller;


import cn.itcast.tags.up.HdfsTools;
import cn.itcast.tags.web.bean.Codes;
import cn.itcast.tags.web.bean.HttpResult;
import cn.itcast.tags.web.bean.dto.ModelDto;
import cn.itcast.tags.web.bean.dto.TagDto;
import cn.itcast.tags.web.bean.dto.TagModelDto;
import cn.itcast.tags.web.service.TagService;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;

@RestController
public class TagAndModelController {
    @Autowired
    private TagService tagService;

    /**
     * 123级标签添加
     * @param tags
     */
    @PutMapping("tags/relation")
    public void addTag(@RequestBody List<TagDto> tags){
        System.out.println(tags);
        tagService.saveTags(tags);
    }

    /**
     * 123级标签显示
     * @param pid
     * @param level
     * @return
     */
    @GetMapping("tags")
    public HttpResult<List<TagDto>> findTagByPidOrLevel(@RequestParam(required = false) Long pid, @RequestParam(required = false) Integer level){
        List<TagDto> list = null;
        //如果传过来的是父ID,那么就用父ID查询
        if (pid != null) {
            list = tagService.findByPid(pid);
        }
        //如果传过来的是等级,就按照等级进行查询
        if (level != null) {
            list = tagService.findByLevel(level);
        }
        //比如本次执行是否成功
        return new HttpResult<List<TagDto>>(Codes.SUCCESS, "查询成功", list);
    }


    /**
     * 四级界面新增标签模型
     *
     * 一个4级标签对应1个模型
     * 标签：标签的名称,标签的规则,标签等级
     * 模型：有Jar包的路径,Jar包的执行计划.Jar包执行的主类,Jar包执行的时候额外的参数
     *
     *
     * @param tagModelDto
     * @return
     */
    @PutMapping("tags/model")
    public HttpResult putModel(@RequestBody TagModelDto tagModelDto){
        System.out.println(tagModelDto);
        tagService.addTagModel(tagModelDto.getTag(), tagModelDto.getModel());
        return new HttpResult(Codes.SUCCESS, "成功", null);
    }

    @GetMapping("tags/model")
    public HttpResult getModel(Long pid){
        List<TagModelDto> dto = tagService.findModelByPid(pid);
        return new HttpResult(Codes.SUCCESS, "查询成功", dto);
    }


    @PutMapping("tags/data")
    public HttpResult putData(@RequestBody TagDto tagDto){
        tagService.addDataTag(tagDto);
        return new HttpResult(Codes.SUCCESS, "添加成功", null);
    }

    /**
     * 启动/停止模型
     * @param id
     * @param modelDto
     * @return
     */
    @PostMapping("tags/{id}/model")
    public HttpResult changeModelState(@PathVariable Long id, @RequestBody ModelDto modelDto){
        tagService.updateModelState(id, modelDto.getState());
        return new HttpResult(Codes.SUCCESS, "执行成功", null);
    }


    /**
     * 文件上传
     * @param file
     * @return
     */
    @PostMapping("/tags/upload")
    public HttpResult<String> postTagsFile(@RequestParam("file") MultipartFile file) {
        String basePath = "/apps/temp/jars/";
        //创建Jar包名字
        String fileName = UUID.randomUUID().toString() + ".jar";
        String path = basePath + fileName;
        try {
            InputStream inputStream = file.getInputStream();
            IOUtils.copy(inputStream, new FileOutputStream(new File("temp.jar")));
            HdfsTools.build().uploadLocalFile2HDFS("temp.jar",path);
            return new HttpResult<>(Codes.SUCCESS, "", "hdfs://bigdata-cdh01.itcast.cn:8020"+path);
        } catch (IOException e) {
            e.printStackTrace();
            return new HttpResult<>(Codes.ERROR, "文件上传失败", null);
        }
    }



}
