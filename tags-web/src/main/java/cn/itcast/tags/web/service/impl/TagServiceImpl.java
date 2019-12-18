package cn.itcast.tags.web.service.impl;

import cn.itcast.tags.web.bean.dto.ModelDto;
import cn.itcast.tags.web.bean.dto.TagDto;
import cn.itcast.tags.web.bean.dto.TagModelDto;
import cn.itcast.tags.web.bean.po.ModelPo;
import cn.itcast.tags.web.bean.po.TagPo;
import cn.itcast.tags.web.repo.ModelRepository;
import cn.itcast.tags.web.repo.TagRepository;
import cn.itcast.tags.web.service.Engine;
import cn.itcast.tags.web.service.TagService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class TagServiceImpl implements TagService {
    @Autowired
    private TagRepository tagRepo;
    @Autowired
    private ModelRepository modelRepo;
    @Autowired
    private Engine engine;


    @Override
    public void saveTags(List<TagDto> tags) {
        //[TagDto(id=null, name=电商, rule=null, level=1, pid=null),
        // TagDto(id=null, name=商城, rule=null, level=2, pid=null),
        // TagDto(id=null, name=属性, rule=null, level=3, pid=null)]
        /*
            将tags保存到数据库
            保存1级标签
            得到1级标签的ID
            将1级的ID给2级的父ID
            保存2级标签
            得到2级标签的ID
            将2级的ID给3级的父ID
        */
        //先将list进行排序,防止出现乱序.
        tags.sort((tag1, tag2) -> {
            if (tag1.getLevel() > tag2.getLevel()) {
                return 1;
            }
            if (tag1.getLevel() < tag2.getLevel()) {
                return -1;
            }
            return 0;
        });
        //开始将数据进行保存
        //  tagRepo.save()

        TagPo tmpPo = null;

        for (TagDto tag : tags) {
            //将dto转换为po,方便后面的数据操作
            TagPo tagPo = convert(tag);
            if (tmpPo == null) {
                //肯定是第一次循环,此时肯定是一级标签
                List<TagPo> list = tagRepo.findByNameAndLevelAndPid(tagPo.getName(), tagPo.getLevel(), tagPo.getPid());
                if (list == null || list.size() == 0) {
                    //没有查询到,那么就直接保存
                    tmpPo = tagRepo.save(tagPo);
                } else {
                    //如果查到,那么就将这个值取出来
                    tmpPo = list.get(0);
                }
            } else {
                //tmpPo不等于null,说明肯定是之前有存储过了
                //如果进入Else,那么肯定是二三级标签
                tagPo.setPid(tmpPo.getId());
                //我们可以使用标签名称/等级/父id进行查询,如果能查到,那么我们就不保存了.
                List<TagPo> list = tagRepo.findByNameAndLevelAndPid(tagPo.getName(), tagPo.getLevel(), tagPo.getPid());
                if (list == null || list.size() == 0) {
                    //没有查询到,那么就直接保存
                    tmpPo = tagRepo.save(tagPo);
                } else {
                    //如果查到.将数据库里面的值赋值给tmpPo
                    tmpPo = list.get(0);
                }
            }
        }
    }

    @Override
    public List<TagDto> findByPid(Long pid) {
        List<TagPo> list = tagRepo.findByPid(pid);
        //将po集合转换为dto集合.

//        for (TagPo tagPo : list) {
//            TagDto dto = convert(tagPo);
//        }
        return list.stream().map(this::convert).collect(Collectors.toList());
    }

    @Override
    public List<TagDto> findByLevel(Integer level) {
        List<TagPo> list = tagRepo.findByLevel(level);
        List<TagDto> listDto = list.stream().map(this::convert).collect(Collectors.toList());
        return listDto;
    }


    @Override
    public void addTagModel(TagDto tagDto, ModelDto modelDto) {
        TagPo tagPo = tagRepo.save(convert(tagDto));
        modelRepo.save(convert(modelDto, tagPo.getId()));
    }

    @Override
    public List<TagModelDto> findModelByPid(Long pid) {
        List<TagPo> tagPos = tagRepo.findByPid(pid);
        return tagPos.stream().map((tagPo) -> {
            Long id = tagPo.getId();
            ModelPo modelPo = modelRepo.findByTagId(id);
            if (modelPo == null) {
                //找不到model,就只返回tag
                return new TagModelDto(convert(tagPo),null);
            }
            return new TagModelDto(convert(tagPo), convert(modelPo));
        }).collect(Collectors.toList());
    }

    @Override
    public void addDataTag(TagDto tagDto) {
        tagRepo.save(convert(tagDto));
    }


    @Override
    public void updateModelState(Long id, Integer state) {
        ModelPo modelPo = modelRepo.findByTagId(id);
        //如果传递过来的状态是3,那么就是启动,如果是4那么就是停止

        if (state == ModelPo.STATE_ENABLE) {
            //启动流程
            engine.startModel(convert(modelPo));
        }
        if (state == ModelPo.STATE_DISABLE) {
            //关闭流程
            engine.stopModel(convert(modelPo));
        }
        //更新状态信息
        modelPo.setState(state);
        modelRepo.save(modelPo);
    }



    private ModelDto convert(ModelPo modelPo) {
        ModelDto modelDto = new ModelDto();
        modelDto.setId(modelPo.getId());
        modelDto.setName(modelPo.getName());
        modelDto.setMainClass(modelPo.getMainClass());
        modelDto.setPath(modelPo.getPath());
        modelDto.setArgs(modelPo.getArgs());
        modelDto.setState(modelPo.getState());
        modelDto.setSchedule(modelDto.parseDate(modelPo.getSchedule()));
        return modelDto;
    }

    private ModelPo convert(ModelDto modelDto, Long id) {
        ModelPo modelPo = new ModelPo();
        modelPo.setId(modelDto.getId());
        modelPo.setTagId(id);
        modelPo.setName(modelDto.getName());
        modelPo.setMainClass(modelDto.getMainClass());
        modelPo.setPath(modelDto.getPath());
        modelPo.setSchedule(modelDto.getSchedule().toPattern());
        modelPo.setCtime(new Date());
        modelPo.setUtime(new Date());
        modelPo.setState(modelDto.getState());
        modelPo.setArgs(modelDto.getArgs());
        return modelPo;
    }



    /**
     * po转换为dto对象
     * @param tagPo
     * @return
     */
    private TagDto convert(TagPo tagPo){
        TagDto tagDto = new TagDto();
        tagDto.setId(tagPo.getId());
        tagDto.setLevel(tagPo.getLevel());
        tagDto.setName(tagPo.getName());
        tagDto.setPid(tagPo.getPid());
        tagDto.setRule(tagPo.getRule());
        return tagDto;
    }


    /**
     * 将TagDto转换为TagPo对象
     * @return
     */
    private TagPo convert(TagDto tagDto) {
        TagPo tagPo = new TagPo();
        tagPo.setId(tagDto.getId());
        tagPo.setName(tagDto.getName());
        tagPo.setRule(tagDto.getRule());
        tagPo.setLevel(tagDto.getLevel());
        if (tagDto.getLevel() == 1) {
            //如果当前等级为1级,那么设置父ID为-1
            tagPo.setPid(-1L);
        } else {
            tagPo.setPid(tagDto.getPid());
        }
        tagPo.setCtime(new Date());
        tagPo.setUtime(new Date());
        return tagPo;
    }



}
