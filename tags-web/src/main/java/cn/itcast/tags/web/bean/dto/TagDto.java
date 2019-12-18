package cn.itcast.tags.web.bean.dto;

import lombok.Data;

@Data
public class TagDto {
    private Long id;
    private String name;
    private String rule; //标签的规则
    private Integer level;
    private Long pid; //标签的父ID
}
