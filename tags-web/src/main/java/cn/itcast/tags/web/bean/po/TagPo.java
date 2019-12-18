package cn.itcast.tags.web.bean.po;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.util.Date;

@Data
@Entity(name = "tbl_basic_tag")
public class TagPo {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;
    private String rule;
    private Integer level;
    private Long pid;

    private Date ctime;
    private Date utime;
}
