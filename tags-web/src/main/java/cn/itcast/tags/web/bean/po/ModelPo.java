package cn.itcast.tags.web.bean.po;

import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity(name = "tbl_model")
public class ModelPo {
    //当前模型的状态
    public static final Integer STATE_APPLYING = 1;
    public static final Integer STATE_PASSED = 2;
    public static final Integer STATE_ENABLE = 3;
    public static final Integer STATE_DISABLE = 4;
    //当前模型执行频次
    public static final Integer FREQUENCY_ONCE = 0;
    public static final Integer FREQUENCY_DAILY = 1;
    public static final Integer FREQUENCY_WEEKLY = 2;
    public static final Integer FREQUENCY_MONTHLY = 3;
    public static final Integer FREQUENCY_YEARLY = 4;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "tag_id")
    private Long tagId;

    @Column(name = "model_name")
    private String name;
    private Integer state; // 1申请中、2审核通过、3运行中、4未运行


    @Column(name = "model_path")
    private String path;
    @Column(name = "model_main")
    private String mainClass;
    @Column(name = "sche_time")
    private String schedule;
    private String args;

    private Date ctime;
    private Date utime;
}
