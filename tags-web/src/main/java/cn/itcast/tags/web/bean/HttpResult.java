package cn.itcast.tags.web.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 响应结果的封装类
 */
@Data
@AllArgsConstructor //生成有参构造
public class HttpResult<T> {
    private Integer code;   //666成功    444失败
    private String msg;//提示信息,一般都是让用户看的.
    private T data;//我们响应的集合数据
}
