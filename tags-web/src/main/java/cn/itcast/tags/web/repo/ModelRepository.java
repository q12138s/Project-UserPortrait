package cn.itcast.tags.web.repo;

import cn.itcast.tags.web.bean.po.ModelPo;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ModelRepository  extends JpaRepository<ModelPo, Long> {

    ModelPo findByTagId(Long tagId);
}
