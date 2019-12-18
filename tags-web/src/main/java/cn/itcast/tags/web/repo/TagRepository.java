package cn.itcast.tags.web.repo;

import cn.itcast.tags.web.bean.po.TagPo;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TagRepository extends JpaRepository<TagPo, Long> {

    public List<TagPo> findByNameAndLevelAndPid(String name, Integer level, Long pid);

    /**
     * 根据PID进行查询
     * @param pid
     * @return
     */
    public List<TagPo> findByPid(Long pid);

    /**
     * 根据等级进行查询
     * @param level
     * @return
     */
    public List<TagPo> findByLevel(Integer level);
}
