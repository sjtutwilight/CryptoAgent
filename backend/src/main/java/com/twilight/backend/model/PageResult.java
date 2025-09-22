package com.twilight.backend.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 分页结果模型
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PageResult<T> {
    /**
     * 数据列表
     */
    private List<T> data;

    /**
     * 当前页码
     */
    private Integer page;

    /**
     * 每页大小
     */
    private Integer size;

    /**
     * 总记录数
     */
    private Long total;

    /**
     * 总页数
     */
    private Integer totalPages;

    /**
     * 是否有下一页
     */
    private Boolean hasNext;

    /**
     * 是否有上一页
     */
    private Boolean hasPrevious;

    /**
     * 创建分页结果
     * 
     * @param data 数据列表
     * @param page 当前页码
     * @param size 每页大小
     * @param total 总记录数
     * @return 分页结果
     */
    public static <T> PageResult<T> of(List<T> data, Integer page, Integer size, Long total) {
        PageResult<T> result = new PageResult<>();
        result.setData(data);
        result.setPage(page);
        result.setSize(size);
        result.setTotal(total);
        
        int totalPages = (int) Math.ceil((double) total / size);
        result.setTotalPages(totalPages);
        result.setHasNext(page < totalPages);
        result.setHasPrevious(page > 1);
        
        return result;
    }

    /**
     * 创建空的分页结果
     * 
     * @param page 当前页码
     * @param size 每页大小
     * @return 空分页结果
     */
    public static <T> PageResult<T> empty(Integer page, Integer size) {
        return of(List.of(), page, size, 0L);
    }
}
