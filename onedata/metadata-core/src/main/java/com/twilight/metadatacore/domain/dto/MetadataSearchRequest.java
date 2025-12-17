package com.twilight.metadatacore.domain.dto;

import com.twilight.metadatacore.domain.enums.MetadataStatus;
import java.util.Collections;
import java.util.List;

public class MetadataSearchRequest {

    private String keyword;
    private String domain;
    private String type;
    private String platform;
    private MetadataStatus status;
    private List<String> tags = Collections.emptyList();
    private int page = 0;
    private int size = 20;
    private String sortBy = "updatedAt";
    private SortDirection sortDirection = SortDirection.DESC;

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        this.keyword = keyword;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public MetadataStatus getStatus() {
        return status;
    }

    public void setStatus(MetadataStatus status) {
        this.status = status;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public SortDirection getSortDirection() {
        return sortDirection;
    }

    public void setSortDirection(SortDirection sortDirection) {
        this.sortDirection = sortDirection;
    }

    public enum SortDirection {
        ASC,
        DESC
    }
}
