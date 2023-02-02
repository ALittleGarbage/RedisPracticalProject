package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScoreResult {
    private List<?> list;
    private Integer offset;
    private Long minTime;
}
