package com.uber.hudi.tools.utils;

import com.beust.jcommander.converters.IParameterSplitter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CleanCommaSplitter implements IParameterSplitter {
  @Override
  public List<String> split(String value) {
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
} 