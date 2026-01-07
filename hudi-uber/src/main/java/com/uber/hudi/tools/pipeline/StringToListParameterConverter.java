package com.uber.hudi.tools.pipeline;

import com.beust.jcommander.IStringConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class converts a string parameter to List parameter. For splitting it uses ';' character.
 */
public class StringToListParameterConverter  implements IStringConverter<List<String>> {
  @Override
  public List<String> convert(String value) {
    if (value == null || value.length() == 0) {
      return Collections.<String>emptyList();
    }
    return Arrays.stream(value.split(";")).collect(Collectors.toList());
  }
}
