/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@RunWith(Parameterized.class)
public class FilterPartitionTest extends BaseFilterTest
{
  private static class NoBitmapSelectorFilter extends SelectorFilter
  {
    public NoBitmapSelectorFilter(
        String dimension,
        String value
    )
    {
      super(dimension, value);
    }

    @Override
    public boolean supportsBitmapIndex(BitmapIndexSelector selector)
    {
      return false;
    }
  }

  private static class NoBitmapDimensionPredicateFilter extends DimensionPredicateFilter
  {
    public NoBitmapDimensionPredicateFilter(
        final String dimension,
        final DruidPredicateFactory predicateFactory,
        final ExtractionFn extractionFn
    )
    {
      super(dimension, predicateFactory, extractionFn);
    }

    @Override
    public boolean supportsBitmapIndex(BitmapIndexSelector selector)
    {
      return false;
    }
  }

  private static class NoBitmapSelectorDimFilter extends SelectorDimFilter
  {
    NoBitmapSelectorDimFilter(String dimension, String value, ExtractionFn extractionFn)
    {
      super(dimension, value, extractionFn);
    }

    @Override
    public Filter toFilter()
    {
      ExtractionFn extractionFn = getExtractionFn();
      String dimension = getDimension();
      String value = getValue();
      if (extractionFn == null) {
        return new NoBitmapSelectorFilter(dimension, value);
      } else {
        final String valueOrNull = NullHandling.emptyToNullIfNeeded(value);
        final DruidPredicateFactory predicateFactory = new DruidPredicateFactory()
        {
          @Override
          public Predicate<String> makeStringPredicate()
          {
            return input -> Objects.equals(valueOrNull, input);
          }

          @Override
          public DruidLongPredicate makeLongPredicate()
          {
            return input -> Objects.equals(valueOrNull, String.valueOf(input));
          }

          @Override
          public DruidFloatPredicate makeFloatPredicate()
          {
            return input -> Objects.equals(valueOrNull, String.valueOf(input));
          }

          @Override
          public DruidDoublePredicate makeDoublePredicate()
          {
            return input -> Objects.equals(valueOrNull, String.valueOf(input));
          }

        };

        return new NoBitmapDimensionPredicateFilter(dimension, predicateFactory, extractionFn);
      }
    }
  }

  private static String JS_FN = "function(str) { return 'super-' + str; }";
  private static ExtractionFn JS_EXTRACTION_FN = new JavaScriptExtractionFn(JS_FN, false, JavaScriptConfig.getEnabledInstance());

  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim0", "dim1", "dim2", "dim3")),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parseBatch(ImmutableMap.of("dim0", "0", "dim1", "", "dim2", ImmutableList.of("a", "b"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "1", "dim1", "10", "dim2", ImmutableList.of())).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "2", "dim1", "2", "dim2", ImmutableList.of(""))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "3", "dim1", "1", "dim2", ImmutableList.of("a"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "4", "dim1", "def", "dim2", ImmutableList.of("c"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "5", "dim1", "abc")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "6", "dim1", "B453B411", "dim2", ImmutableList.of("c", "d", "e"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "7", "dim1", "HELLO", "dim2", ImmutableList.of("foo"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "8", "dim1", "abc", "dim2", ImmutableList.of("bar"))).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "9", "dim1", "1", "dim2", ImmutableList.of("foo", "bar"))).get(0)
  );

  public FilterPartitionTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(testName, ROWS, indexBuilder, finisher, cnf, optimize);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(FilterPartitionTest.class.getName());
  }

  @Test
  public void testSinglePreFilterWithNulls()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new SelectorDimFilter("dim1", null, null), ImmutableList.of("0"));
    } else {
      assertFilterMatches(new SelectorDimFilter("dim1", null, null), ImmutableList.of());
    }
    assertFilterMatches(new SelectorDimFilter("dim1", "", null), ImmutableList.of("0"));
    assertFilterMatches(new SelectorDimFilter("dim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new SelectorDimFilter("dim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new SelectorDimFilter("dim1", "1", null), ImmutableList.of("3", "9"));
    assertFilterMatches(new SelectorDimFilter("dim1", "def", null), ImmutableList.of("4"));
    assertFilterMatches(new SelectorDimFilter("dim1", "abc", null), ImmutableList.of("5", "8"));
    assertFilterMatches(new SelectorDimFilter("dim1", "ab", null), ImmutableList.of());
  }

  @Test
  public void testSinglePostFilterWithNulls()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", null, null), ImmutableList.of("0"));
    } else {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", null, null), ImmutableList.of());
    }
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "", null), ImmutableList.of("0"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "10", null), ImmutableList.of("1"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "2", null), ImmutableList.of("2"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "1", null), ImmutableList.of("3", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "def", null), ImmutableList.of("4"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "abc", null), ImmutableList.of("5", "8"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "ab", null), ImmutableList.of());

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN), ImmutableList.of("0"));
    } else {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-", JS_EXTRACTION_FN), ImmutableList.of("0"));
    }
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-10", JS_EXTRACTION_FN), ImmutableList.of("1"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN), ImmutableList.of("2"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-1", JS_EXTRACTION_FN), ImmutableList.of("3", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-def", JS_EXTRACTION_FN), ImmutableList.of("4"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN), ImmutableList.of("5", "8"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim1", "super-ab", JS_EXTRACTION_FN), ImmutableList.of());
  }

  @Test
  public void testBasicPreAndPostFilterWithNulls()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "a", null),
              new NoBitmapSelectorDimFilter("dim1", null, null)
          )),
          ImmutableList.of("0")
      );
    } else {
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "a", null),
              new NoBitmapSelectorDimFilter("dim1", null, null)
          )),
          ImmutableList.of()
      );
    }

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "10", null),
            new NoBitmapSelectorDimFilter("dim2", null, null)
        )),
        ImmutableList.of("1")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "1", null),
            new NoBitmapSelectorDimFilter("dim2", "foo", null)
        )),
        ImmutableList.of("9")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "HELLO", null),
            new NoBitmapSelectorDimFilter("dim2", "bar", null)
        )),
        ImmutableList.of()
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim2", "bar", null),
            new SelectorDimFilter("dim1", "NOT_A_VALUE", null)
        )),
        ImmutableList.of()
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("0")
      );
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("2")
      );
    } else {
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim1", "super-", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("0")
      );
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of()
      );
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("2")
      );
      assertFilterMatches(
          new AndDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of()
      );
    }

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-10", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("1")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-1", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("9")
    );

    assertFilterMatches(
        new AndDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-bar", JS_EXTRACTION_FN)
        )),
        ImmutableList.of()
    );
  }

  @Test
  public void testOrPostFilterWithNulls()
  {
    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim2", "a", null),
            new NoBitmapSelectorDimFilter("dim1", null, null)
        )),
        ImmutableList.of("0", "3")
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "abc", null),
              new NoBitmapSelectorDimFilter("dim2", null, null)
          )),
          ImmutableList.of("1", "2", "5", "8")
      );
    } else {
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "abc", null),
              new NoBitmapSelectorDimFilter("dim2", null, null)
          )),
          ImmutableList.of("1", "5", "8")
      );
    }

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "2", null),
            new NoBitmapSelectorDimFilter("dim2", null, null)
        )),
        ImmutableList.of("1", "2", "5")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "INVALID_VALUE", null),
            new NoBitmapSelectorDimFilter("dim2", "foo", null)
        )),
        ImmutableList.of("7", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "HELLO", null),
            new NoBitmapSelectorDimFilter("dim2", "bar", null)
        )),
        ImmutableList.of("7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "HELLO", null),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", null)
        )),
        ImmutableList.of("7")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "INVALID", null),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", null)
        )),
        ImmutableList.of()
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim2", "super-a", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim1", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "3")
    );

    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("1", "2", "5", "8")
      );
    } else {
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("1", "5", "8")
      );
      assertFilterMatches(
          new OrDimFilter(Arrays.asList(
              new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
              new NoBitmapSelectorDimFilter("dim2", "super-", JS_EXTRACTION_FN)
          )),
          ImmutableList.of("2", "5", "8")
      );
    }

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-2", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("1", "2", "5")
    );
    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "INVALID_VALUE", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("7", "9")
    );
    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new SelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new NoBitmapSelectorDimFilter("dim2", "super-bar", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", null)
        )),
        ImmutableList.of("7")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "INVALID", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "NOT_A_VALUE", JS_EXTRACTION_FN)
        )),
        ImmutableList.of()
    );
  }

  @Test
  public void testMissingColumnSpecifiedInDimensionList()
  {
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", null, null), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new NoBitmapSelectorDimFilter("dim3", "", null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    } else {
      assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "", null), ImmutableList.of());
    }
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "a", null), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "b", null), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "c", null), ImmutableList.of());

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", null),
            new SelectorDimFilter("dim3", "NOTHERE", null)
        )),
        ImmutableList.of("5", "8")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", null),
            new SelectorDimFilter("dim3", null, null)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "a", JS_EXTRACTION_FN), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "b", JS_EXTRACTION_FN), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim3", "c", JS_EXTRACTION_FN), ImmutableList.of());

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim3", "NOTHERE", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("5", "8")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim3", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );
  }

  @Test
  public void testMissingColumnNotSpecifiedInDimensionList()
  {
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", null, null), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(
          new NoBitmapSelectorDimFilter("dim4", "", null),
          ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
      );
    } else {
      assertFilterMatches(
          new NoBitmapSelectorDimFilter("dim4", "", null),
          ImmutableList.of()
      );
    }
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "a", null), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "b", null), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "c", null), ImmutableList.of());

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "abc", null),
            new SelectorDimFilter("dim4", null, null)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim4", null, null),
            new SelectorDimFilter("dim1", "abc", null)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN),
                        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "a", JS_EXTRACTION_FN), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "b", JS_EXTRACTION_FN), ImmutableList.of());
    assertFilterMatches(new NoBitmapSelectorDimFilter("dim4", "c", JS_EXTRACTION_FN), ImmutableList.of());

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );

    assertFilterMatches(
        new OrDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim4", "super-null", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim1", "super-abc", JS_EXTRACTION_FN)
        )),
        ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    );
  }

  @Test
  public void testDistributeOrCNF()
  {
    DimFilter dimFilter1 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dim0", "6", null),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "def", null),
            new SelectorDimFilter("dim2", "c", null)
        )
        ))
    );

    Filter filter1 = dimFilter1.toFilter();
    Filter filter1CNF = Filters.convertToCNF(filter1);

    Assert.assertEquals(AndFilter.class, filter1CNF.getClass());
    Assert.assertEquals(2, ((AndFilter) filter1CNF).getFilters().size());

    assertFilterMatches(
        dimFilter1,
        ImmutableList.of("4", "6")
    );

    DimFilter dimFilter2 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dim0", "2", null),
        new SelectorDimFilter("dim0", "3", null),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "HELLO", null),
            new SelectorDimFilter("dim2", "foo", null)
        )
        ))
    );

    assertFilterMatches(
        dimFilter2,
        ImmutableList.of("2", "3", "7")
    );

    DimFilter dimFilter3 = new OrDimFilter(Arrays.asList(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "1", null),
            new SelectorDimFilter("dim2", "foo", null)
        )
        ))
    );

    assertFilterMatches(
        dimFilter3,
        ImmutableList.of("2", "3", "4", "6", "7", "9")
    );
  }

  @Test
  public void testDistributeOrCNFExtractionFn()
  {
    DimFilter dimFilter1 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dim0", "super-6", JS_EXTRACTION_FN),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-def", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "super-c", JS_EXTRACTION_FN)
        )
        ))
    );

    Filter filter1 = dimFilter1.toFilter();
    Filter filter1CNF = Filters.convertToCNF(filter1);

    Assert.assertEquals(AndFilter.class, filter1CNF.getClass());
    Assert.assertEquals(2, ((AndFilter) filter1CNF).getFilters().size());

    assertFilterMatches(
        dimFilter1,
        ImmutableList.of("4", "6")
    );

    DimFilter dimFilter2 = new OrDimFilter(Arrays.asList(
        new SelectorDimFilter("dim0", "super-2", JS_EXTRACTION_FN),
        new SelectorDimFilter("dim0", "super-3", JS_EXTRACTION_FN),
        new AndDimFilter(Arrays.asList(
            new NoBitmapSelectorDimFilter("dim1", "super-HELLO", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )
        ))
    );

    assertFilterMatches(
        dimFilter2,
        ImmutableList.of("2", "3", "7")
    );

    DimFilter dimFilter3 = new OrDimFilter(
        dimFilter1,
        dimFilter2,
        new AndDimFilter(
            new NoBitmapSelectorDimFilter("dim1", "super-1", JS_EXTRACTION_FN),
            new SelectorDimFilter("dim2", "super-foo", JS_EXTRACTION_FN)
        )
    );

    assertFilterMatches(
        dimFilter3,
        ImmutableList.of("2", "3", "4", "6", "7", "9")
    );
  }
}
