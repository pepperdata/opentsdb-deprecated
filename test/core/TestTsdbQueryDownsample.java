// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import net.opentsdb.utils.DateTime;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;

import com.stumbleupon.async.Deferred;
import net.opentsdb.uid.NoSuchUniqueName;
import org.junit.Before;

import net.opentsdb.storage.MockBase;

import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import org.apache.zookeeper.proto.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Tests downsampling with query.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({TSDB.class, Config.class, UniqueId.class, HBaseClient.class,
  CompactionQueue.class, GetRequest.class, PutRequest.class, KeyValue.class,
  Scanner.class, TsdbQuery.class, DeleteRequest.class, Annotation.class,
  RowKey.class, Span.class, SpanGroup.class, IncomingDataPoints.class })
public class TestTsdbQueryDownsample {

  private TSDB tsdb = null;
  private HBaseClient client = mock(HBaseClient.class);
  private UniqueId metrics = mock(UniqueId.class);
  private UniqueId tag_names = mock(UniqueId.class);
  private UniqueId tag_values = mock(UniqueId.class);
  private TsdbQuery query = null;
  private MockBase storage = null;

  @Before
  public void before() throws Exception {
    tsdb = TSDB.newTsdbForTest(client);
    query = new TsdbQuery(tsdb);

    // replace the "real" field objects with mocks
    Field met = tsdb.getClass().getDeclaredField("metrics");
    met.setAccessible(true);
    met.set(tsdb, metrics);

    Field tagk = tsdb.getClass().getDeclaredField("tag_names");
    tagk.setAccessible(true);
    tagk.set(tsdb, tag_names);

    Field tagv = tsdb.getClass().getDeclaredField("tag_values");
    tagv.setAccessible(true);
    tagv.set(tsdb, tag_values);

    // mock UniqueId
    when(metrics.getId("sys.cpu.user")).thenReturn(new byte[] { 0, 0, 1 });
    when(metrics.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
    when(metrics.getId("sys.cpu.system"))
      .thenThrow(new NoSuchUniqueName("sys.cpu.system", "metric"));
    when(metrics.getId("sys.cpu.nice")).thenReturn(new byte[] { 0, 0, 2 });
    when(metrics.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("sys.cpu.nice"));
    when(tag_names.getId("host")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_names.getIdAsync("host")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("host"));
    when(tag_names.getOrCreateIdAsync("host")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_names.getIdAsync("dc"))
      .thenThrow(new NoSuchUniqueName("dc", "metric"));
    when(tag_values.getId("web01")).thenReturn(new byte[] { 0, 0, 1 });
    when(tag_values.getIdAsync("web01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 1 }))
      .thenReturn(Deferred.fromResult("web01"));
    when(tag_values.getOrCreateIdAsync("web01")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 1 }));
    when(tag_values.getId("web02")).thenReturn(new byte[] { 0, 0, 2 });
    when(tag_values.getIdAsync("web02")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_values.getNameAsync(new byte[] { 0, 0, 2 }))
      .thenReturn(Deferred.fromResult("web02"));
    when(tag_values.getOrCreateIdAsync("web02")).thenReturn(
        Deferred.fromResult(new byte[] { 0, 0, 2 }));
    when(tag_values.getId("web03"))
      .thenThrow(new NoSuchUniqueName("web03", "metric"));

    when(metrics.width()).thenReturn((short)3);
    when(tag_names.width()).thenReturn((short)3);
    when(tag_values.width()).thenReturn((short)3);
  }

  @Test
  public void downsample() throws Exception {
    int downsampleInterval = (int)DateTime.parseDuration("60s");
    query.downsample(downsampleInterval, Aggregators.SUM);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    assertEquals(60000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - downsampleInterval / 1000 -
                         Const.MAX_TIMESPAN_SECS;
    assertEquals(scanStartTime,
                 TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long hbaseScanStartTime = scanStartTime - Const.MAX_TIMESPAN_SECS + 1;
    assertEquals(hbaseScanStartTime,
                 TsdbQuery.ForTesting.getHBaseScanStartTimeSeconds(query));
    long scanEndTime = 1357041600 + downsampleInterval / 1000 +
                       Const.MAX_TIMESPAN_SECS;
    assertEquals(scanEndTime,
                 TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test
  public void downsampleMilliseconds() throws Exception {
    int downsampleInterval = (int)DateTime.parseDuration("60s");
    query.downsample(downsampleInterval, Aggregators.SUM);
    query.setStartTime(2356998400000L);
    query.setEndTime(2357041600000L);
    assertEquals(60000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 2356998400L - downsampleInterval / 1000 -
                         Const.MAX_TIMESPAN_SECS;
    assertEquals(scanStartTime,
                 TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long hbaseScanStartTime = scanStartTime - Const.MAX_TIMESPAN_SECS + 1;
    assertEquals(hbaseScanStartTime,
                 TsdbQuery.ForTesting.getHBaseScanStartTimeSeconds(query));
    long scanEndTime = 2357041600L + downsampleInterval / 1000 +
                       Const.MAX_TIMESPAN_SECS;
    assertEquals(scanEndTime,
                 TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test
  public void testScanTime_bigDownsampleTime() throws Exception {
    query.setInterpolationWindow(DateTime.parseDuration("110s"));
    int downsampleInterval = (int)DateTime.parseDuration("200s");
    query.downsample(downsampleInterval, Aggregators.SUM);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    assertEquals(200000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - downsampleInterval / 1000 -
                         DateTime.parseDuration("110s") / 1000;
    assertEquals(scanStartTime,
        TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long hbaseScanStartTime = scanStartTime - Const.MAX_TIMESPAN_SECS + 1;
    assertEquals(hbaseScanStartTime,
                 TsdbQuery.ForTesting.getHBaseScanStartTimeSeconds(query));
    long scanEndTime = 1357041600 + downsampleInterval / 1000 +
                       DateTime.parseDuration("110s") / 1000;
    assertEquals(scanEndTime,
                 TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test
  public void testScanTime_customerOverride() throws Exception {
    query.setInterpolationWindow(DateTime.parseDuration("110s"));
    query.setHbaseTimeStartExtensionMillis(DateTime.parseDuration("17s"));
    query.setHbaseTimeEndExtensionMillis(DateTime.parseDuration("37s"));
    int downsampleInterval = (int)DateTime.parseDuration("200s");
    query.downsample(downsampleInterval, Aggregators.SUM);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    assertEquals(200000, TsdbQuery.ForTesting.getDownsampleIntervalMs(query));
    long scanStartTime = 1356998400 - 17;
    assertEquals(scanStartTime,
                 TsdbQuery.ForTesting.getScanStartTimeSeconds(query));
    long hbaseScanStartTime = scanStartTime - Const.MAX_TIMESPAN_SECS + 1;
    assertEquals(hbaseScanStartTime,
                 TsdbQuery.ForTesting.getHBaseScanStartTimeSeconds(query));
    long scanEndTime = 1357041600 + 37;
    assertEquals(scanEndTime,
                 TsdbQuery.ForTesting.getScanEndTimeSeconds(query));
  }

  @Test (expected = NullPointerException.class)
  public void downsampleNullAgg() throws Exception {
    query.downsample(60, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void downsampleInvalidInterval() throws Exception {
    query.downsample(0, Aggregators.SUM);
  }

  @Test
  public void runLongSingleTSDownsample() throws Exception {
    storeLongTimeSeriesSeconds(true, false);;
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in intervals: (1), (2, 3), (4, 5), ... (298, 299), (300)
    int i = 0;
    for (DataPoint dp : dps[0]) {
      // Downsampler outputs just doubles.
      assertFalse(dp.isInteger());
      if (i == 0) {
        // The first time interval has just one value - (1).
        assertEquals(1, dp.doubleValue(), 0.00001);
      } else if (i >= 150) {
        // The last time interval has just one value - (300).
        assertEquals(300, dp.doubleValue(), 0.00001);
      } else {
        // Each interval has two consecutive numbers - (i*2, i*2+1).
        // Takes the average of the values of this interval.
        double value = i * 2 + 0.5;
        assertEquals(value, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 60000);
      ++i;
    }
    // Out of 300 values, the first and the last intervals have one value each,
    // and the 149 intervals in the middle have two values for each.
    assertEquals(151, dps[0].size());
  }

  @Test
  public void runLongSingleTSDownsampleMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    verify(client).newScanner(tsdb.table);
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in intervals: (1), (2, 3), (4, 5), ... (298, 299), (300)
    int i = 0;
    for (DataPoint dp : dps[0]) {
      // Downsampler outputs just doubles.
      assertFalse(dp.isInteger());
      if (i == 0) {
        // The first time interval has just one value - (1).
        assertEquals(1, dp.doubleValue(), 0.00001);
      } else if (i >= 150) {
        // The last time interval has just one value - (300).
        assertEquals(300, dp.doubleValue(), 0.00001);
      } else {
        // Each interval has two consecutive numbers - (i*2, i*2+1).
        // Takes the average of the values of this interval.
        double value = i * 2 + 0.5;
        assertEquals(value, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 1000);
      ++i;
    }
    // Out of 300 values, the first and the last intervals have one value each,
    // and the 149 intervals in the middle have two values for each.
    assertEquals(151, dps[0].size());
  }

  /**
   * This test is storing > Short.MAX_VALUE data points in a single row and
   * making sure the state and iterators function properly. 1.x used a short as
   * we would only have a max of 3600 data points but now we can have over 4M
   * so we have to index with an int and store the state in a long.
   */
  @Test
  public void runLongSingleTSDownsampleMsLarge() throws Exception {
    setQueryStorage();
    long ts = 1356998400500L;
    // mimicks having 64K data points in a row
    final int limit = 64000;
    final byte[] qualifier = new byte[4 * limit];
    for (int i = 0; i < limit; i++) {
      System.arraycopy(Internal.buildQualifier(ts, (short) 0), 0,
          qualifier, i * 4, 4);
      ts += 50;
    }
    final byte[] values = new byte[limit + 2];
    storage.addColumn(MockBase.stringToBytes("00000150E22700000001000001"),
        qualifier, values);

    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    for (DataPoint dp : dps[0]) {
      // NOTE: Downsampler supports just double values.
      // TODO: Keep the original type - long or double.
      assertEquals(0, dp.doubleValue(), 0);
    }
    // The first timestamp in second is 1356998400.500L, and the last one is
    // 1357001600.450. Downsampler generates the timestamps in [1356998400,
    // 1357001600], so there should be 3201 values.
    assertEquals(3201, dps[0].size());
  }

  @Test
  public void runLongSingleTSDownsampleAndRate() throws Exception {
    storeLongTimeSeriesSecondsWithBasetime(1356998403L, true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in 30-second intervals: (1356998433s, 1), (1356998463s, 2)
    //   (1356998493s, 3), (1356998523s, 4), 5, ... 298, 299, 300
    // After aggregation as rate: 1/30, (1/30, 1/30), ... (1/30, 1/30)
    // After avg-downsampling: 1/30, 1/30, 1/30, ... 1/30
    long expectedTimestamp = 1356998460000L;
    for (DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      assertEquals(0.033F, dp.doubleValue(), 0.001);
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 60000);
      assertEquals(expectedTimestamp, dp.timestamp());
      expectedTimestamp += 60000;
    }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runLongSingleTSDownsampleAndRateMs() throws Exception {
    storeLongTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in intervals: (1), (2, 3), (4, 5), ... (298, 299), (300)
    // After downsampling: 1, 2.5, 4.5, ... 298.5, 300
    int i = 0;
    for (DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      if (i == 0) {
        // The value of the first interval is one and the next one is 2.5
        // 1.5 = (2.5 - 1) / 1.000 seconds.
        assertEquals(1.5F, dp.doubleValue(), 0.001);
      } else if (i >= 149) {
        // The value of the last interval is 300 and the previous one is 298.5
        // 1.5 = (300 - 298.5) / 1.000 seconds.
        assertEquals(1.5, dp.doubleValue(), 0.00001);
      } else {
        // 2 = 2 / 1.000 seconds where 2 is the difference of the values
        // of two consecutive intervals.
        assertEquals(2F, dp.doubleValue(), 0.001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 1000);
      ++i;
    }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsample() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in intervals: (1.25), (1.5, 1.75), (2, 2.25), ...
    // (75.5, 75.75), (76).
    // After downsampling: 1.25, 1.625, 2.125, ... 75.625, 76
    int i = 0;
    for (DataPoint dp : dps[0]) {
      if (i == 0) {
        // The first time interval has just one value - 1.25.
        assertEquals(1.25, dp.doubleValue(), 0.00001);
      } else if (i >= 150) {
        // The last time interval has just one value - 76.
        assertEquals(76D, dp.doubleValue(), 0.00001);
      } else {
        // Each interval has two consecutive numbers - (i*0.5+1, i*0.5+1.25).
        // Takes the average of the values of this interval.
        double value = (i + 2.25) / 2;
        assertEquals(value, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 60000);
      ++i;
    }
    // Out of 300 values, the first and the last intervals have one value each,
    // and the 149 intervals in the middle have two values for each.
    assertEquals(151, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsampleMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in intervals: (1.25), (1.5, 1.75), (2, 2.25), ...
    // (75.5, 75.75), (76).
    // After downsampling: 1.25, 1.625, 2.125, ... 75.625, 76
    int i = 0;
    for (DataPoint dp : dps[0]) {
      if (i == 0) {
        // The first time interval has just one value.
        assertEquals(1.25, dp.doubleValue(), 0.00001);
      } else if (i >= 150) {
        // The last time interval has just one value.
        assertEquals(76D, dp.doubleValue(), 0.00001);
      } else {
        // Each interval has two consecutive numbers - (i*0.5+1, i*0.5+1.25).
        // Takes the average of the values of this interval.
        double value = (i + 2.25) / 2;
        assertEquals(value, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 1000);
      ++i;
    }
    // Out of 300 values, the first and the last intervals have one value each,
    // and the 149 intervals in the middle have two values for each.
    assertEquals(151, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsampleAndRate() throws Exception {
    storeFloatTimeSeriesSeconds(true, false);
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(60000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in 30-second intervals: (1356998430s, 1.25),
    // (1356998460s, 1.5), (1356998490s, 1.75), ... 75.5, 75.75, 66
    // After aggregation as rate: 0.25/30, (0.25/30, 0.25/30), ...
    // After avg-downsampling: 0.25/30, 0.25/30, 0.25/30, ... 0.25/30
    long expectedTimestamp = 1356998460000L;
    for (DataPoint dp : dps[0]) {
      assertFalse(dp.isInteger());
      assertEquals(0.00833F, dp.doubleValue(), 0.00001);
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 60000);
      assertEquals(expectedTimestamp, dp.timestamp());
      expectedTimestamp += 60000;
    }
    assertEquals(150, dps[0].size());
  }

  @Test
  public void runFloatSingleTSDownsampleAndRateMs() throws Exception {
    storeFloatTimeSeriesMs();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.downsample(1000, Aggregators.AVG);
    query.setTimeSeries("sys.cpu.user", tags, Aggregators.SUM, true);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals("sys.cpu.user", dps[0].metricName());
    assertTrue(dps[0].getAggregatedTags().isEmpty());
    assertNull(dps[0].getAnnotations());
    assertEquals("web01", dps[0].getTags().get("host"));

    // Timeseries in intervals: (1.25), (1.5, 1.75), (2, 2.25), ...
    // (75.5, 75.75), (76).
    // After downsampling: 1.25, 1.625, 2.125, ... 75.625, 76
    int i = 0;
    for (DataPoint dp : dps[0]) {
      if (i == 0) {
        // The value of the first interval is 1.25 and the next one is 1.625
        // 0.375 = (1.625 - 1.25) / 1.000 seconds.
        assertEquals(0.375F, dp.doubleValue(), 0.000001);
      } else if (i >= 149) {
        // The value of the last interval is 76 and the previous one is 75.625
        // 0.375 = (76 - 75.625) / 1.000 seconds.
        assertEquals(0.375F, dp.doubleValue(), 0.000001);
      } else {
        // 0.5 = 0.5 / 1.000 seconds where 0.5 is the difference of the values
        // of two consecutive intervals.
        assertEquals(0.5F, dp.doubleValue(), 0.00001);
      }
      // Timestamp of an interval should be aligned by the interval.
      assertEquals(0, dp.timestamp() % 1000);
      ++i;
    }
    assertEquals(150, dps[0].size());
  }

  // ----------------- //
  // Helper functions. //
  // ----------------- //

  private void storeLongTimeSeriesSeconds(final boolean two_metrics,
      final boolean offset) throws Exception {
    storeLongTimeSeriesSecondsWithBasetime(1356998400L, two_metrics, offset);
  }

  private void storeLongTimeSeriesSecondsWithBasetime(final long baseTimestamp,
      final boolean two_metrics, final boolean offset) throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = baseTimestamp;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
      }
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = baseTimestamp + (offset ? 15 : 0);
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
      }
    }
  }

  private void storeLongTimeSeriesMs() throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (int i = 1; i <= 300; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400000L;
    for (int i = 300; i > 0; i--) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }
  }

  private void storeFloatTimeSeriesSeconds(final boolean two_metrics,
      final boolean offset) throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
      }
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = offset ? 1356998415 : 1356998400;
    for (float i = 75F; i > 0; i -= 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 30, i, tags).joinUninterruptibly();
      if (two_metrics) {
        tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
      }
    }
  }

  private void storeFloatTimeSeriesMs() throws Exception {
    setQueryStorage();
    // dump a bunch of rows of two metrics so that we can test filtering out
    // on the metric
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400000L;
    for (float i = 1.25F; i <= 76; i += 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }

    // dump a parallel set but invert the values
    tags.clear();
    tags.put("host", "web02");
    timestamp = 1356998400000L;
    for (float i = 75F; i > 0; i -= 0.25F) {
      tsdb.addPoint("sys.cpu.user", timestamp += 500, i, tags).joinUninterruptibly();
      tsdb.addPoint("sys.cpu.nice", timestamp, i, tags).joinUninterruptibly();
    }
  }

  @SuppressWarnings("unchecked")
  private void setQueryStorage() throws Exception {
    storage = new MockBase(tsdb, client, true, true, true, true);
    storage.setFamily("t".getBytes(MockBase.ASCII()));

    PowerMockito.mockStatic(IncomingDataPoints.class);
    PowerMockito.doAnswer(
        new Answer<byte[]>() {
          public byte[] answer(final InvocationOnMock args)
            throws Exception {
            final String metric = (String)args.getArguments()[1];
            final Map<String, String> tags =
              (Map<String, String>)args.getArguments()[2];

            if (metric.equals("sys.cpu.user")) {
              if (tags.get("host").equals("web01")) {
                return new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
              } else {
                return new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2};
              }
            } else {
              if (tags.get("host").equals("web01")) {
                return new byte[] { 0, 0, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
              } else {
                return new byte[] { 0, 0, 2, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2};
              }
            }
          }
        }
    ).when(IncomingDataPoints.class, "rowKeyTemplate", (TSDB)any(), anyString(),
        (Map<String, String>)any());
  }
}
