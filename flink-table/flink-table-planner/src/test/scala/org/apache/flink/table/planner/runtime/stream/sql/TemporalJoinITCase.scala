/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.core.testutils.EachCallbackWrapper
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.{getResultsAsStrings, registerData}
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.utils.LegacyRowExtension
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.{ExtendWith, RegisterExtension}

import java.time.LocalDateTime
import java.time.format.DateTimeParseException

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class TemporalJoinITCase(state: StateBackendMode) extends StreamingWithStateTestBase(state) {

  @RegisterExtension private val _: EachCallbackWrapper[LegacyRowExtension] =
    new EachCallbackWrapper[LegacyRowExtension](new LegacyRowExtension)

  // test data for Processing-Time temporal table join
  val procTimeOrderData = List(
    changelogRow("+I", 1L, "Euro", "no1", 12L),
    changelogRow("+I", 2L, "US Dollar", "no1", 14L),
    changelogRow("+I", 3L, "US Dollar", "no2", 18L),
    changelogRow("+I", 4L, "RMB", "no1", 40L)
  )

  val procTimeCurrencyData = List(
    changelogRow("+I", "Euro", "no1", 114L),
    changelogRow("+I", "US Dollar", "no1", 102L),
    changelogRow("+I", "Yen", "no1", 1L),
    changelogRow("+I", "RMB", "no1", 702L),
    changelogRow("+I", "Euro", "no1", 118L),
    changelogRow("+I", "US Dollar", "no2", 106L)
  )

  val procTimeCurrencyChangelogData = List(
    changelogRow("+I", "Euro", "no1", 114L),
    changelogRow("+I", "US Dollar", "no1", 102L),
    changelogRow("+I", "Yen", "no1", 1L),
    changelogRow("+I", "RMB", "no1", 702L),
    changelogRow("-U", "RMB", "no1", 702L),
    changelogRow("+U", "RMB", "no1", 802L),
    changelogRow("+I", "Euro", "no1", 118L),
    changelogRow("+I", "US Dollar", "no2", 106L)
  )

  // test data for Event-Time temporal table join
  val rowTimeOrderData = List(
    changelogRow("+I", 1L, "Euro", "no1", 12L, "2020-08-15T00:01:00"),
    changelogRow("+I", 2L, "US Dollar", "no1", 1L, "2020-08-15T00:02:00"),
    changelogRow("+I", 3L, "RMB", "no1", 40L, "2020-08-15T00:03:00"),
    changelogRow("+I", 4L, "Euro", "no1", 14L, "2020-08-16T00:04:00"),
    changelogRow("-U", 2L, "US Dollar", "no1", 1L, "2020-08-16T00:03:00"),
    changelogRow("+U", 2L, "US Dollar", "no1", 18L, "2020-08-16T00:03:00"),
    changelogRow("+I", 5L, "RMB", "no1", 40L, "2020-08-16T00:03:00"),
    changelogRow("+I", 6L, "RMB", "no1", 40L, "2020-08-16T00:04:00"),
    changelogRow("-D", 6L, "RMB", "no1", 40L, "2020-08-16T00:04:00")
  )

  val rowTimeCurrencyDataUsingMetaTime = List(
    changelogRow("+I", "Euro", "no1", 114L, "2020-08-15T00:00:01"),
    changelogRow("+I", "US Dollar", "no1", 102L, "2020-08-15T00:00:02"),
    changelogRow("+I", "Yen", "no1", 1L, "2020-08-15T00:00:03"),
    changelogRow("+I", "RMB", "no1", 702L, "2020-08-15T00:00:04"),
    changelogRow("-U", "Euro", "no1", 114L, "2020-08-16T00:01:00"),
    changelogRow("+U", "Euro", "no1", 118L, "2020-08-16T00:01:00"),
    changelogRow("-U", "US Dollar", "no1", 102L, "2020-08-16T00:02:00"),
    changelogRow("+U", "US Dollar", "no1", 106L, "2020-08-16T00:02:00"),
    changelogRow("-D", "RMB", "no1", 702L, "2020-08-16T00:02:00")
  )

  val rowTimeCurrencyDataUsingBeforeTime = List(
    changelogRow("+I", "Euro", "no1", 114L, "2020-08-15T00:00:01"),
    changelogRow("+I", "US Dollar", "no1", 102L, "2020-08-15T00:00:02"),
    changelogRow("+I", "Yen", "no1", 1L, "2020-08-15T00:00:03"),
    changelogRow("+I", "RMB", "no1", 702L, "2020-08-15T00:00:04"),
    changelogRow("-U", "Euro", "no1", 114L, "2020-08-15T00:00:01"),
    changelogRow("+U", "Euro", "no1", 118L, "2020-08-16T00:01:00"),
    changelogRow("-U", "US Dollar", "no1", 102L, "2020-08-15T00:00:02"),
    changelogRow("+U", "US Dollar", "no1", 106L, "2020-08-16T00:02:00"),
    changelogRow("-D", "RMB", "no1", 702L, "2020-08-15T00:00:04")
  )

  val upsertSourceCurrencyData = List(
    changelogRow("+U", "Euro", "no1", 114L, "2020-08-15T00:00:01"),
    changelogRow("+U", "US Dollar", "no1", 102L, "2020-08-15T00:00:02"),
    changelogRow("+U", "Yen", "no1", 1L, "2020-08-15T00:00:03"),
    changelogRow("+U", "RMB", "no1", 702L, "2020-08-15T00:00:04"),
    changelogRow("+U", "Euro", "no1", 118L, "2020-08-16T00:01:00"),
    changelogRow("+U", "US Dollar", "no1", 104L, "2020-08-16T00:02:00"),
    changelogRow("-D", "RMB", "no1", 702L, "2020-08-15T00:00:04")
  )

  val rowTimeInsertOnlyCurrencyData = List(
    changelogRow("+I", "Euro", "no1", 114L, "2020-08-15T00:00:01"),
    changelogRow("+I", "US Dollar", "no1", 102L, "2020-08-15T00:00:02"),
    changelogRow("+I", "Yen", "no1", 1L, "2020-08-15T00:00:03"),
    changelogRow("+I", "RMB", "no1", 702L, "2020-08-15T00:00:04"),
    changelogRow("+I", "Euro", "no1", 118L, "2020-08-16T00:01:00"),
    changelogRow("+I", "US Dollar", "no1", 102L, "2020-08-16T00:02:00"),
    changelogRow("+I", "US Dollar", "no1", 106L, "2020-08-16T00:02:00")
  )

  @BeforeEach
  def prepare(): Unit = {
    val procTimeOrderDataId = registerData(procTimeOrderData)
    tEnv.executeSql(s"""
                       |CREATE TABLE orders_proctime (
                       |  order_id BIGINT,
                       |  currency STRING,
                       |  currency_no STRING,
                       |  amount BIGINT,
                       |  proctime as PROCTIME()
                       |) WITH (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'data-id' = '$procTimeOrderDataId'
                       |)
                       |""".stripMargin)

    // register a non-lookup table
    val procTimeCurrencyDataId = registerData(procTimeCurrencyData)
    tEnv.executeSql(s"""
                       |CREATE TABLE currency_proctime (
                       |  currency STRING,
                       |  currency_no STRING,
                       |  rate BIGINT,
                       |  proctime as PROCTIME(),
                       |  PRIMARY KEY(currency, currency_no) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'disable-lookup' = 'true',
                       |  'data-id' = '$procTimeCurrencyDataId'
                       |)
                       |""".stripMargin)

    val procTimeCurrencyChangelogDataId = registerData(procTimeCurrencyChangelogData)
    tEnv.executeSql(s"""
                       |CREATE TABLE changelog_currency_proctime (
                       |  currency STRING,
                       |  currency_no STRING,
                       |  rate BIGINT,
                       |  proctime as PROCTIME(),
                       |  PRIMARY KEY(currency, currency_no) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'bounded' = 'false',
                       |  'disable-lookup' = 'true',
                       |  'changelog-mode' = 'I,UA,UB,D',
                       |  'data-id' = '$procTimeCurrencyChangelogDataId'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""
                       |CREATE VIEW latest_rates AS
                       |SELECT
                       |  currency,
                       |  currency_no,
                       |  rate,
                       |  proctime FROM
                       |      ( SELECT *, ROW_NUMBER() OVER (PARTITION BY currency, currency_no
                       |        ORDER BY proctime DESC) AS rowNum
                       |        FROM currency_proctime) T
                       | WHERE rowNum = 1""".stripMargin)

    createSinkTable(
      "proctime_default_sink",
      Some(s"""
              |  order_id BIGINT,
              |  currency STRING,
              |  amount BIGINT,
              |  l_time TIMESTAMP_LTZ(3),
              |  rate BIGINT,
              |  r_time TIMESTAMP_LTZ(3),
              |  PRIMARY KEY(order_id) NOT ENFORCED
              |""".stripMargin)
    )

    val rowTimeOrderDataId = registerData(rowTimeOrderData)
    tEnv.executeSql(s"""
                       |CREATE TABLE orders_rowtime (
                       |  order_id BIGINT,
                       |  currency STRING,
                       |  currency_no STRING,
                       |  amount BIGINT,
                       |  order_time TIMESTAMP(3),
                       |  WATERMARK FOR order_time AS order_time,
                       |  PRIMARY KEY (order_id) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'changelog-mode' = 'I,UA,UB,D',
                       |  'data-id' = '$rowTimeOrderDataId'
                       |)
                       |""".stripMargin)

    val rowTimeCurrencyDataId = registerData(rowTimeCurrencyDataUsingMetaTime)
    tEnv.executeSql(s"""
                       |CREATE TABLE versioned_currency_with_single_key (
                       |  currency STRING,
                       |  currency_no STRING,
                       |  rate  BIGINT,
                       |  currency_time TIMESTAMP(3),
                       |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
                       |  PRIMARY KEY(currency) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'changelog-mode' = 'I,UA,UB,D',
                       |  'data-id' = '$rowTimeCurrencyDataId'
                       |)
                       |""".stripMargin)

    tEnv.executeSql(s"""
                       |CREATE TABLE versioned_currency_with_multi_key (
                       |  currency STRING,
                       |  currency_no STRING,
                       |  rate  BIGINT,
                       |  currency_time TIMESTAMP(3),
                       |  WATERMARK FOR currency_time AS currency_time - interval '10' SECOND,
                       |  PRIMARY KEY(currency, currency_no) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'changelog-mode' = 'I,UA,UB,D',
                       |  'data-id' = '$rowTimeCurrencyDataId'
                       |)
                       |""".stripMargin)

    val currencyDataUsingBeforeTimeId = registerData(rowTimeCurrencyDataUsingBeforeTime)

    // set watermark to 2 days which means the late event would be late at most 2 days,
    // the late event will be processed well in tests that uses before time as changelog time
    tEnv.executeSql(s"""
                       |CREATE TABLE currency_using_update_before_time (
                       |  currency STRING,
                       |  currency_no STRING,
                       |  rate  BIGINT,
                       |  currency_time TIMESTAMP(3),
                       |  WATERMARK FOR currency_time AS currency_time - interval '2' DAY,
                       |  PRIMARY KEY(currency) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'changelog-mode' = 'I,UA,UB,D',
                       |  'data-id' = '$currencyDataUsingBeforeTimeId'
                       |)
                       |""".stripMargin)

    val upsertSourceDataId = registerData(upsertSourceCurrencyData)
    tEnv.executeSql(s"""
                       |CREATE TABLE upsert_currency (
                       |  currency STRING,
                       |  currency_no STRING,
                       |  rate  BIGINT,
                       |  currency_time TIMESTAMP(3),
                       |  WATERMARK FOR currency_time AS currency_time - interval '2' DAY,
                       |  PRIMARY KEY(currency) NOT ENFORCED
                       |) WITH (
                       |  'connector' = 'values',
                       |  'changelog-mode' = 'UA,D',
                       |  'data-id' = '$upsertSourceDataId'
                       |)
                       |""".stripMargin)

    createSinkTable("rowtime_default_sink", None)

    val rowTimeInsertOnlyCurrencyDataId = registerData(rowTimeInsertOnlyCurrencyData)
    // insert-only table
    tEnv.executeSql(s"""
                       |CREATE TABLE currency_history (
                       |  currency STRING,
                       |  currency_no STRING,
                       |  rate  BIGINT,
                       |  currency_time TIMESTAMP(3),
                       |  WATERMARK FOR currency_time AS currency_time - interval '0.001' SECOND
                       |) WITH (
                       |  'connector' = 'values',
                       |  'data-id' = '$rowTimeInsertOnlyCurrencyDataId',
                       |  'changelog-mode' = 'I')
                       |  """.stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE VIEW currency_deduplicated_first_row AS
         |SELECT
         |  currency,
         |  currency_no,
         |  rate,
         |  currency_time FROM
         |      (SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY currency_time)
         |       AS rowNum FROM currency_history) T
         | WHERE rowNum = 1""".stripMargin)

    tEnv.executeSql(
      s"""
         |CREATE VIEW currency_deduplicated_last_row AS
         |SELECT
         |  currency,
         |  currency_no,
         |  rate,
         |  currency_time FROM
         |      (SELECT *, ROW_NUMBER() OVER (PARTITION BY currency ORDER BY currency_time DESC)
         |       AS rowNum FROM currency_history) T
         | WHERE rowNum = 1""".stripMargin)
  }

  /**
   * Because of nature of the processing time, we can not (or at least it is not that easy) validate
   * the result here. Instead of that, here we are just testing whether there are no exceptions in a
   * full blown ITCase. Actual correctness is tested in unit tests.
   */
  @TestTemplate
  def testProcTimeTemporalJoin(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    assertThatThrownBy(() => tEnv.executeSql(sql).await())
      .hasMessage("Processing-time temporal join is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testProcTimeLeftTemporalJoin(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " LEFT JOIN currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    assertThatThrownBy(() => tEnv.executeSql(sql).await())
      .hasMessage("Processing-time temporal join is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testProcTimeTemporalJoinChangelogSource(): Unit = {
    createSinkTable(
      "proctime_sink1",
      Some(s"""
              | currency STRING,
              | currency_no STRING,
              | rate BIGINT,
              | proctime TIMESTAMP_LTZ(3)
              | """.stripMargin)
    )

    val sql = "INSERT INTO proctime_sink1 " +
      " SELECT r.* FROM orders_proctime AS o " +
      " JOIN changelog_currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    assertThatThrownBy(() => tEnv.executeSql(sql).await())
      .hasMessage("Processing-time temporal join is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testProcTimeTemporalJoinWithView(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    assertThatThrownBy(() => tEnv.executeSql(sql).await())
      .hasMessage("Processing-time temporal join is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testProcTimeLeftTemporalJoinWithView(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " LEFT JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no"

    assertThatThrownBy(() => tEnv.executeSql(sql).await())
      .hasMessage("Processing-time temporal join is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testProcTimeTemporalJoinWithViewNonEqui(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime AS r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " AND o.amount > r.rate"

    assertThatThrownBy(() => tEnv.executeSql(sql).await())
      .hasMessage("Processing-time temporal join is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testProcTimeLeftTemporalJoinWithViewWithPredicates(): Unit = {
    val sql = "INSERT INTO proctime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r.proctime " +
      " FROM orders_proctime AS o " +
      " LEFT JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime AS r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no" +
      " AND o.amount > r.rate"

    assertThatThrownBy(() => tEnv.executeSql(sql).await())
      .hasMessage("Processing-time temporal join is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testProcTimeMultiTemporalJoin(): Unit = {
    createSinkTable(
      "proctime_sink2",
      Some(s"""
              |  order_id BIGINT,
              |  currency STRING,
              |  amount BIGINT,
              |  l_time TIMESTAMP_LTZ(3),
              |  rate BIGINT,
              |  r_time TIMESTAMP_LTZ(3),
              |  PRIMARY KEY(order_id) NOT ENFORCED
              |""".stripMargin)
    )
    val sql = "INSERT INTO proctime_sink2 " +
      " SELECT o.order_id, o.currency, o.amount, o.proctime, r.rate, r1.proctime " +
      " FROM orders_proctime AS o " +
      " JOIN latest_rates FOR SYSTEM_TIME AS OF o.proctime as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " JOIN currency_proctime FOR SYSTEM_TIME AS OF o.proctime as r1" +
      " ON o.currency = r1.currency and o.currency_no = r1.currency_no"

    assertThatThrownBy(() => tEnv.executeSql(sql).await())
      .hasMessage("Processing-time temporal join is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testEventTimeTemporalJoin(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN versioned_currency_with_single_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"

    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeTemporalJoinThatJoinkeyContainsPk(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN versioned_currency_with_single_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency AND o.currency_no = r.currency_no"

    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeTemporalJoinWithFilter(): Unit = {
    tEnv.executeSql(
      "CREATE VIEW v1 AS" +
        " SELECT * FROM versioned_currency_with_single_key")
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o " +
      " JOIN v1 FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency" +
      " WHERE rate < 115"
    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02")
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeLeftTemporalJoin(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o LEFT JOIN versioned_currency_with_single_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"
    tEnv.executeSql(sql).await()

    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01",
      "5,RMB,40,2020-08-16T00:03,null,null"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeTemporalJoinChangelogUsingBeforeTime(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o LEFT JOIN currency_using_update_before_time " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"
    tEnv.executeSql(sql).await()

    // Note: the event time semantics in delete event is when the delete event happened,
    // records "+I(2,US Dollar)" and "+I(3,RMB)" would not correlate the deleted events
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,null,null",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01",
      "5,RMB,40,2020-08-16T00:03,null,null"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeLeftTemporalJoinUpsertSource(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o LEFT JOIN upsert_currency " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency "
    tEnv.executeSql(sql).await()

    // Note: the event time semantics in delete event is when the delete event happened,
    // record "+I(3,RMB)" would not correlate the deleted event
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,104,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,null,null",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01",
      "5,RMB,40,2020-08-16T00:03,null,null"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeTemporalJoinWithMultiKeys(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN versioned_currency_with_multi_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency_no = r.currency_no AND o.currency = r.currency"
    tEnv.executeSql(sql).await()

    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeTemporalJoinWithNonEqualCondition(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN currency_using_update_before_time " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " and o.order_id < 5 and r.rate > 102"
    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeTemporalJoinEqualConditionOnKey(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN currency_using_update_before_time " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " and o.currency = 'Euro' and r.rate > 102"
    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeMultiTemporalJoin(): Unit = {
    createSinkTable(
      "rowtime_sink1",
      Some(
        s"""
           |  order_id BIGINT,
           |  currency STRING,
           |  amount BIGINT,
           |  l_time TIMESTAMP(3),
           |  rate BIGINT,
           |  r_time TIMESTAMP(3),
           |  r1_rate BIGINT,
           |  r1_time TIMESTAMP(3),
           |  PRIMARY KEY(order_id) NOT ENFORCED
           |""".stripMargin
      )
    )
    val sql = "INSERT INTO rowtime_sink1 " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time," +
      " r1.rate, r1.currency_time FROM orders_rowtime AS o " +
      " LEFT JOIN versioned_currency_with_multi_key " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency and o.currency_no = r.currency_no " +
      " LEFT JOIN versioned_currency_with_single_key  FOR SYSTEM_TIME AS OF o.order_time as r1 " +
      " ON o.currency = r1.currency"

    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02,106,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04,702,2020-08-15T00:00:04",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01,118,2020-08-16T00:01",
      "5,RMB,40,2020-08-16T00:03,null,null,null,null"
    )
    assertThat(getResultsAsStrings("rowtime_sink1").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeTemporalJoinWithDeduplicateFirstView(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o " +
      " LEFT JOIN currency_deduplicated_first_row " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"

    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,102,2020-08-15T00:00:02",
      "3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04",
      "4,Euro,14,2020-08-16T00:04,114,2020-08-15T00:00:01",
      "5,RMB,40,2020-08-16T00:03,702,2020-08-15T00:00:04"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeTemporalJoinWithDeduplicateLastView(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o " +
      " JOIN currency_deduplicated_last_row " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"

    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01",
      "5,RMB,40,2020-08-16T00:03,702,2020-08-15T00:00:04"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeLeftTemporalJoinWithView(): Unit = {
    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o " +
      " LEFT JOIN currency_deduplicated_last_row " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency AND substr(o.currency, 1, 2) = 'US' "

    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,null,null",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,null,null",
      "4,Euro,14,2020-08-16T00:04,null,null",
      "5,RMB,40,2020-08-16T00:03,null,null"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testMiniBatchEventTimeViewTemporalJoin(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, Boolean.box(true))
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY.key(), "10 s")
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, Long.box(4L))

    val sql = "INSERT INTO rowtime_default_sink " +
      " SELECT o.order_id, o.currency, o.amount, o.order_time, r.rate, r.currency_time " +
      " FROM orders_rowtime AS o JOIN " +
      " currency_deduplicated_last_row " +
      " FOR SYSTEM_TIME AS OF o.order_time as r " +
      " ON o.currency = r.currency"

    tEnv.executeSql(sql).await()
    val expected = List(
      "1,Euro,12,2020-08-15T00:01,114,2020-08-15T00:00:01",
      "2,US Dollar,18,2020-08-16T00:03,106,2020-08-16T00:02",
      "3,RMB,40,2020-08-15T00:03,702,2020-08-15T00:00:04",
      "4,Euro,14,2020-08-16T00:04,118,2020-08-16T00:01",
      "5,RMB,40,2020-08-16T00:03,702,2020-08-15T00:00:04"
    )
    assertThat(getResultsAsStrings("rowtime_default_sink").sorted).isEqualTo(expected.sorted)
  }

  private def createSinkTable(tableName: String, columns: Option[String]): Unit = {
    val columnsDDL = columns match {
      case Some(cols) => cols
      case _ =>
        s"""
           |  order_id BIGINT,
           |  currency STRING,
           |  amount BIGINT,
           |  l_time TIMESTAMP(3),
           |  rate BIGINT,
           |  r_time TIMESTAMP(3),
           |  PRIMARY KEY(order_id) NOT ENFORCED
           |""".stripMargin
    }

    tEnv.executeSql(s"""
                       |CREATE TABLE $tableName (
                       | $columnsDDL
                       |) WITH (
                       |  'connector' = 'values',
                       |  'sink-insert-only' = 'false',
                       |  'changelog-mode' = 'I,UA,UB,D'
                       |)
                       |""".stripMargin)
  }

  private def changelogRow(kind: String, values: Any*): Row = {
    val objects = values.map {
      case l: Long => Long.box(l)
      case i: Int => Int.box(i)
      case date: String =>
        try {
          LocalDateTime.parse(date)
        } catch {
          case _: DateTimeParseException => date
        }
      case o: Object => o
    }
    TestValuesTableFactory.changelogRow(kind, objects.toArray: _*)
  }
}
