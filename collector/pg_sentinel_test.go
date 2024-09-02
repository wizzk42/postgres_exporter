// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package collector

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestPGSentinelCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db, version: semver.MustParse("12.0.0")}

	mock.ExpectQuery(sanitizeQuery(pgSentinelStatStatementsHistory)).WithoutArgs().WillReturnRows(
		sqlmock.NewRows([]string{
			"ash_time",
			"queryid",
			"rows_per_second",
			"calls_per_second",
			"rows_per_call",
		}).AddRow(
			"2024-01-01 00:00:00",
			"1234",
			10,
			5,
			1,
		))

	mock.ExpectQuery(sanitizeQuery(pgSentinelAverageActiveSessionsQuery)).WithoutArgs().WillReturnRows(
		sqlmock.NewRows([]string{
			"AAS",
		}).AddRow("99.0"))

	mock.ExpectQuery(sanitizeQuery(pgSentinelCpuQuery)).WithoutArgs().WillReturnRows(
		sqlmock.NewRows([]string{
			"%",
			"AAS",
			"backend_type",
			"queryid",
			"query",
		}).AddRow(
			"99",
			"10.0",
			"mock_backend",
			"1234",
			"mock_query",
		))

	mock.ExpectQuery(sanitizeQuery(pgSentinelIoPerQuery)).WithoutArgs().WillReturnRows(
		sqlmock.NewRows([]string{
			"%",
			"AAS",
			"backend_type",
			"queryid",
			"wait_event_type",
		}).AddRow(
			"99",
			"10.0",
			"mock_backend",
			"1234",
			"client",
		))
	mock.ExpectQuery(sanitizeQuery(pgSentinelWaitEventPerDatabaseQuery)).WithoutArgs().WillReturnRows(
		sqlmock.NewRows([]string{
			"%",
			"AAS",
			"database",
			"wait_event_type",
			"wait_event",
		}).AddRow(
			"99",
			"10",
			"postgres",
			"client",
			"LWLock",
		))
	mock.ExpectQuery(sanitizeQuery(pgSentinelWaitEventTypePerQuery)).WithoutArgs().WillReturnRows(
		sqlmock.NewRows([]string{
			"%",
			"AAS",
			"queryid",
			"wait_event_type",
		}).AddRow(
			"99",
			"10.0",
			"1234",
			"client",
		))
	mock.ExpectQuery(sanitizeQuery(pgSentinelRecursiveWaitChainQuery)).WithoutArgs().WillReturnRows(
		sqlmock.NewRows([]string{
			"% of total wait",
			"seconds",
			"wait_chain",
		}).AddRow(
			"99",
			"10",
			"mock_wait_chain",
		))

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGSentinelCollector{}

		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGSentinelCollector.Update: %s", err)
		}
	}()

	expected := []MetricResult{
		{
			labels: labelMap{
				"query_id": "1234",
			},
			value:      10,
			metricType: dto.MetricType_COUNTER,
		},
		{
			labels: labelMap{
				"query_id": "1234",
			},
			value:      5,
			metricType: dto.MetricType_COUNTER,
		},
		{
			labels: labelMap{
				"query_id": "1234",
			},
			value:      1,
			metricType: dto.MetricType_COUNTER,
		},
		{
			labels: labelMap{
				"active_sessions": "active_sessions",
			},
			value:      99,
			metricType: dto.MetricType_GAUGE,
		},
		{
			labels: labelMap{
				"aas":          "10.0",
				"backend_type": "mock_backend",
				"query_id":     "1234",
				"query":        "mock_query",
			},
			value:      99,
			metricType: dto.MetricType_GAUGE,
		},
		{
			labels: labelMap{
				"aas":             "10.0",
				"backend_type":    "mock_backend",
				"query_id":        "1234",
				"wait_event_type": "client",
			},
			value:      99,
			metricType: dto.MetricType_GAUGE,
		},
		{
			labels: labelMap{
				"database":        "postgres",
				"aas":             "10.00",
				"wait_event_type": "client",
				"wait_event":      "LWLock",
			},
			value:      99,
			metricType: dto.MetricType_GAUGE,
		},
		{
			labels: labelMap{
				"aas":             "10.0",
				"query_id":        "1234",
				"wait_event_type": "client",
			},
			value:      99,
			metricType: dto.MetricType_GAUGE,
		},
		{
			labels: labelMap{
				"path": "mock_wait_chain",
			},
			value:      99,
			metricType: dto.MetricType_GAUGE,
		},
		{
			labels: labelMap{
				"path": "mock_wait_chain",
			},
			value:      10,
			metricType: dto.MetricType_GAUGE,
		},
	}

	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, m)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}
