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
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestPreparedTransactionsCollector(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Error opening a stub db connection: %s", err)
	}
	defer db.Close()

	inst := &instance{db: db}

	mock.ExpectQuery(sanitizeQuery(pgPreparedTransactionsOverviewQuery)).WillReturnRows(
		sqlmock.NewRows([]string{
			"local_transaction_id",
			"global_transaction_id",
			"owner",
			"database",
			"prepared_timestamp",
		}).AddRow(
			"12345",
			"test_1234",
			"postgres",
			"postgres2",
			"2024-08-05 10:11:12",
		))
	mock.ExpectQuery(sanitizeQuery(pgPreparedTransactionsCountPerDatabaseAndOwnerQuery)).WillReturnRows(
		sqlmock.NewRows([]string{
			"owner",
			"database",
			"num_transactions",
		}).AddRow(
			"postgres_owner",
			"postgres",
			"123",
		))
	mock.ExpectQuery(sanitizeQuery(pgTransactionsInProgressQuery)).WillReturnRows(
		sqlmock.NewRows([]string{
			"xips",
			"xmin",
			"xmax",
			"num_xacts",
		}).AddRow(
			"{123,321}",
			"123",
			"322",
			"199",
		))

	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		c := PGPreparedTransactionsCollector{}
		if err := c.Update(context.Background(), inst, ch); err != nil {
			t.Errorf("Error calling PGPreparedTransactionsCollector.Update: %s", err)
		}
	}()

	timestamp, _ := time.Parse(time.RFC3339, "2024-08-05T10:11:12Z")
	expected := []MetricResult{
		{
			labels: labelMap{
				"local_transaction_id":  "12345",
				"global_transaction_id": "test_1234",
				"owner":                 "postgres",
				"database":              "postgres2",
				"timestamp":             timestamp.Format(time.RFC3339),
			},
			value:      1,
			metricType: dto.MetricType_COUNTER,
		},
		{
			labels: labelMap{
				"owner":    "postgres_owner",
				"database": "postgres",
			},
			value:      123,
			metricType: dto.MetricType_COUNTER,
		},
		{
			labels: labelMap{
				"xids": "{123,321}",
				"xmin": "123",
				"xmax": "322",
			},
			value:      199,
			metricType: dto.MetricType_COUNTER,
		},
	}
	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			m := readMetric(<-ch)
			convey.So(m, convey.ShouldResemble, expect)
		}
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}
