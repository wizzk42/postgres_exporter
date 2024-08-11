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
	"database/sql"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/jackc/pgtype"
	"github.com/prometheus/client_golang/prometheus"
)

const pgPreparedTransactionsSubsystem = "prepared_transactions"

func init() {
	registerCollector(pgPreparedTransactionsSubsystem, defaultEnabled, NewPreparedTransactionsCollector)
}

type PGPreparedTransactionsCollector struct {
	log log.Logger
}

func NewPreparedTransactionsCollector(config collectorConfig) (Collector, error) {
	return &PGPreparedTransactionsCollector{
		log: config.logger,
	}, nil
}

var (
	pgPreparedTransactionsOverviewDesc = prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			pgPreparedTransactionsSubsystem,
			"overview",
		),
		"Currently active prepared transactions",
		[]string{
			"local_transaction_id",
			"global_transaction_id",
			"owner",
			"database",
			"timestamp",
		},
		prometheus.Labels{},
	)

	pgPreparedTransactionsCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			pgPreparedTransactionsSubsystem,
			"count",
		),
		"Prepared Transactions grouped by owner and database",
		[]string{
			"owner",
			"database",
		},
		prometheus.Labels{},
	)

	pgTransactionsInProgressDesc = prometheus.NewDesc(
		prometheus.BuildFQName(
			namespace,
			pgPreparedTransactionsSubsystem,
			"in_progress",
		),
		"Transactions in progress (xids), xmin/xmax: min/max Transaction still active",
		[]string{
			"xids",
			"xmin",
			"xmax",
		},
		prometheus.Labels{},
	)

	pgPreparedTransactionsOverviewQuery = `
		select
		     "xacts"."transaction" as local_transaction_id
		   , "xacts"."gid" as global_transaction_id
		   , "xacts"."owner" as owner
		   , "xacts"."database" as database
		   , "xacts"."prepared" as prepared_timestamp
		from
		    pg_prepared_xacts xacts
		order by
		      "xacts"."owner" asc
		    , "xacts"."database" asc
		    , "xacts"."prepared" desc
	`

	pgPreparedTransactionsCountPerDatabaseAndOwnerQuery = `
		select
			  "xacts"."owner" as owner
			, "xacts"."database" as database
			, count(*) as num_transactions
		from
			pg_prepared_xacts xacts
		group by
			  "xacts"."owner"
			, "xacts"."database"
	`

	pgTransactionsInProgressQuery = `
		with q_snapshot as (
			select
		    	pg_current_snapshot() as snapshot
		)
		select
		      array(
		          select
		              pg_snapshot_xip(s.snapshot)
		      ) as "xips"
			, pg_snapshot_xmin(s.snapshot) as "xmin"
			, pg_snapshot_xmax(s.snapshot) as "xmax"
			, array_length(array(
		          select
		              pg_snapshot_xip(s.snapshot)
		      ), 1) as "num_transactions"
		from
		    q_snapshot as s
	`
)

func (c PGPreparedTransactionsCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {

	errOverview := updateOverviewMetric(ctx, instance, ch)
	if errOverview != nil {
		return errOverview
	}

	errCount := updateCountByDatabaseAndOwnerMetric(ctx, instance, ch)
	if errCount != nil {
		return errCount
	}

	errXips := updateTransactionsProgress(ctx, instance, ch)
	if errXips != nil {
		return errXips
	}

	return nil
}

func updateOverviewMetric(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	rows, err := instance.db.QueryContext(ctx,
		pgPreparedTransactionsOverviewQuery,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var localTransactionId pgtype.XID
	var globalTransactionIdLabel, ownerLabel, databaseLabel sql.NullString
	var preparedTimestamp pgtype.Timestamp

	for rows.Next() {
		if err := rows.Scan(&localTransactionId, &globalTransactionIdLabel, &ownerLabel, &databaseLabel, &preparedTimestamp); err != nil {
			return err
		}

		if !globalTransactionIdLabel.Valid || !ownerLabel.Valid || !databaseLabel.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			pgPreparedTransactionsOverviewDesc,
			prometheus.CounterValue,
			1,
			formatXID(localTransactionId),
			globalTransactionIdLabel.String,
			ownerLabel.String,
			databaseLabel.String,
			preparedTimestamp.Time.Format(time.RFC3339),
		)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func updateCountByDatabaseAndOwnerMetric(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	rows, err := instance.db.QueryContext(ctx,
		pgPreparedTransactionsCountPerDatabaseAndOwnerQuery,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			ownerLabel, databaseLabel sql.NullString
			count                     uint64
		)

		if err := rows.Scan(&ownerLabel, &databaseLabel, &count); err != nil {
			return err
		}

		if !ownerLabel.Valid || !databaseLabel.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			pgPreparedTransactionsCountDesc,
			prometheus.CounterValue,
			float64(count),
			ownerLabel.String,
			databaseLabel.String,
		)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func updateTransactionsProgress(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	rows, err := instance.db.QueryContext(ctx,
		pgTransactionsInProgressQuery,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			xidList           sql.NullString
			xmin, xmax, count sql.NullInt64
		)

		if err := rows.Scan(&xidList, &xmin, &xmax, &count); err != nil {
			return err
		}

		if !xmin.Valid || !xmax.Valid || !count.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			pgTransactionsInProgressDesc,
			prometheus.CounterValue,
			float64(count.Int64),
			xidList.String,
			fmt.Sprintf("%d", xmin.Int64),
			fmt.Sprintf("%d", xmax.Int64),
		)
	}

	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func formatXID(xid pgtype.XID) string {
	return fmt.Sprint(xid.Uint)
}

func formatXIDs(xids []pgtype.XID) string {
	result := ""
	for i := 0; i < len(xids); i += 1 {
		result += fmt.Sprintf("%d,", xids[i].Uint)
	}

	return result
}
