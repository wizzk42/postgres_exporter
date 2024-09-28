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
	"github.com/go-kit/log"
	"github.com/jackc/pgtype"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
)

const sentinelSubsystem = "sentinel"

func init() {
	registerCollector(sentinelSubsystem, defaultEnabled, NewPGSentinelCollector)
}

type PGSentinelCollector struct {
	log log.Logger
}

var (
	sentinelStatStatementsHistoryRowsPerSecondDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "ssh_rps"),
		"Statement history per query: Rows per Second",
		[]string{"query_id"},
		prometheus.Labels{},
	)
	sentinelStatStatementsHistoryCallsPerSecondDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "ssh_cps"),
		"Statement history per query: Calls per Second",
		[]string{"query_id"},
		prometheus.Labels{},
	)
	sentinelStatStatementsHistoryRowsPerCallDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "ssh_rpc"),
		"Statement history per query: Rows per Call",
		[]string{"query_id"},
		prometheus.Labels{},
	)
	sentinetAverageActiveSessionsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "aas"),
		"Average Active Sessions",
		[]string{"active_sessions"},
		prometheus.Labels{},
	)
	sentinelCpuPerQueryDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "cpu"),
		"Percent CPU usage per query",
		[]string{"aas", "backend_type", "query_id", "query"},
		prometheus.Labels{},
	)
	sentinelIoPerQueryDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "io"),
		"Percent IO usage per query",
		[]string{"aas", "backend_type", "query_id", "query", "wait_event_type"},
		prometheus.Labels{},
	)
	sentinelWaitEventPerDatabaseDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "wait_events_database"),
		"Wait events per database",
		[]string{"aas", "database", "wait_event_type", "wait_event"},
		prometheus.Labels{},
	)
	sentinelWaitEventTypePerQueryDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "wait_events_query"),
		"Wait events per query",
		[]string{"aas", "query_id", "query", "wait_event_type"},
		prometheus.Labels{},
	)
	sentinelRecursiveWaitChainPercentDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "wait_chain_percent"),
		"Wait Chains",
		[]string{"path"},
		prometheus.Labels{},
	)
	sentinelRecursiveWaitChainSecondsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, sentinelSubsystem, "wait_chain_seconds"),
		"Wait Chains",
		[]string{"path"},
		prometheus.Labels{},
	)

	// Queries
	pgSentinelStatStatementsHistory = `
		select
			  ash_time
		    , queryid
			, delta_rows/seconds as "rows_per_seconds"
		    , delta_calls/seconds as "calls_per_second"
		    , delta_rows/delta_calls as "rows_per_calls"
		from (
			select
			      ash_time
			    , queryid
			    , extract(epoch from ash_time::timestamp) - lag (extract(epoch from ash_time::timestamp)) over (
					partition by
				    	pgssh.queryid
					order by
				    	ash_time
				) as "seconds"
			    , rows-lag(rows) over (
					partition by
					    pgssh.queryid
					order by ash_time
			    ) as "delta_rows"
			    , calls-lag(calls) over (
					partition by
					    pgssh.queryid
					order by
					    ash_time
			    ) as "delta_calls"
			from
			    pg_stat_statements_history pgssh
		) as delta
		where
		    delta_calls > 0
		and
		    seconds > 0
		order by
		    ash_time desc;
	`

	pgSentinelAverageActiveSessionsQuery = `
		with ash as (
			select
				  *
				, ceil(extract(epoch from max(ash_time)over()-min(ash_time)over()))::numeric samples
			from
				pg_active_session_history
			where
				ash_time >= current_timestamp - interval '10 minutes'
		)
		select
			round(count(*)/samples,2) as "AAS"
		from
			ash
		group by
			samples
    `

	pgSentinelCpuQuery = `
		with ash as (
			select
				  *
				, ceil(extract(epoch from max(ash_time)over()-min(ash_time)over()))::numeric samples
			from
				pg_active_session_history
			where
				ash_time >= current_timestamp - interval '10 minutes'
		)
		select
			  round(100 * count(*)/sum(count(*)) over(),0) as "%"
            , round(count(*)/samples,2) as "AAS"
			, backend_type
		    , queryid
            , pg_stat_statements.query
		from
			ash
		left outer join
			pg_stat_statements using(queryid)
		where
			wait_event='CPU'
		group by
		      samples
		    , queryid
			, pg_stat_statements.query
            , backend_type
		order by
			1 desc
		fetch first 10 rows only
	`

	pgSentinelIoPerQuery = `
		with ash as (
			select
				 *
				, ceil(extract(epoch from max(ash_time)over()-min(ash_time)over()))::numeric samples
		from
			pg_active_session_history
		where
			ash_time >= current_timestamp - interval '10 minutes'
		)
		select
			  round(100 * count(*)/sum(count(*)) over(),0) as "%"
			, round(count(*)/samples,2) as "AAS"
			, backend_type
 			, queryid
		    , pg_stat_statements.query
			, wait_event_type
		from
			ash
		left outer join
			pg_stat_statements using(queryid)
		where
			wait_event_type='IO'
		group by
			  backend_type
			, queryid
		    , pg_stat_statements.query
			, samples
			, wait_event_type
		order by
			1 desc
		fetch first 10 rows only
	`

	pgSentinelWaitEventPerDatabaseQuery = `
		with ash as (
			select
			      *
				, ceil(extract(epoch from max(ash_time)over()-min(ash_time)over()))::numeric samples
			from
			    pg_active_session_history
			where
			    ash_time >= current_timestamp - interval '10 minutes'
		)
		select
		      round(100 * count(*)/sum(count(*)) over(),0) as "%"
		    , round(count(*)/samples,2) as "AAS"
		    , datname as "database"
		    , wait_event_type
		    , wait_event
		from
		    ash
		group by
		      samples
		    , datname
		    , wait_event_type
		    , wait_event
		order by
		    1 desc
	`

	pgSentinelWaitEventTypePerQuery = `
		with ash as (
			select
				  *
				, ceil(extract(epoch from max(ash_time)over()-min(ash_time)over()))::numeric samples
			from
				pg_active_session_history
			where
				ash_time >= current_timestamp - interval '10 minutes'
		)
		select
			  round(100 * count(*)/sum(count(*)) over(),0) as "%"
			, round(count(*)/samples,2) as "AAS"
			, queryid
		    , pg_stat_statements.query
			, wait_event_type
		from
		    ash
		left outer join
		    pg_stat_statements using(queryid)
		where
			wait_event_type != 'CPU'
		group by
			  queryid
		    , pg_stat_statements.query
			, samples
			, wait_event_type
		order by
			1 desc
		fetch first 10 rows only
	`

	pgSentinelRecursiveWaitChainQuery = `
		with recursive search_wait_chain(
			  ash_time
			, pid
			, blockerpid
			, wait_event_type
			, wait_event,level
			, path
		) as (
			select
				  ash_time
				, pid
				, blockerpid
				, wait_event_type
				, wait_event
				, 1 as level
				, 'pid:' || pid || ' (' || wait_event_type || ' : ' || wait_event || ') ->' || 'pid:' || blockerpid as path
			from
				pg_active_session_history
			where
				blockers > 0
			union all
			select
				  p.ash_time
				, p.pid
				, p.blockerpid
				, p.wait_event_type
				, p.wait_event
				, swc.level + 1 as level
				, 'pid:' || p.pid || ' (' || p.wait_event_type || ' : ' || p.wait_event || ') ->' || swc.path as path
			from
				  pg_active_session_history p
				, search_wait_chain swc
			where
				p.blockerpid = swc.pid
			and
				p.ash_time = swc.ash_time
			and
				p.blockers > 0
		)
		select
			  round(100 * count(*) / cnt) || '%' as "% of total wait"
			, count(*) as seconds
			, path as wait_chain
		from (
			select
				  pid
				, wait_event
                , path
			    , sum(count) over() as cnt
			from (
				select
					  ash_time
					, level
                    , pid
					, wait_event
				    , path
 					, count(*) as count
					, max(level) over(partition by ash_time, pid) as max_level
				from
					search_wait_chain
				where
					level > 0
				group by
					  ash_time
					, level
					, pid
					, wait_event
					, path
			) as all_wait_chain
			where
				level=max_level
		) as wait_chain
		group by
			  path
		    , cnt
		order by
			count(*) desc
	`
)

func NewPGSentinelCollector(config collectorConfig) (Collector, error) {
	return &PGSentinelCollector{log: config.logger}, nil
}

func (PGSentinelCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	sshErr := updateStatStatementsHistory(ctx, instance, ch)
	if sshErr != nil {
		return sshErr
	}
	aasErr := updateAverageActiveSessions(ctx, instance, ch)
	if aasErr != nil {
		return aasErr
	}
	cpuErr := updateCpuUsagePerQuery(ctx, instance, ch)
	if cpuErr != nil {
		return cpuErr
	}
	ioErr := updateIoUsagePerQuery(ctx, instance, ch)
	if ioErr != nil {
		return ioErr
	}
	waitEventsPerDatabaseErr := updateWaitEventPerDatabase(ctx, instance, ch)
	if waitEventsPerDatabaseErr != nil {
		return waitEventsPerDatabaseErr
	}
	waitEventsPerQueryErr := updateWaitEventTypePerQuery(ctx, instance, ch)
	if waitEventsPerQueryErr != nil {
		return waitEventsPerQueryErr
	}
	recursiveWaitErr := updateRecursiveWaitChainUsage(ctx, instance, ch)
	if recursiveWaitErr != nil {
		return recursiveWaitErr
	}
	return nil
}
func updateStatStatementsHistory(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgSentinelStatStatementsHistory)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			ashTime        pgtype.Timestamp
			queryId        sql.NullString
			rowsPerSecond  sql.NullFloat64
			callsPerSecond sql.NullFloat64
			rowsPerCall    sql.NullFloat64
		)

		err = rows.Scan(&ashTime, &queryId, &rowsPerSecond, &callsPerSecond, &rowsPerCall)
		if err != nil {
			return err
		}

		if !queryId.Valid || !rowsPerSecond.Valid || !callsPerSecond.Valid || !rowsPerCall.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetricWithCreatedTimestamp(
			sentinelStatStatementsHistoryRowsPerSecondDesc,
			prometheus.CounterValue,
			rowsPerSecond.Float64,
			ashTime.Time,
			queryId.String,
		)
		ch <- prometheus.MustNewConstMetricWithCreatedTimestamp(
			sentinelStatStatementsHistoryCallsPerSecondDesc,
			prometheus.CounterValue,
			callsPerSecond.Float64,
			ashTime.Time,
			queryId.String,
		)
		ch <- prometheus.MustNewConstMetricWithCreatedTimestamp(
			sentinelStatStatementsHistoryRowsPerCallDesc,
			prometheus.CounterValue,
			rowsPerCall.Float64,
			ashTime.Time,
			queryId.String,
		)
	}

	return nil
}

func updateAverageActiveSessions(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgSentinelAverageActiveSessionsQuery)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var percent sql.NullFloat64

		err = rows.Scan(&percent)
		if err != nil {
			return err
		}

		if !percent.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			sentinetAverageActiveSessionsDesc,
			prometheus.GaugeValue,
			percent.Float64,
			"active_sessions",
		)
	}

	return nil
}

func updateCpuUsagePerQuery(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgSentinelCpuQuery)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			percent                          sql.NullFloat64
			aas, backendType, queryId, query sql.NullString
		)

		err = rows.Scan(&percent, &aas, &backendType, &queryId, &query)
		if err != nil {
			return err
		}

		if !percent.Valid || !aas.Valid || !backendType.Valid || !queryId.Valid || !query.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			sentinelCpuPerQueryDesc,
			prometheus.GaugeValue,
			percent.Float64,
			aas.String, backendType.String, queryId.String, query.String,
		)
	}

	return nil
}

func updateIoUsagePerQuery(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgSentinelIoPerQuery)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			percent                                         sql.NullFloat64
			aas, backendType, queryId, query, waitEventType sql.NullString
		)

		err = rows.Scan(&percent, &aas, &backendType, &queryId, &query, &waitEventType)
		if err != nil {
			return err
		}

		if !percent.Valid || !aas.Valid || !backendType.Valid || !queryId.Valid || !query.Valid || !waitEventType.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			sentinelIoPerQueryDesc,
			prometheus.GaugeValue,
			percent.Float64,
			aas.String, backendType.String, queryId.String, query.String, waitEventType.String,
		)
	}

	return nil
}

func updateWaitEventPerDatabase(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgSentinelWaitEventPerDatabaseQuery)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {

		var (
			percent, activeSessions            sql.NullFloat64
			database, waitEventType, waitEvent sql.NullString
		)

		err := rows.Scan(&percent, &activeSessions, &database, &waitEventType, &waitEvent)
		if err != nil {
			return err
		}

		if !percent.Valid || !activeSessions.Valid || !waitEventType.Valid || !waitEvent.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			sentinelWaitEventPerDatabaseDesc,
			prometheus.GaugeValue,
			percent.Float64,
			strconv.FormatFloat(activeSessions.Float64, 'f', 2, 64), database.String, waitEventType.String, waitEvent.String,
		)
	}

	return nil
}
func updateWaitEventTypePerQuery(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgSentinelWaitEventTypePerQuery)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			percent                            sql.NullFloat64
			aas, queryId, query, waitEventType sql.NullString
		)

		err = rows.Scan(&percent, &aas, &queryId, &query, &waitEventType)
		if err != nil {
			return err
		}

		if !percent.Valid || !aas.Valid || !queryId.Valid || !query.Valid || !waitEventType.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			sentinelWaitEventTypePerQueryDesc,
			prometheus.GaugeValue,
			percent.Float64,
			aas.String, queryId.String, query.String, waitEventType.String,
		)
	}

	return nil
}

func updateRecursiveWaitChainUsage(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgSentinelRecursiveWaitChainQuery)

	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			percent sql.NullFloat64
			seconds sql.NullInt64
			path    sql.NullString
		)

		err = rows.Scan(&percent, &seconds, &path)
		if err != nil {
			return err
		}

		if !percent.Valid || !seconds.Valid || !path.Valid {
			continue
		}

		ch <- prometheus.MustNewConstMetric(
			sentinelRecursiveWaitChainPercentDesc,
			prometheus.GaugeValue,
			percent.Float64,
			path.String,
		)
		ch <- prometheus.MustNewConstMetric(
			sentinelRecursiveWaitChainSecondsDesc,
			prometheus.GaugeValue,
			float64(seconds.Int64),
			path.String,
		)
	}

	return nil
}
