// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	insertDeleteRangeSQL   = `INSERT IGNORE INTO mysql.gc_delete_range VALUES ("%d", "%d", "%s", "%s", "%d")`
	loadDeleteRangeSQL     = `SELECT job_id, element_id, start_key, end_key FROM mysql.gc_delete_range WHERE ts < %v`
	completeDeleteRangeSQL = `DELETE FROM mysql.gc_delete_range WHERE job_id = %d AND element_id = %d`
)

// LoadPendingBgJobsIntoDeleteTable loads all pending DDL backgroud jobs
// into table `gc_delete_range` so that gc worker can process them.
// NOTE: This function WILL NOT start and run in a new transaction internally.
func LoadPendingBgJobsIntoDeleteTable(ctx context.Context) (err error) {
	var met = meta.NewMeta(ctx.Txn())
	for {
		var job *model.Job
		job, err = met.DeQueueBgJob()
		if err != nil || job == nil {
			break
		}
		err = insertBgJobIntoDeleteRangeTable(ctx, job)
		if err != nil {
			break
		}
	}
	return errors.Trace(err)
}

// insertBgJobIntoDeleteRangeTable parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range table. The primary key is
// job ID, so we ignore key conflict error.
func insertBgJobIntoDeleteRangeTable(ctx context.Context, job *model.Job) error {
	s := ctx.(sqlexec.SQLExecutor)
	switch job.Type {
	case model.ActionDropSchema:
		var tableIDs []int64
		if err := job.DecodeArgs(&tableIDs); err != nil {
			return errors.Trace(err)
		}
		for _, tableID := range tableIDs {
			startKey := tablecodec.EncodeTablePrefix(tableID)
			endKey := tablecodec.EncodeTablePrefix(tableID + 1)
			if err := doInsert(s, job.ID, tableID, startKey, endKey, time.Now().Unix()); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionDropTable, model.ActionTruncateTable:
		tableID := job.TableID
		startKey := tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		return doInsert(s, job.ID, tableID, startKey, endKey, time.Now().Unix())
	}
	return nil
}

func startTxn(ctx context.Context) error {
	s := ctx.(sqlexec.SQLExecutor)
	_, err := s.Execute("BEGIN")
	return errors.Trace(err)
}

func commitTxn(ctx context.Context) error {
	s := ctx.(sqlexec.SQLExecutor)
	_, err := s.Execute("COMMIT")
	return errors.Trace(err)
}

func doInsert(s sqlexec.SQLExecutor, jobID int64, elementID int64, startKey, endKey kv.Key, ts int64) error {
	log.Infof("[ddl] insert into delete-range table with key: (%d,%d)", jobID, elementID)
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	sql := fmt.Sprintf(insertDeleteRangeSQL, jobID, elementID, startKeyEncoded, endKeyEncoded, ts)
	_, err := s.Execute(sql)
	return errors.Trace(err)
}

// DelRangeTask is for run delete-range command in gc_worker.
type DelRangeTask struct {
	jobID, elementID int64
	startKey, endKey []byte
}

// Range returns the range [start, end) to delete.
func (t DelRangeTask) Range() ([]byte, []byte) {
	return t.startKey, t.endKey
}

// LoadDeleteRanges loads delete range tasks from gc_delete_range table.
func LoadDeleteRanges(ctx context.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	sql := fmt.Sprintf(loadDeleteRangeSQL, safePoint)
	rss, err := ctx.(sqlexec.SQLExecutor).Execute(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rs := rss[0]
	for {
		row, err := rs.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		startKey, err := hex.DecodeString(row.Data[2].GetString())
		if err != nil {
			return nil, errors.Trace(err)
		}
		endKey, err := hex.DecodeString(row.Data[3].GetString())
		if err != nil {
			return nil, errors.Trace(err)
		}
		ranges = append(ranges, DelRangeTask{
			jobID:     row.Data[0].GetInt64(),
			elementID: row.Data[1].GetInt64(),
			startKey:  startKey,
			endKey:    endKey,
		})
	}
	return ranges, nil
}

// CompleteDeleteRange deletes a record from gc_delete_range table.
// NOTE: This function WILL NOT start and run in a new transaction internally.
func CompleteDeleteRange(ctx context.Context, dr DelRangeTask) error {
	sql := fmt.Sprintf(completeDeleteRangeSQL, dr.jobID, dr.elementID)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(sql)
	return errors.Trace(err)
}

// delRangeEmulator is only for localstore which doesn't support delete-range.
type delRangeEmulator struct {
	ddl    *ddl
	sqlCtx context.Context
}

// newDelRangeEmulator binds a SQL context on a delRangeEmulator and returns it.
func newDelRangeEmulator(ddl *ddl, ctxPool *pools.ResourcePool) *delRangeEmulator {
	resource, _ := ctxPool.Get()
	ctx := resource.(context.Context)
	ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusAutocommit, false)
	return &delRangeEmulator{
		ddl:    ddl,
		sqlCtx: ctx,
	}
	return nil
}

func (delRange *delRangeEmulator) start() {
	go func() {
		defer delRange.ddl.wait.Done()

		checkTime := 60 * time.Second // TODO: get from safepoint.
		ticker := time.NewTicker(checkTime)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
			case <-delRange.ddl.quitCh:
				return
			}
			// TODO: Emulate delete-range here.
		}
	}()
}
