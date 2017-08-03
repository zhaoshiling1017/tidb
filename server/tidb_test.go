// Copyright 2015 PingCAP, Inc.
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
// +build !race

package server

import (
	"database/sql"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/config"
)

type TidbTestSuite struct {
	tidbdrv *TiDBDriver
	server  *Server
}

var _ = Suite(new(TidbTestSuite))

func (ts *TidbTestSuite) SetUpSuite(c *C) {
	log.SetLevelByString("error")
	store, err := tidb.NewStore("memory:///tmp/tidb")
	c.Assert(err, IsNil)
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(store)
	cfg := &config.Config{
		Addr:         ":4001",
		LogLevel:     "debug",
		StatusAddr:   ":10090",
		ReportStatus: true,
		TCPKeepAlive: true,
	}

	server, err := NewServer(cfg, ts.tidbdrv, nil)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	waitUntilServerOnline(cfg.StatusAddr)

	// Run this test here because parallel would affect the result of it.
	runTestStmtCount(c)
	defaultLoadDataBatchCnt = 3
}

func (ts *TidbTestSuite) TearDownSuite(c *C) {
	if ts.server != nil {
		ts.server.Close()
	}
}

func (ts *TidbTestSuite) TestRegression(c *C) {
	if regression {
		c.Parallel()
		runTestRegression(c, "Regression")
	}
}

func (ts *TidbTestSuite) TestUint64(c *C) {
	runTestPrepareResultFieldType(c)
}

func (ts *TidbTestSuite) TestSpecialType(c *C) {
	c.Parallel()
	runTestSpecialType(c)
}

func (ts *TidbTestSuite) TestPreparedString(c *C) {
	c.Parallel()
	runTestPreparedString(c)
}

func (ts *TidbTestSuite) TestLoadData(c *C) {
	runTestLoadData(c)
}

func (ts *TidbTestSuite) TestConcurrentUpdate(c *C) {
	runTestConcurrentUpdate(c)
}

func (ts *TidbTestSuite) TestErrorCode(c *C) {
	runTestErrorCode(c)
}

func (ts *TidbTestSuite) TestAuth(c *C) {
	runTestAuth(c)
}

func (ts *TidbTestSuite) TestIssues(c *C) {
	runTestIssues(c)
}

func (ts *TidbTestSuite) TestResultFieldTableIsNull(c *C) {
	c.Parallel()
	runTestResultFieldTableIsNull(c)
}

func (ts *TidbTestSuite) TestStatusAPI(c *C) {
	runTestStatusAPI(c)
}

func (ts *TidbTestSuite) TestMultiStatements(c *C) {
	c.Parallel()
	runTestMultiStatements(c)
}

func (ts *TidbTestSuite) TestSocket(c *C) {
	c.Parallel()
	cfg := &config.Config{
		LogLevel:   "debug",
		StatusAddr: ":10091",
		Socket:     "/tmp/tidbtest.sock",
	}

	server, err := NewServer(cfg, ts.tidbdrv, nil)
	c.Assert(err, IsNil)
	go server.Run()
	time.Sleep(time.Millisecond * 100)
	tcpDsn := dsn
	dsn = "root@unix(/tmp/tidbtest.sock)/test?strict=true"
	runTestRegression(c, "SocketRegression")
	dsn = tcpDsn
	server.Close()
}

func (ts *TidbTestSuite) TestIssue3662(c *C) {
	c.Parallel()
	db, err := sql.Open("mysql", "root@tcp(localhost:4001)/a_database_not_exist")
	c.Assert(err, IsNil)
	defer db.Close()

	// According to documentation, "Open may just validate its arguments without
	// creating a connection to the database. To verify that the data source name
	// is valid, call Ping."
	err = db.Ping()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Error 1049: Unknown database 'a_database_not_exist'")
}

func (ts *TidbTestSuite) TestIssue3680(c *C) {
	c.Parallel()
	db, err := sql.Open("mysql", "non_existing_user@tcp(127.0.0.1:4001)/")
	c.Assert(err, IsNil)
	defer db.Close()

	// According to documentation, "Open may just validate its arguments without
	// creating a connection to the database. To verify that the data source name
	// is valid, call Ping."
	err = db.Ping()
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "Error 1045: Access denied for user 'non_existing_user'@'127.0.0.1' (using password: YES)")
}

func (ts *TidbTestSuite) TestDBNameEscape(c *C) {
	c.Parallel()
	runTests(c, dsn, func(dbt *DBTest) {
		dbt.mustExec("create database `aa-a`;")
	})
	// The database name is aa-a, '-' is not permitted as identifier, it should be `aa-a` to be a legal sql.
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4001)/aa-a")
	c.Assert(err, IsNil)
	defer db.Close()
	c.Assert(db.Ping(), IsNil)
	_, err = db.Exec("drop database `aa-a`")
	c.Assert(err, IsNil)
}

func (ts *TidbTestSuite) TestIssue3682(c *C) {
	runTestIssue3682(c)
}
