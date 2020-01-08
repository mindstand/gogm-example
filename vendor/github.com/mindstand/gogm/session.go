// Copyright (c) 2019 MindStand Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package gogm

import (
	"errors"
	"fmt"
	dsl "github.com/mindstand/go-cypherdsl"
	driver "github.com/mindstand/golang-neo4j-bolt-driver"
	"reflect"
)

const defaultDepth = 1

type Session struct {
	conn         *driver.BoltConn
	tx           driver.Tx
	DefaultDepth int
	LoadStrategy LoadStrategy
}

func NewSession(readonly bool) (*Session, error) {
	if driverPool == nil {
		return nil, errors.New("driverPool cannot be nil")
	}

	session := new(Session)

	var mode driver.DriverMode

	if readonly {
		mode = driver.ReadOnlyMode
	} else {
		mode = driver.ReadWriteMode
	}

	conn, err := driverPool.Open(mode)
	if err != nil {
		return nil, err
	}

	session.conn = conn

	session.DefaultDepth = defaultDepth

	return session, nil
}

func (s *Session) Begin() error {
	if s.conn == nil {
		return errors.New("neo4j connection not initialized")
	}

	if s.tx != nil {
		return fmt.Errorf("transaction already started: %w", ErrTransaction)
	}

	var err error

	s.tx, err = s.conn.Begin()
	if err != nil {
		return err
	}

	return nil
}

func (s *Session) Rollback() error {
	if s.conn == nil {
		return errors.New("neo4j connection not initialized")
	}

	if s.tx == nil {
		return fmt.Errorf("cannot Rollback nil transaction: %w", ErrTransaction)
	}

	err := s.tx.Rollback()
	if err != nil {
		return err
	}

	s.tx = nil
	return nil
}

func (s *Session) RollbackWithError(originalError error) error {
	err := s.Rollback()
	if err != nil {
		return fmt.Errorf("original error: `%s`, rollback error: `%s`", originalError.Error(), err.Error())
	}

	return originalError
}

func (s *Session) Commit() error {
	if s.conn == nil {
		return errors.New("neo4j connection not initialized")
	}

	if s.tx == nil {
		return fmt.Errorf("cannot commit nil transaction: %w", ErrTransaction)
	}

	err := s.tx.Commit()
	if err != nil {
		return err
	}

	s.tx = nil
	return nil
}

func (s *Session) Load(respObj interface{}, id string) error {
	return s.LoadDepthFilterPagination(respObj, id, s.DefaultDepth, nil, nil, nil)
}

func (s *Session) LoadDepth(respObj interface{}, id string, depth int) error {
	return s.LoadDepthFilterPagination(respObj, id, depth, nil, nil, nil)
}

func (s *Session) LoadDepthFilter(respObj interface{}, id string, depth int, filter *dsl.ConditionBuilder, params map[string]interface{}) error {
	return s.LoadDepthFilterPagination(respObj, id, depth, filter, params, nil)
}

func (s *Session) LoadDepthFilterPagination(respObj interface{}, id string, depth int, filter dsl.ConditionOperator, params map[string]interface{}, pagination *Pagination) error {
	respType := reflect.TypeOf(respObj)

	//validate type is ptr
	if respType.Kind() != reflect.Ptr {
		return errors.New("respObj must be type ptr")
	}

	//"deref" reflect interface type
	respType = respType.Elem()

	//get the type name -- this maps directly to the label
	respObjName := respType.Name()

	//will need to keep track of these variables
	varName := "n"

	var query dsl.Cypher
	var err error

	//make the query based off of the load strategy
	switch s.LoadStrategy {
	case PATH_LOAD_STRATEGY:
		query, err = PathLoadStrategyOne(varName, respObjName, depth, filter)
		if err != nil {
			return err
		}
	case SCHEMA_LOAD_STRATEGY:
		return errors.New("schema load strategy not supported yet")
	default:
		return errors.New("unknown load strategy")
	}

	//if the query requires pagination, set that up
	if pagination != nil {
		err := pagination.Validate()
		if err != nil {
			return err
		}

		query = query.
			OrderBy(dsl.OrderByConfig{
				Name:   pagination.OrderByVarName,
				Member: pagination.OrderByField,
				Desc:   pagination.OrderByDesc,
			}).
			Skip(pagination.LimitPerPage * pagination.PageNumber).
			Limit(pagination.LimitPerPage)
	}

	if params == nil {
		params = map[string]interface{}{
			"uuid": id,
		}
	} else {
		params["uuid"] = id
	}

	rows, err := query.WithNeo(s.conn).Query(params)
	if err != nil {
		return err
	}

	return decodeNeoRows(rows, respObj)
}

func (s *Session) LoadAll(respObj interface{}) error {
	return s.LoadAllDepthFilterPagination(respObj, s.DefaultDepth, nil, nil, nil)
}

func (s *Session) LoadAllDepth(respObj interface{}, depth int) error {
	return s.LoadAllDepthFilterPagination(respObj, depth, nil, nil, nil)
}

func (s *Session) LoadAllDepthFilter(respObj interface{}, depth int, filter dsl.ConditionOperator, params map[string]interface{}) error {
	return s.LoadAllDepthFilterPagination(respObj, depth, filter, params, nil)
}

func (s *Session) LoadAllDepthFilterPagination(respObj interface{}, depth int, filter dsl.ConditionOperator, params map[string]interface{}, pagination *Pagination) error {
	rawRespType := reflect.TypeOf(respObj)

	if rawRespType.Kind() != reflect.Ptr {
		return fmt.Errorf("respObj must be a pointer to a slice, instead it is %T", respObj)
	}

	//deref to a slice
	respType := rawRespType.Elem()

	//validate type is ptr
	if respType.Kind() != reflect.Slice {
		return fmt.Errorf("respObj must be type slice, instead it is %T", respObj)
	}

	//"deref" reflect interface type
	respType = respType.Elem()

	if respType.Kind() == reflect.Ptr {
		//slice of pointers
		respType = respType.Elem()
	}

	//get the type name -- this maps directly to the label
	respObjName := respType.Name()

	//will need to keep track of these variables
	varName := "n"

	var query dsl.Cypher
	var err error

	//make the query based off of the load strategy
	switch s.LoadStrategy {
	case PATH_LOAD_STRATEGY:
		query, err = PathLoadStrategyMany(varName, respObjName, depth, filter)
		if err != nil {
			return err
		}
	case SCHEMA_LOAD_STRATEGY:
		return errors.New("schema load strategy not supported yet")
	default:
		return errors.New("unknown load strategy")
	}

	//if the query requires pagination, set that up
	if pagination != nil {
		err := pagination.Validate()
		if err != nil {
			return err
		}

		query = query.
			OrderBy(dsl.OrderByConfig{
				Name:   pagination.OrderByVarName,
				Member: pagination.OrderByField,
				Desc:   pagination.OrderByDesc,
			}).
			Skip(pagination.LimitPerPage * pagination.PageNumber).
			Limit(pagination.LimitPerPage)
	}

	rows, err := query.WithNeo(s.conn).Query(params)
	if err != nil {
		return err
	}

	return decodeNeoRows(rows, respObj)
}

func (s *Session) LoadAllEdgeConstraint(respObj interface{}, endNodeType, endNodeField string, edgeConstraint interface{}, minJumps, maxJumps, depth int, filter dsl.ConditionOperator) error {
	rawRespType := reflect.TypeOf(respObj)

	if rawRespType.Kind() != reflect.Ptr {
		return fmt.Errorf("respObj must be a pointer to a slice, instead it is %T", respObj)
	}

	//deref to a slice
	respType := rawRespType.Elem()

	//validate type is ptr
	if respType.Kind() != reflect.Slice {
		return fmt.Errorf("respObj must be type slice, instead it is %T", respObj)
	}

	//"deref" reflect interface type
	respType = respType.Elem()

	if respType.Kind() == reflect.Ptr {
		//slice of pointers
		respType = respType.Elem()
	}

	//get the type name -- this maps directly to the label
	respObjName := respType.Name()

	//will need to keep track of these variables
	varName := "n"

	var query dsl.Cypher
	var err error

	//make the query based off of the load strategy
	switch s.LoadStrategy {
	case PATH_LOAD_STRATEGY:
		query, err = PathLoadStrategyEdgeConstraint(varName, respObjName, endNodeType, endNodeField, minJumps, maxJumps, depth, filter)
		if err != nil {
			return err
		}
	case SCHEMA_LOAD_STRATEGY:
		return errors.New("schema load strategy not supported yet")
	default:
		return errors.New("unknown load strategy")
	}

	rows, err := query.WithNeo(s.conn).Query(map[string]interface{}{
		endNodeField: edgeConstraint,
	})
	if err != nil {
		return err
	}

	return decodeNeoRows(rows, respObj)
}

func (s *Session) Save(saveObj interface{}) error {
	return s.SaveDepth(saveObj, s.DefaultDepth)
}

func (s *Session) SaveDepth(saveObj interface{}, depth int) error {
	if s.conn == nil {
		return errors.New("neo4j connection not initialized")
	}

	return saveDepth(s.conn, saveObj, depth)
}

func (s *Session) Delete(deleteObj interface{}) error {
	if s.conn == nil {
		return errors.New("neo4j connection not initialized")
	}

	if deleteObj == nil {
		return errors.New("deleteObj can not be nil")
	}

	return deleteNode(s.conn, deleteObj)
}

func (s *Session) DeleteUUID(uuid string) error {
	if s.conn == nil {
		return errors.New("neo4j connection not initialized")
	}

	return deleteByUuids(s.conn, uuid)
}

func (s *Session) Query(query string, properties map[string]interface{}, respObj interface{}) error {
	if s.conn == nil {
		return errors.New("neo4j connection not initialized")
	}

	rows, err := dsl.QB().Cypher(query).WithNeo(s.conn).Query(properties)
	if err != nil {
		return err
	}

	return decodeNeoRows(rows, respObj)
}

func (s *Session) QueryRaw(query string, properties map[string]interface{}) ([][]interface{}, error) {
	if s.conn == nil {
		return nil, errors.New("neo4j connection not initialized")
	}

	rows, err := dsl.QB().Cypher(query).WithNeo(s.conn).Query(properties)
	if err != nil {
		return nil, err
	}

	data, _, err := rows.All()
	if err != nil {
		return nil, err
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (s *Session) PurgeDatabase() error {
	if s.conn == nil {
		return errors.New("neo4j connection not initialized")
	}

	_, err := dsl.QB().Match(dsl.Path().V(dsl.V{Name: "n"}).Build()).Delete(true, "n").WithNeo(s.conn).Exec(nil)
	return err
}

func (s *Session) Close() error {
	if s.conn == nil {
		return errors.New("neo4j connection not initialized")
	}

	if s.conn == nil {
		return fmt.Errorf("cannot close nil connection: %w", ErrInternal)
	}

	if s.tx != nil {
		log.Warn("attempting to close a session with a pending transaction")
		return fmt.Errorf("cannot close a session with a pending transaction: %w", ErrTransaction)
	}

	err := driverPool.Reclaim(s.conn)
	if err != nil {
		return err
	}

	s.conn = nil

	return nil
}
