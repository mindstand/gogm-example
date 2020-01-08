package go_cypherdsl

import (
	"errors"
	"fmt"
	neo "github.com/mindstand/golang-neo4j-bolt-driver"
	"strings"
)

type stmt struct {
	Query  string
	Params map[string]interface{}
}

type QueryBuilder struct {
	Start   *queryPartNode
	Current *queryPartNode
	errors  []error

	preparedStatements []stmt

	conn *neo.BoltConn
}

func QB() *QueryBuilder {
	return &QueryBuilder{}
}

func (q *QueryBuilder) addNext(s string) {
	node := &queryPartNode{
		Part: s,
	}

	if q.Start == nil {
		q.Start = node
		q.Current = node
	} else {
		q.Current.Next = node
		q.Current = node
	}
}

func (q *QueryBuilder) addError(err error) {
	if q.errors == nil {
		q.errors = []error{}
	}

	q.errors = append(q.errors, err)
}

func (q *QueryBuilder) hasErrors() bool {
	return q.errors != nil && len(q.errors) > 0
}

type queryPartNode struct {
	Part string
	Next *queryPartNode
}

func (q *QueryBuilder) Match(p *PathBuilder) Cypher {
	if p == nil {
		q.addError(errors.New("path can not be nil"))
		return q
	}

	query, err := p.ToCypher()
	if err != nil {
		q.addError(err)
		return q
	}

	q.addNext("MATCH " + query)
	return q
}

func (q *QueryBuilder) OptionalMatch(p *PathBuilder) Cypher {
	if p == nil {
		q.addError(errors.New("path can not be nil"))
		return q
	}

	query, err := p.ToCypher()
	if err != nil {
		q.addError(err)
		return q
	}

	q.addNext("OPTIONAL MATCH " + query)
	return q
}

func (q *QueryBuilder) Create(c CreateQuery, err error) Cypher {
	if err != nil {
		q.addError(err)
		return q
	}

	q.addNext("CREATE " + string(c))
	return q
}

func (q *QueryBuilder) Where(cb ConditionOperator) Cypher {
	if cb == nil {
		q.addError(errors.New("condition builder can not be nil"))
		return q
	}

	w, err := cb.Build()
	if err != nil {
		q.addError(err)
		return q
	}

	q.addNext("WHERE " + string(w))
	return q
}

func (q *QueryBuilder) Merge(mergeConf *MergeConfig) Cypher {
	if mergeConf == nil {
		q.addError(errors.New("mergeConf can not be nil"))
		return q
	}
	cypher, err := mergeConf.ToString()
	if err != nil {
		q.addError(err)
		return q
	}

	q.addNext("MERGE " + cypher)

	return q
}

func (q *QueryBuilder) Return(distinct bool, parts ...ReturnPart) Cypher {
	str, err := NewReturnClause(distinct, parts...)
	if err != nil {
		q.addError(err)
		return q
	}

	q.addNext(string(str))
	return q
}

func (q *QueryBuilder) Delete(detach bool, params ...string) Cypher {
	cypher, err := deleteToString(detach, params...)
	if err != nil {
		q.addError(err)
		return q
	}

	q.addNext(cypher)
	return q
}

func (q *QueryBuilder) Set(sets ...SetConfig) Cypher {
	if len(sets) == 0 {
		q.addError(errors.New("sets can not be empty"))
		return q
	}

	query := "SET"

	for _, setStmt := range sets {
		str, err := setStmt.ToString()
		if err != nil {
			q.addError(err)
			return q
		}

		query += fmt.Sprintf(" %s,", str)
	}

	q.addNext(strings.TrimSuffix(query, ","))
	return q
}

func (q *QueryBuilder) Remove(removes ...RemoveConfig) Cypher {
	if len(removes) == 0 {
		q.addError(errors.New("removes can not be empty"))
	}

	query := "REMOVE"

	for _, remove := range removes {
		str, err := remove.ToString()
		if err != nil {
			q.addError(err)
			return q
		}
		query += fmt.Sprintf(" %s,", str)
	}

	q.addNext(strings.TrimSuffix(query, ","))
	return q
}

func (q *QueryBuilder) OrderBy(orderBys ...OrderByConfig) Cypher {
	if len(orderBys) == 0 {
		q.addError(errors.New("removes can not be empty"))
	}

	query := "ORDER BY"

	for _, orders := range orderBys {
		str, err := orders.ToString()
		if err != nil {
			q.addError(err)
			return q
		}
		query += fmt.Sprintf(" %s,", str)
	}

	q.addNext(strings.TrimSuffix(query, ","))
	return q
}

func (q *QueryBuilder) Limit(num int) Cypher {
	q.addNext(fmt.Sprintf("LIMIT %v", num))
	return q
}

func (q *QueryBuilder) Skip(num int) Cypher {
	q.addNext(fmt.Sprintf("SKIP %v", num))
	return q
}

func (q *QueryBuilder) With(conf *WithConfig) Cypher {
	if conf == nil {
		q.addError(errors.New("conf can not be nil on With"))
		return q
	}

	str, err := conf.ToString()
	if err != nil {
		q.addError(err)
		return q
	}

	q.addNext(fmt.Sprintf("WITH %s", str))
	return q
}

func (q *QueryBuilder) Unwind(unwind *UnwindConfig) Cypher {
	if unwind == nil {
		q.addError(errors.New("unwind config cannot be nil"))
		return q
	}

	str, err := unwind.ToString()
	if err != nil {
		q.addError(err)
		return q
	}

	q.addNext(fmt.Sprintf("UNWIND %s", str))
	return q
}

func (q *QueryBuilder) Union(all bool) Cypher {
	query := "UNION"

	if all {
		query += " ALL"
	}

	q.addNext(query)
	return q
}

func (q *QueryBuilder) Cypher(c string) Cypher {
	q.addNext(c)
	return q
}

func (q *QueryBuilder) WithNeo(conn *neo.BoltConn) Cypher {
	if conn == nil {
		q.addError(errors.New("connection can not be nil"))
		return q
	}

	q.conn = conn

	return q
}

func (q *QueryBuilder) Query(params map[string]interface{}) (neo.Rows, error) {
	if q.conn == nil {
		return nil, errors.New("connection not specified")
	}

	query, err := q.build()
	if err != nil {
		return nil, err
	}

	//init map to empty if its nil
	if params == nil {
		params = map[string]interface{}{}
	}

	log.Debugf("Executing '%s' with params '%v'", query, params)

	return q.conn.QueryNeo(query, params)
}

func (q *QueryBuilder) Exec(params map[string]interface{}) (neo.Result, error) {
	if q.conn == nil {
		return nil, errors.New("connection not specified")
	}

	query, err := q.build()
	if err != nil {
		return nil, err
	}

	//init map to empty if its nil
	if params == nil {
		params = map[string]interface{}{}
	}

	log.Debugf("Executing '%s' with params '%v'", query, params)

	return q.conn.ExecNeo(query, params)
}

func (q *QueryBuilder) ToCypher() (string, error) {
	return q.build()
}

func (q *QueryBuilder) build() (string, error) {
	//fail if errors are found
	if q.hasErrors() {
		str := "errors found: "
		for _, err := range q.errors {
			str += err.Error() + ";"
		}

		str = strings.TrimSuffix(str, ";") + fmt.Sprintf(" -- total errors (%v)", len(q.errors))
		return "", errors.New(str)
	}

	if q.Start == nil || q.Current == nil {
		return "", errors.New("no nodes were added")
	}

	query := ""

	cur := q.Start

	for {
		if cur == nil {
			break
		}

		query += cur.Part + " "

		cur = cur.Next
	}

	return strings.TrimSuffix(query, " "), nil
}

func (q *QueryBuilder) AddToPreparedStatement(params map[string]interface{}) error {
	query, err := q.build()
	if err != nil {
		return err
	}

	if q.preparedStatements == nil {
		q.preparedStatements = []stmt{}
	}

	q.preparedStatements = append(q.preparedStatements, stmt{
		Query:  query,
		Params: params,
	})

	return nil
}

func (q *QueryBuilder) ExecutePreparedStatements() ([]neo.Result, error) {
	if q.preparedStatements == nil || len(q.preparedStatements) == 0 {
		return nil, errors.New("no statements are prepared")
	}

	var queries []string
	var params []map[string]interface{}

	for _, statement := range q.preparedStatements {
		queries = append(queries, statement.Query)
		params = append(params, statement.Params)
	}

	return q.conn.ExecPipeline(queries, params...)
}
