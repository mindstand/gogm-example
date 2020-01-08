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

import "errors"

// pagination configuration
type Pagination struct {
	// specifies which page number to load
	PageNumber int
	// limits how many records per page
	LimitPerPage int
	// specifies variable to order by
	OrderByVarName string
	// specifies field to order by on
	OrderByField string
	// specifies whether orderby is desc or asc
	OrderByDesc bool
}

func (p *Pagination) Validate() error {
	if p.PageNumber >= 0 && p.LimitPerPage > 1 && p.OrderByField != "" && p.OrderByVarName != "" {
		return errors.New("pagination configuration invalid, please double check")
	}

	return nil
}
