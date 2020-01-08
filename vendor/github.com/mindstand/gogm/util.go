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
	"github.com/google/uuid"
	go_cypherdsl "github.com/mindstand/go-cypherdsl"
	"reflect"
	"strings"
	"sync"
	"time"
)

// checks if integer is in slice
func int64SliceContains(s []int64, e int64) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// checks if string is in slice
func stringSliceContains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// sets uuid for stuct if uuid field is empty
func handleNodeState(val *reflect.Value, fieldName string) (bool, string, map[string]*RelationConfig, error) {
	if val == nil {
		return false, "", nil, errors.New("value can not be nil")
	}

	if reflect.TypeOf(*val).Kind() == reflect.Ptr {
		*val = val.Elem()
	}

	checkUuid := reflect.Indirect(*val).FieldByName(fieldName).String()

	loadVal := reflect.Indirect(*val).FieldByName("LoadMap")

	iConf := loadVal.Interface()

	if iConf != nil && loadVal.Len() != 0 && checkUuid != "" {
		// node is not new
		relConf, ok := iConf.(map[string]*RelationConfig)
		if !ok {
			return false, "", nil, fmt.Errorf("unable to cast conf to [map[string]*RelationConfig], %w", ErrInternal)
		}

		return false, checkUuid, relConf, nil
	} else {
		// definitely new
		var newUuid string

		if checkUuid == "" {
			newUuid = uuid.New().String()
		} else {
			newUuid = checkUuid
		}

		reflect.Indirect(*val).FieldByName(fieldName).Set(reflect.ValueOf(newUuid))

		return true, newUuid, map[string]*RelationConfig{}, nil

	}
}

// gets the type name from reflect type
func getTypeName(val reflect.Type) (string, error) {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() == reflect.Slice {
		val = val.Elem()
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
	}

	if val.Kind() == reflect.Struct {
		return val.Name(), nil
	} else {
		return "", fmt.Errorf("can not take name from kind {%s)", val.Kind().String())
	}
}

// converts struct fields to map that cypher can use
func toCypherParamsMap(val reflect.Value, config structDecoratorConfig) (map[string]interface{}, error) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	if val.Type().Kind() == reflect.Interface || val.Type().Kind() == reflect.Ptr {
		val = val.Elem()
	}

	ret := map[string]interface{}{}

	for _, conf := range config.Fields {
		if conf.Relationship != "" || conf.Name == "id" || conf.Ignore {
			continue
		}

		if conf.IsTime {
			if conf.Type.Kind() == reflect.Int64 {
				ret[conf.Name] = val.FieldByName(conf.FieldName).Interface()
			} else {
				dateInterface := val.FieldByName(conf.FieldName).Interface()

				dateObj, ok := dateInterface.(time.Time)
				if !ok {
					return nil, errors.New("cant convert date to time.Time")
				}

				ret[conf.Name] = dateObj.Format(time.RFC3339)
			}
		} else if conf.Properties {
			//check if field is a map
			if conf.Type.Kind() == reflect.Map {
				//try to cast it
				propsMap, ok := val.FieldByName(conf.FieldName).Interface().(map[string]interface{})
				if ok {
					//if it works, create the fields
					for k, v := range propsMap {
						ret[conf.Name+"."+k] = v
					}
				} else {
					return nil, errors.New("unable to convert map to map[string]interface{}")
				}
			} else {
				return nil, errors.New("properties type is not a map")
			}
		} else {
			//check if field is type aliased
			if conf.IsTypeDef {
				ret[conf.Name] = val.FieldByName(conf.FieldName).Convert(conf.TypedefActual).Interface()
			} else {
				ret[conf.Name] = val.FieldByName(conf.FieldName).Interface()
			}
		}
	}

	return ret, err
}

type relationConfigs struct {
	// [type-relationship][fieldType][]decoratorConfig
	configs map[string]map[string][]decoratorConfig

	mutex sync.Mutex
}

func (r *relationConfigs) getKey(nodeType, relationship string) string {
	return fmt.Sprintf("%s-%s", nodeType, relationship)
}

func (r *relationConfigs) Add(nodeType, relationship, fieldType string, dec decoratorConfig) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.configs == nil {
		r.configs = map[string]map[string][]decoratorConfig{}
	}

	key := r.getKey(nodeType, relationship)

	if _, ok := r.configs[key]; !ok {
		r.configs[key] = map[string][]decoratorConfig{}
	}

	if _, ok := r.configs[key][fieldType]; !ok {
		r.configs[key][fieldType] = []decoratorConfig{}
	}

	log.Infof("mapped relations [%s][%s][%v]", key, fieldType, len(r.configs[key][fieldType]))

	r.configs[key][fieldType] = append(r.configs[key][fieldType], dec)
}

func (r *relationConfigs) GetConfigs(startNodeType, startNodeFieldType, endNodeType, endNodeFieldType, relationship string) (start, end *decoratorConfig, err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.configs == nil {
		return nil, nil, errors.New("no configs provided")
	}

	start, err = r.getConfig(startNodeType, relationship, startNodeFieldType, go_cypherdsl.DirectionOutgoing)
	if err != nil {
		return nil, nil, err
	}

	end, err = r.getConfig(endNodeType, relationship, endNodeFieldType, go_cypherdsl.DirectionIncoming)
	if err != nil {
		return nil, nil, err
	}

	return start, end, nil
}

func (r *relationConfigs) getConfig(nodeType, relationship, fieldType string, direction go_cypherdsl.Direction) (*decoratorConfig, error) {
	if r.configs == nil {
		return nil, errors.New("no configs provided")
	}

	key := r.getKey(nodeType, relationship)

	if _, ok := r.configs[key]; !ok {
		return nil, fmt.Errorf("no configs for key [%s]", key)
	}

	var ok bool
	var confs []decoratorConfig

	if confs, ok = r.configs[key][fieldType]; !ok {
		return nil, fmt.Errorf("no configs for key [%s] and field type [%s]", key, fieldType)
	}

	if len(confs) == 1 {
		return &confs[0], nil
	} else if len(confs) > 1 {
		for _, c := range confs {
			if c.Direction == direction {
				return &c, nil
			}
		}
		return nil, errors.New("relation with correct direction not found")
	} else {
		return nil, fmt.Errorf("config not found, %w", ErrInternal)
	}
}

type validation struct {
	Incoming []string
	Outgoing []string
	None     []string
	Both     []string
}

func (r *relationConfigs) Validate() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	checkMap := map[string]*validation{}

	for title, confMap := range r.configs {
		parts := strings.Split(title, "-")
		if len(parts) != 2 {
			return fmt.Errorf("invalid length for parts [%v] should be 2. Rel is [%s], %w", len(parts), title, ErrValidation)
		}

		//vType := parts[0]
		relType := parts[1]

		for field, configs := range confMap {
			for _, config := range configs {
				if _, ok := checkMap[relType]; !ok {
					checkMap[relType] = &validation{
						Incoming: []string{},
						Outgoing: []string{},
						None:     []string{},
						Both:     []string{},
					}
				}

				validate := checkMap[relType]

				switch config.Direction {
				case go_cypherdsl.DirectionIncoming:
					validate.Incoming = append(validate.Incoming, field)
					break
				case go_cypherdsl.DirectionOutgoing:
					validate.Outgoing = append(validate.Outgoing, field)
					break
				case go_cypherdsl.DirectionNone:
					validate.None = append(validate.None, field)
					break
				case go_cypherdsl.DirectionBoth:
					validate.Both = append(validate.Both, field)
					break
				default:
					return fmt.Errorf("unrecognized direction [%s], %w", config.Direction.ToString(), ErrValidation)
				}
			}
		}
	}

	for relType, validateConfig := range checkMap {
		//check normal
		if len(validateConfig.Outgoing) != len(validateConfig.Incoming) {
			return fmt.Errorf("invalid directional configuration on relationship [%s], %w", relType, ErrValidation)
		}

		//check both direction
		if len(validateConfig.Both) != 0 {
			if len(validateConfig.Both)%2 != 0 {
				return fmt.Errorf("invalid length for 'both' validation, %w", ErrValidation)
			}
		}

		//check none direction
		if len(validateConfig.None) != 0 {
			if len(validateConfig.None)%2 != 0 {
				return fmt.Errorf("invalid length for 'both' validation, %w", ErrValidation)
			}
		}
	}
	return nil
}

//isDifferentType, differentType, error
func getActualTypeIfAliased(iType reflect.Type) (bool, reflect.Type, error) {
	if iType == nil {
		return false, nil, errors.New("iType can not be nil")
	}

	if iType.Kind() == reflect.Ptr {
		iType = iType.Elem()
	}

	//check if its a struct or an interface, we can skip that
	if iType.Kind() == reflect.Struct || iType.Kind() == reflect.Interface || iType.Kind() == reflect.Slice || iType.Kind() == reflect.Map {
		return false, nil, nil
	}

	//type is the same as the kind
	if iType.Kind().String() == iType.Name() {
		return false, nil, nil
	}

	actualType, err := getPrimitiveType(iType.Kind())
	if err != nil {
		return false, nil, err
	}

	return true, actualType, nil
}

func getPrimitiveType(k reflect.Kind) (reflect.Type, error) {
	switch k {
	case reflect.Int:
		return reflect.TypeOf(0), nil
	case reflect.Int64:
		return reflect.TypeOf(int64(0)), nil
	case reflect.Int32:
		return reflect.TypeOf(int32(0)), nil
	case reflect.Int16:
		return reflect.TypeOf(int16(0)), nil
	case reflect.Int8:
		return reflect.TypeOf(int8(0)), nil
	case reflect.Uint64:
		return reflect.TypeOf(uint64(0)), nil
	case reflect.Uint32:
		return reflect.TypeOf(uint32(0)), nil
	case reflect.Uint16:
		return reflect.TypeOf(uint16(0)), nil
	case reflect.Uint8:
		return reflect.TypeOf(uint8(0)), nil
	case reflect.Uint:
		return reflect.TypeOf(uint(0)), nil
	case reflect.Bool:
		return reflect.TypeOf(false), nil
	case reflect.Float64:
		return reflect.TypeOf(float64(0)), nil
	case reflect.Float32:
		return reflect.TypeOf(float32(0)), nil
	case reflect.String:
		return reflect.TypeOf(""), nil
	default:
		return nil, fmt.Errorf("[%s] not supported", k.String())
	}
}
