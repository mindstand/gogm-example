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
	"reflect"
	"strings"
	"time"
)

// defined the decorator name for struct tag
const decoratorName = "gogm"

// reflect type for go time.Time
var timeType = reflect.TypeOf(time.Time{})

//sub fields of the decorator
const (
	// specifies the name in neo4j
	//requires assignment (if specified)
	paramNameField = "name"

	// specifies the name of the relationship
	//requires assignment (if edge field)
	relationshipNameField = "relationship"

	//specifies direction, can only be (incoming|outgoing|both|none)
	//requires assignment (if edge field)
	directionField = "direction"

	//specifies if the field contains time representation
	timeField = "time"

	//specifies if the field is to be indexed
	indexField = "index"

	//specifies if the field is unique
	uniqueField = "unique"

	//specifies is the field is a primary key
	primaryKeyField = "pk"

	//specifies if the field is map of type `map[string]interface{}`
	propertiesField = "properties"

	//specifies if the field is to be ignored
	ignoreField = "-"

	//specifies deliminator between GoGM tags
	deliminator = ";"

	//assignment operator for GoGM tags
	assignmentOperator = "="
)

//decorator config defines configuration of GoGM field
type decoratorConfig struct {
	// holds reflect type for the field
	Type reflect.Type `json:"-"`
	// holds the name of the field for neo4j
	Name string `json:"name"`
	// holds the name of the field in the struct
	FieldName string `json:"field_name"`
	// holds the name of the relationship
	Relationship string `json:"relationship"`
	// holds the direction
	Direction dsl.Direction `json:"direction"`
	// specifies if field is to be unique
	Unique bool `json:"unique"`
	// specifies if field is to be indexed
	Index bool `json:"index"`
	// specifies if field represents many relationship
	ManyRelationship bool `json:"many_relationship"`
	// uses edge specifies if the edge is a special node
	UsesEdgeNode bool `json:"uses_edge_node"`
	// specifies whether the field is the nodes primary key
	PrimaryKey bool `json:"primary_key"`
	// specify if the field holds properties
	Properties bool `json:"properties"`
	// specifies if the field contains time value
	IsTime bool `json:"is_time"`
	// specifies if the field contains a typedef of another type
	IsTypeDef bool `json:"is_type_def"`
	// holds the reflect type of the root type if typedefed
	TypedefActual reflect.Type `json:"-"`
	// specifies whether to ignore the field
	Ignore bool `json:"ignore"`
}

// Equals checks equality of decorator configs
func (d *decoratorConfig) Equals(comp *decoratorConfig) bool {
	if comp == nil {
		return false
	}

	return d.Name == comp.Name && d.FieldName == comp.FieldName && d.Relationship == comp.Relationship &&
		d.Direction == comp.Direction && d.Unique == comp.Unique && d.Index == comp.Index && d.ManyRelationship == comp.ManyRelationship &&
		d.UsesEdgeNode == comp.UsesEdgeNode && d.PrimaryKey == comp.PrimaryKey && d.Properties == comp.Properties && d.IsTime == comp.IsTime &&
		d.IsTypeDef == comp.IsTypeDef && d.Ignore == comp.Ignore
}

// specifies configuration on GoGM node
type structDecoratorConfig struct {
	// Holds fields -> their configurations
	// field name : decorator configuration
	Fields map[string]decoratorConfig `json:"fields"`
	// holds label for the node, maps to struct name
	Label string `json:"label"`
	// specifies if the node is a vertex or an edge (if true, its a vertex)
	IsVertex bool `json:"is_vertex"`
	// holds the reflect type of the struct
	Type reflect.Type `json:"-"`
}

// Equals checks equality of structDecoratorConfigs
func (s *structDecoratorConfig) Equals(comp *structDecoratorConfig) bool {
	if comp == nil {
		return false
	}

	if comp.Fields != nil && s.Fields != nil {
		for field, decConfig := range s.Fields {
			if compConfig, ok := comp.Fields[field]; ok {
				if !compConfig.Equals(&decConfig) {
					return false
				}
			} else {
				return false
			}
		}
	} else {
		return false
	}

	return s.IsVertex == comp.IsVertex && s.Label == comp.Label
}

// Validate checks if the configuration is valid
func (d *decoratorConfig) Validate() error {
	if d.Ignore {
		if d.Relationship != "" || d.Unique || d.Index || d.ManyRelationship || d.UsesEdgeNode ||
			d.PrimaryKey || d.Properties || d.IsTime || d.Name != d.FieldName {
			log.Println(d)
			return NewInvalidDecoratorConfigError("ignore tag cannot be combined with any other tag", "")
		}

		return nil
	}

	//shouldn't happen, more of a sanity check
	if d.Name == "" {
		return NewInvalidDecoratorConfigError("name must be defined", "")
	}

	kind := d.Type.Kind()

	//check for valid properties
	if kind == reflect.Map || d.Properties {
		if !d.Properties {
			return NewInvalidDecoratorConfigError("properties must be added to gogm config on field with a map", d.Name)
		}

		if kind != reflect.Map || d.Type != reflect.TypeOf(map[string]interface{}{}) {
			return NewInvalidDecoratorConfigError("properties must be a map with signature map[string]interface{}", d.Name)
		}

		if d.PrimaryKey || d.Relationship != "" || d.Direction != 0 || d.Index || d.Unique {
			return NewInvalidDecoratorConfigError("field marked as properties can only have name defined", d.Name)
		}

		//valid properties
		return nil
	}

	//check if type is pointer
	if kind == reflect.Ptr {
		//if it is, get the type of the dereference
		kind = d.Type.Elem().Kind()
	}

	//check valid relationship
	if d.Direction != 0 || d.Relationship != "" || (kind == reflect.Struct && d.Type != timeType) || kind == reflect.Slice {
		if d.Relationship == "" {
			return NewInvalidDecoratorConfigError("relationship has to be defined when creating a relationship", d.FieldName)
		}

		//check empty/undefined direction
		if d.Direction == 0 {
			d.Direction = dsl.DirectionOutgoing //default direction is outgoing
		}

		if kind != reflect.Struct && kind != reflect.Slice {
			return NewInvalidDecoratorConfigError("relationship can only be defined on a struct or a slice", d.Name)
		}

		//check that it isn't defining anything else that shouldn't be defined
		if d.PrimaryKey || d.Properties || d.Index || d.Unique {
			return NewInvalidDecoratorConfigError("can only define relationship, direction and name on a relationship", d.Name)
		}

		// check that name is not defined (should be defaulted to field name)
		if d.Name != d.FieldName {
			return NewInvalidDecoratorConfigError("name tag can not be defined on a relationship (Name and FieldName must be the same)", d.Name)
		}

		//relationship is valid now
		return nil
	}

	//validate timeField
	if d.IsTime {
		if kind != reflect.Int64 && d.Type != timeType {
			return errors.New("can not be a time value and not be either an int64 or time.Time")
		}

		//time is valid
		return nil
	}

	//standard field checks now

	//check pk and index and unique on the same field
	if d.PrimaryKey && (d.Index || d.Unique) {
		return NewInvalidDecoratorConfigError("can not specify Index or Unique on primary key", d.Name)
	}

	if d.Index && d.Unique {
		return NewInvalidDecoratorConfigError("can not specify Index and Unique on the same field", d.Name)
	}

	//validate pk
	if d.PrimaryKey {
		rootKind := d.Type.Kind()

		if rootKind != reflect.String && rootKind != reflect.Int64 {
			return NewInvalidDecoratorConfigError(fmt.Sprintf("invalid type for primary key %s", rootKind.String()), d.Name)
		}

		if rootKind == reflect.String {
			if d.Name != "uuid" {
				return NewInvalidDecoratorConfigError("primary key with type string must be named 'uuid'", d.Name)
			}
		}

		if rootKind == reflect.Int64 {
			if d.Name != "id" {
				return NewInvalidDecoratorConfigError("primary key with type int64 must be named 'id'", d.Name)
			}
		}
	}

	//should be good from here
	return nil
}

var edgeType = reflect.TypeOf(new(IEdge)).Elem()

// newDecoratorConfig generates decorator config for field
// takes in the raw tag, name of the field and reflect type
func newDecoratorConfig(decorator, name string, varType reflect.Type) (*decoratorConfig, error) {
	fields := strings.Split(decorator, deliminator)

	if len(fields) == 0 {
		return nil, errors.New("decorator can not be empty")
	}

	//init bools to false
	toReturn := decoratorConfig{
		Unique:     false,
		PrimaryKey: false,
		Ignore:     false,
		Direction:  0,
		IsTime:     false,
		Type:       varType,
		FieldName:  name,
	}

	for _, field := range fields {

		//if its an assignment, further parsing is needed
		if strings.Contains(field, assignmentOperator) {
			assign := strings.Split(field, assignmentOperator)
			if len(assign) != 2 {
				return nil, errors.New("empty assignment") //todo replace with better error
			}

			key := assign[0]
			val := assign[1]

			switch key {
			case paramNameField:
				toReturn.Name = val
				continue
			case relationshipNameField:
				toReturn.Relationship = val
				if varType.Kind() == reflect.Slice {
					toReturn.ManyRelationship = true
					if varType.Elem().Kind() != reflect.Ptr {
						return nil, errors.New("slice must be of pointer type")
					}
					toReturn.UsesEdgeNode = varType.Elem().Implements(edgeType)
				} else {
					toReturn.ManyRelationship = false
					toReturn.UsesEdgeNode = varType.Implements(edgeType)
				}

				continue
			case directionField:
				dir := strings.ToLower(val)
				switch strings.ToLower(dir) {
				case "incoming":
					toReturn.Direction = dsl.DirectionIncoming
					continue
				case "outgoing":
					toReturn.Direction = dsl.DirectionOutgoing
					continue
				case "none":
					toReturn.Direction = dsl.DirectionNone
					continue
				case "both":
					toReturn.Direction = dsl.DirectionBoth
					continue
				default:
					toReturn.Direction = dsl.DirectionNone
					continue
				}
			default:
				return nil, fmt.Errorf("key '%s' is not recognized", key) //todo replace with better errors
			}
		}

		//simple bool check
		switch field {
		case uniqueField:
			toReturn.Unique = true
			continue
		case primaryKeyField:
			toReturn.PrimaryKey = true
			continue
		case ignoreField:
			toReturn.Ignore = true
			continue
		case propertiesField:
			toReturn.Properties = true
			continue
		case indexField:
			toReturn.Index = true
			continue
		case timeField:
			toReturn.IsTime = true
		default:
			return nil, fmt.Errorf("key '%s' is not recognized", field) //todo replace with better error
		}
	}

	//if its not a relationship, check if the field was typedeffed
	if toReturn.Relationship == "" {
		//check if field is type def
		isTypeDef, newType, err := getActualTypeIfAliased(varType)
		if err != nil {
			return nil, err
		}

		//handle if it is
		if isTypeDef {
			if newType == nil {
				return nil, errors.New("new type can not be nil")
			}

			toReturn.IsTypeDef = true
			toReturn.TypedefActual = newType
		}
	}

	//use var name if name is not set explicitly
	if toReturn.Name == "" {
		toReturn.Name = name
	} else if toReturn.Relationship != "" {
		// check that name is never defined on a relationship
		return nil, errors.New("name tag can not be defined on a relationship")
	}

	//ensure config complies with constraints
	err := toReturn.Validate()
	if err != nil {
		return nil, err
	}

	return &toReturn, nil
}

//validates if struct decorator is valid
func (s *structDecoratorConfig) Validate() error {
	if s.Fields == nil {
		return errors.New("no fields defined")
	}

	pkCount := 0
	rels := 0

	for _, conf := range s.Fields {
		if conf.PrimaryKey {
			pkCount++
		}

		if conf.Relationship != "" {
			rels++
		}
	}

	if pkCount == 0 {
		return NewInvalidStructConfigError("primary key required on node/edge " + s.Label)
	} else if pkCount > 1 {
		return NewInvalidStructConfigError("too many primary keys defined")
	}

	//edge specific check
	if !s.IsVertex {
		if rels > 0 {
			return NewInvalidStructConfigError("relationships can not be defined on edges")
		}
	}

	//good now
	return nil
}

// getStructDecoratorConfig generates structDecoratorConfig for struct
func getStructDecoratorConfig(i interface{}, mappedRelations *relationConfigs) (*structDecoratorConfig, error) {
	toReturn := &structDecoratorConfig{}

	t := reflect.TypeOf(i)

	if t.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("must pass pointer to struct, instead got %T", i)
	}

	t = t.Elem()

	isEdge := false

	//check if its an edge
	if _, ok := i.(IEdge); ok {
		isEdge = true
	}

	toReturn.IsVertex = !isEdge

	toReturn.Label = t.Name()

	toReturn.Type = t

	if t.NumField() == 0 {
		return nil, errors.New("struct has no fields") //todo make error more thorough
	}

	toReturn.Fields = map[string]decoratorConfig{}

	fields := getFields(t)

	if fields == nil || len(fields) == 0 {
		return nil, errors.New("failed to parse fields")
	}

	//iterate through fields and get their configuration
	for _, field := range fields {
		tag := field.Tag.Get(decoratorName)

		if tag != "" {
			config, err := newDecoratorConfig(tag, field.Name, field.Type)
			if err != nil {
				return nil, err
			}

			if config == nil {
				return nil, errors.New("config is nil") //todo better error
			}

			if config.Relationship != "" {
				var endType reflect.Type

				if field.Type.Kind() == reflect.Ptr {
					endType = field.Type.Elem()
				} else if field.Type.Kind() == reflect.Slice {
					temp := field.Type.Elem()
					if strings.Contains(temp.String(), "interface") {
						return nil, fmt.Errorf("relationship field [%s] on type [%s] can not be a slice of generic interface", config.Name, toReturn.Label)
					}
					if temp.Kind() == reflect.Ptr {
						temp = temp.Elem()
					} else {
						return nil, fmt.Errorf("relationship field [%s] on type [%s] must a slice[]*%s", config.Name, toReturn.Label, temp.String())
					}
					endType = temp
				} else {
					endType = field.Type
				}

				endTypeName := ""
				if reflect.PtrTo(endType).Implements(edgeType) {
					log.Info(endType.Name())
					endVal := reflect.New(endType)
					var endTypeVal []reflect.Value

					//log.Info(endVal.String())

					if config.Direction == dsl.DirectionOutgoing {
						endTypeVal = endVal.MethodByName("GetEndNodeType").Call(nil)
					} else {
						endTypeVal = endVal.MethodByName("GetStartNodeType").Call(nil)
					}

					if len(endTypeVal) != 1 {
						return nil, errors.New("GetEndNodeType failed")
					}

					if endTypeVal[0].IsNil() {
						return nil, errors.New("GetEndNodeType() can not return a nil value")
					}

					convertedType, ok := endTypeVal[0].Interface().(reflect.Type)
					if !ok {
						return nil, errors.New("cannot convert to type reflect.Type")
					}

					if convertedType.Kind() == reflect.Ptr {
						endTypeName = convertedType.Elem().Name()
					} else {
						endTypeName = convertedType.Name()
					}
				} else {
					endTypeName = endType.Name()
				}

				mappedRelations.Add(toReturn.Label, config.Relationship, endTypeName, *config)
			}

			toReturn.Fields[field.Name] = *config
		}
	}

	err := toReturn.Validate()
	if err != nil {
		return nil, err
	}

	return toReturn, nil
}

// getFields gets all fields in a struct, specifically also gets fields from embedded structs
func getFields(val reflect.Type) []*reflect.StructField {
	var fields []*reflect.StructField
	if val.Kind() == reflect.Ptr {
		return getFields(val.Elem())
	}

	for i := 0; i < val.NumField(); i++ {
		tempField := val.Field(i)
		if tempField.Anonymous && tempField.Type.Kind() == reflect.Struct {
			fields = append(fields, getFields(tempField.Type)...)
		} else {
			fields = append(fields, &tempField)
		}
	}

	return fields
}
