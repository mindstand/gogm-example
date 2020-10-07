package main

import (
	"fmt"
	"github.com/mindstand/gogm"
	"reflect"
	"time"
)

//nodes
type Department struct {
	gogm.BaseNode

	Name string `gogm:"name=name"`

	Subjects []*Subject `gogm:"direction=outgoing;relationship=CURRICULUM"`
	Teachers []*Teacher `gogm:"direction=incoming;relationship=FOR_DEPARTMENT"`
}

type Subject struct {
	gogm.BaseNode

	Name string `gogm:"name=name"`

	Department *Department `gogm:"direction=incoming;relationship=CURRICULUM"`
	Teachers   []*Teacher  `gogm:"direction=outgoing;relationship=TAUGHT_BY"`
	Courses    []*Course   `gogm:"direction=incoming;relationship=SUBJECT_TAUGHT"`
}

type Teacher struct {
	gogm.BaseNode

	Name string `gogm:"name=name;unique"`

	Courses    []*Course   `gogm:"direction=outgoing;relationship=TEACHES_CLASS"`
	Subjects   []*Subject  `gogm:"direction=incoming;relationship=TAUGHT_BY"`
	Department *Department `gogm:"direction=outgoing;relationship=FOR_DEPARTMENT"`
}

type Course struct {
	gogm.BaseNode

	Name string `gogm:"name=name"`

	Subject     *Subject      `gogm:"direction=outgoing;relationship=SUBJECT_TAUGHT"`
	Teacher     *Teacher      `gogm:"direction=incoming;relationship=TEACHES_CLASS"`
	Enrollments []*Enrollment `gogm:"direction=incoming;relationship=ENROLLED"`
}

type Student struct {
	gogm.BaseNode

	Name   string                 `gogm:"name=name;unique"`
	Grades map[string]interface{} `gogm:"name=grades;properties"`

	Enrollments []*Enrollment `gogm:"direction=outgoing;relationship=ENROLLED"`
}

//edges
type Enrollment struct {
	gogm.BaseNode

	Start *Student
	End   *Course

	EnrolledDate time.Time `gogm:"name=enrolled_date;time"`
}

func (e *Enrollment) GetStartNode() interface{} {
	return e.Start
}

func (e *Enrollment) GetStartNodeType() reflect.Type {
	return reflect.TypeOf(&Student{})
}

func (e *Enrollment) SetStartNode(v interface{}) error {
	student, ok := v.(*Student)
	if !ok {
		return fmt.Errorf("unable to convert to [*Student] from [%T]", v)
	}

	e.Start = student
	return nil
}

func (e *Enrollment) GetEndNode() interface{} {
	return e.End
}

func (e *Enrollment) GetEndNodeType() reflect.Type {
	return reflect.TypeOf(&Course{})
}

func (e *Enrollment) SetEndNode(v interface{}) error {
	course, ok := v.(*Course)
	if !ok {
		return fmt.Errorf("unable to convert to [*Course] from [%T]", v)
	}

	e.End = course
	return nil
}
