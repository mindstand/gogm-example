package main

import (
	"github.com/mindstand/gogm"
	"log"
	"time"
)

func main() {
	conf := gogm.Config{
		Host:      "0.0.0.0",
		Port:      7687,
		IsCluster: false,
		// IsCluster: true, // if we're trying to connect to a casual cluster
		Username:      "neo4j",
		Password:      "password",
		PoolSize:      50,
		IndexStrategy: gogm.ASSERT_INDEX,
	}

	// must register each node, including edges in gogm.Init(). Also note you must pass the pointer
	err := gogm.Init(&conf, &Department{}, &Subject{}, &Teacher{}, &Course{}, &Student{}, &Enrollment{})
	if err != nil {
		log.Fatal(err)
	}

	// create some teachers
	crosby, shully, elias, oates := &Teacher{Name: "Crosby"}, &Teacher{Name: "Shully"}, &Teacher{Name: "Elias"}, &Teacher{Name: "Oates"}

	// create some departments
	compsci, history, physics := &Department{Name: "Compsci"}, &Department{Name: "History"}, &Department{Name: "Physics"}

	// create some subjects
	dataStructures, modernHistory, hardPhysics := &Subject{Name: "dataStructures"}, &Subject{Name: "modernHistory"}, &Subject{Name: "hardPhysics"}

	// create some courses
	cs341_0, cs341_1, hist347, phys122 := &Course{Name: "cs341_0"}, &Course{Name: "cs341_1"}, &Course{Name: "hist347"}, &Course{Name: "phys122"}

	// create a few students
	eric, steven, michael, nikita := &Student{Name: "eric"}, &Student{Name: "steven"}, &Student{Name: "michael"}, &Student{Name: "nikita"}

	// lets assign the teacher to their departments using our generated functions
	// ignoring the errors here for demo purposes
	crosby.LinkToDepartmentOnFieldDepartment(history)
	shully.LinkToDepartmentOnFieldDepartment(physics)
	compsci.LinkToTeacherOnFieldTeachers(elias, oates)

	//lets assign subjects to their departments
	compsci.LinkToSubjectOnFieldSubjects(dataStructures)
	history.LinkToSubjectOnFieldSubjects(modernHistory)
	physics.LinkToSubjectOnFieldSubjects(hardPhysics)

	// now lets link courses to their subjects and teachers
	cs341_0.LinkToTeacherOnFieldTeacher(oates)
	cs341_0.LinkToSubjectOnFieldSubject(dataStructures)

	cs341_1.LinkToTeacherOnFieldTeacher(elias)
	cs341_1.LinkToSubjectOnFieldSubject(dataStructures)

	hist347.LinkToTeacherOnFieldTeacher(crosby)
	hist347.LinkToSubjectOnFieldSubject(modernHistory)

	phys122.LinkToTeacherOnFieldTeacher(shully)
	phys122.LinkToSubjectOnFieldSubject(hardPhysics)

	// lets save and visualize what we have now
	// create a session
	// setting to false since we're not doing readonly, this is more important for casual clusters than single node clusters
	sess, err := gogm.NewSession(false)
	if err != nil {
		log.Fatal(err)
	}

	defer sess.Close()

	// create transaction for saving this
	err = sess.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// also note we're passing in pointers to save depth
	// saving depth of 2 to connect everything correctly
	err = sess.SaveDepth(compsci, 2)
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	err = sess.SaveDepth(history, 2)
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	err = sess.SaveDepth(physics, 2)
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	err = sess.Commit()
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	// now we have all of the teachers, classes, departments and subjects saved.
	// lets assign students to their classes

	eric.LinkToCourseOnFieldEnrollments(cs341_0, &Enrollment{EnrolledDate: time.Now().UTC()})
	eric.LinkToCourseOnFieldEnrollments(hist347, &Enrollment{EnrolledDate: time.Now().UTC()})
	eric.LinkToCourseOnFieldEnrollments(phys122, &Enrollment{EnrolledDate: time.Now().UTC()})

	nikita.LinkToCourseOnFieldEnrollments(cs341_1, &Enrollment{EnrolledDate: time.Now().UTC()})
	nikita.LinkToCourseOnFieldEnrollments(phys122, &Enrollment{EnrolledDate: time.Now().UTC()})
	nikita.LinkToCourseOnFieldEnrollments(hist347, &Enrollment{EnrolledDate: time.Now().UTC()})

	steven.LinkToCourseOnFieldEnrollments(cs341_0, &Enrollment{EnrolledDate: time.Now().UTC()})
	steven.LinkToCourseOnFieldEnrollments(hist347, &Enrollment{EnrolledDate: time.Now().UTC()})

	michael.LinkToCourseOnFieldEnrollments(phys122, &Enrollment{EnrolledDate: time.Now().UTC()})
	michael.LinkToCourseOnFieldEnrollments(hist347, &Enrollment{EnrolledDate: time.Now().UTC()})

	// now to save these assignments
	err = sess.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// only saving to a depth of one
	err = sess.SaveDepth(hist347, 1)
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	err = sess.SaveDepth(cs341_0, 1)
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	err = sess.SaveDepth(cs341_1, 1)
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	err = sess.SaveDepth(phys122, 1)
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	err = sess.Commit()
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	// now we have the whole thing setup.

	// say I drop physics, i would do it like the following
	// gogm stores which relationships it loads nodes with internally so it can figure out if a relationship is removed on save

	err = eric.UnlinkFromCourseOnFieldEnrollments(phys122)
	if err != nil {
		log.Fatal(err)
	}

	err = sess.Begin()
	if err != nil {
		log.Fatal(err)
	}

	err = sess.SaveDepth(eric, 1)
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	err = sess.Commit()
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	// now im only enrolled in 2 courses

	// the following are some examples of how to load data
	// gogm figures out what kind of node you are looking for internally to generate its queries
	var allCourses []*Course
	err = sess.LoadAll(&allCourses)
	if err != nil {
		log.Fatal(err)
	}

	for _, course := range allCourses {
		log.Println(course.Name)
	}

	err = sess.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// heres an example of deleting a node
	err = sess.Delete(steven)
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}

	err = sess.Commit()
	if err != nil {
		log.Fatal(sess.RollbackWithError(err))
	}
}
