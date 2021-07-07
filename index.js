const Filehound = require('filehound');
const fs = require('fs');
const csv = require('csv-parser');
const converter = require('json-2-csv');


const globalStudentCodes = []
let globalCourseCodes = []
let courseAdmissions = []
let longGlobalCourse = []
let errorPerStudent = []
let unitEnrolmentErrorFile = []
let globalCitizenshipsCodes = []


let count = {}


const collegeList = Filehound.create()
    .path("files")
    .directory()
    .findSync();



const fileList = ["student", "campus_course_fee", "campus", "course_of_study", "course_on_campus", "course_prior_credit", "course", "tac_offer"]

kickOffCall();

async function kickOffCall() {

    let studentLongList = await getGlobalCSVs("studentKey");
    globalCourseCodes = await getGlobalCSVs("courseKey");
    longGlobalCourse = await getGlobalCSVs("enrolmentKey")
    for (file of fileList) {
        processFileForEachCollege(file, studentLongList)
    }
}

async function processFileForEachCollege(file, studentLongList, studentShortList = {}) {
    let promises = []

    let packet = file

    //read csv and put into promise for collection later
    for (let college of collegeList) {

        let csvName = `./${college}/${file}.csv`

    
        if (file === "course_admission") {
            try {
                if (fs.existsSync(csvName)) {
                    console.log("doing course admission")
                    let thisProm = readCSVForCourse(csvName, college, studentLongList, studentShortList, packet)
                    promises.push(thisProm);
                }
            } catch (err) { console.log(err) }

        } else if (file === "citizenship") {
            console.log("doing citizenships")
            let thisProm = readCSVCitizenship(csvName, college, studentLongList, studentShortList, packet)
            promises.push(thisProm);
        } else if (file === "student") {
            try {
                if (fs.existsSync(csvName)) {
                    console.log("doing student")
                    let thisProm = readCSVForStudent(csvName, college, packet);
                    promises.push(thisProm);
                }
            } catch (err) { console.log(err) }
        } else if (file === "unit_enrolment") {
            let thisProm = readUnitEnrolment(csvName, college, studentLongList, studentShortList, packet);
            promises.push(thisProm);
        }
        else {
            try {
                if (fs.existsSync(csvName)) {
                    promises.push(readCSV(csvName, college, packet));
                }
            } catch (err) { console.log(err) }
        }
    }


    //once each promise is resolved can progress
    await Promise.all(promises).then((res) => {

        if (file === "course_admission") {
            processFileForEachCollege("unit_enrolment", studentLongList, studentShortList);
        }
        const list = res.flat(1);
        if (file === "student") {
            console.log("students done");
            studentShortList = list
            processFileForEachCollege("course_admission", studentLongList, studentShortList);
            processFileForEachCollege("citizenship", studentLongList, studentShortList);

        }
        if (file === "unit_enrolment") {
            converter.json2csv(errorPerStudent, (err, csv) => {
                fs.writeFileSync(`./output/error.csv`, csv)
            })
        }
        converter.json2csv(list, (err, csv) => {
            fs.writeFileSync(`./output/${file}.csv`, csv);
        })
        converter.json2csv(unitEnrolmentErrorFile, (err, csv) => {
            fs.writeFileSync(`./output/errors.csv`, csv);
        })
        fs.writeFileSync(`./output/count.json`, JSON.stringify(count));
    })
}




function getGlobalCSVs(file) {
    let data = [];
    return new Promise((res, reject) => {
        fs.createReadStream(`./ids/${file}.csv`)
            .pipe(csv())
            .on('data', (row) => {
                data.push(row)
            })
            .on('end', () => {
                res(data);
            })
    })
}


function readCSV(csvName, collegeName, packet) {
    let tempData = []
    return new Promise((resolve, reject) => {
        fs.createReadStream(csvName)
            .pipe(csv())
            .on('data', (row) => {
                row.college = collegeName.split("/")[1]
                tempData.push(row)
                countIt(packet, collegeName, row.status)

            })
            .on('end', () => {
                console.log(tempData.length);
                console.log(packet)
                resolve(tempData)
                // console.log(`read 1 file for ${collegeName}`)
            });
    })
};
function readCSVCitizenship(csvName, collegeName, studentLongList, studentShortList, packet) {
    let tempData = []
    return new Promise((resolve, reject) => {
        fs.createReadStream(csvName)
            .pipe(csv())
            .on('data', (row) => {

                let searchedID = row.internalId.split(":")[2]
                row.inStudent = false

                if (!searchedID) { row.studentCode = row["student_code"] } else {


                    let studentId = globalStudentCodes.filter(obj => { return obj.internalId === searchedID })
                    if (studentId.length == 1) {
                        row.studentCode = studentId[0].studentCode
                        row.inStudent = true
                    }
                    else {
                        studentId = studentLongList.filter(obj => { return obj.CustomerId === searchedID })
                        row.studentCode = "NA"
                        if (studentId.length > 0) {
                            row.studentCode = studentId[0].StudentId
                        }
                    }
                }

                if (row.status != "success") {
                    errorPerStudent.push({
                        "student": row['student_code'],
                        "status": row.status,
                        "packet": packet,
                        "error": row["messageDescriptions"],
                        "uid": row.uuid,
                        "college": row.college,
                        "internalIds": row.internalId,

                    })
                }

                row.college = collegeName.split("/")[1]
                tempData.push(row)
                globalCitizenshipsCodes.push(row)
                countIt(packet, collegeName, row.status)


            })
            .on('end', () => {
                console.log(tempData.length);
                console.log(packet)
                resolve(tempData)
            });
    })
};

function readUnitEnrolment(csvName, collegeName, studentLongList, studentShortList, packet) {
    console.log("running unit enrolment");
    let tempData = []
    return new Promise((resolve, reject) => {
        fs.createReadStream(csvName)
            .pipe(csv())
            .on('data', (row) => {
                if (row.unit_of_study_census_date.split("-")[0] > 2020) {
                    row.inStudentPacket = false;
                    row.inCoursePacket = false;

                    let matchedRecords = []
                    let done = false

                    //check in the course admissions packet, for the course admissions uid 
                    if (row.course_admissions_uid) {
                        matchedRecords = courseAdmissions.filter(obj => { return obj.uuid === row.course_admissions_uid })

                        if (matchedRecords.length > 1) { console.log("I found multiple course admissions that match this course enrolment") }

                        //if it found a match in course true, set student and course codes to match in the course admissions
                        if (matchedRecords.length > 0) {
                            done = true
                            row["student_code"] = matchedRecords[0].studentCode
                            row.courseCode = matchedRecords[0].courseCode
                            row.inCoursePacket = true
                        }

                    }
                    if (!done) {

                        let courseKey = row.internalId.split(":")[4]
                        let returnedCourse = courseAdmissions.filter(obj => { return obj.courseCode === courseKey })
                        if (returnedCourse > 0) {
                            row["student_code"] = returnedCourse[0].studentCode
                            row.courseCode = returnedCourse[0].courseCode
                            row.inCoursePacket = true
                        } else {

                            // check against enrolment lookup using the internal ID of the course.
                            let courseInternal = row.internalId.split(":")[4]
                            let chosenCourses = longGlobalCourse.filter(obj => { return obj.EnrolmentCohortID === courseInternal })
                            if (chosenCourses.length > 0) {
                                //if found set the course code and student code
                                row.courseCode = chosenCourses[0].Course
                                row['student_code'] = chosenCourses[0].CustomerCode
                            } else {
                                //this is the case where its not in the enrolment key section either
                            }
                        }
                    }
                    row.college = collegeName.split("/")[1]
                    let studentsInStudentPacket = globalStudentCodes.filter(obj => { return obj.student_code === row["student_code"] })
                    if (studentsInStudentPacket > 0) { row.inStudentPacket === true }
                    if (row.status != "success") {
                        getRecordForError(row['student_code'])
                        errorPerStudent.push({
                            "student": row['student_code'],
                            "status": row.status,
                            "packet": packet,
                            "error": row["messageDescriptions"],
                            "uid": row.uuid,
                            "college": row.college,
                            "internalIds": row.internalId,

                        })
                    }

                    countIt(packet, collegeName, row.status)
                    tempData.push(row);
                }

            }).on("end", () => {
                resolve(tempData)
            })
    })
}

function getRecordForError(studentCode) {
    let studentTStamp = "";
    let studentStatus = "";
    let citizneshipTStamp = "";
    let citizneshipStatus = "";
    let courseAdmStatus = "";
    let courseAdmTimestamp = "";
    let citizenshipCode = "";
    let studentId = globalStudentCodes.filter(obj => { return obj.studentCode === studentCode })
    if (studentId.length == 1) {
        studentTStamp = studentId[0].timestamp
        studentStatus = studentId[0].status
    }

    let cits = globalCitizenshipsCodes.filter(obj => { return obj.studentCode === studentCode })
    if (cits.length == 1) {
        citizneshipTStamp = cits[0].timestamp
        citizneshipStatus = cits[0].status
        citizenshipCode = cits[0]["citizen_resident_code"]
    }

    let cAdm = globalCourseCodes.filter(obj => { return obj.studentCode === studentCode })
    if (cAdm.length == 1) {
        courseAdmTimestamp = cAdm[0].timestamp
        courseAdmStatus = cAdm[0].status
    }


    unitEnrolmentErrorFile.push({
        studentCode: studentCode,
        studentStatus: studentStatus,
        studentTimestamp: studentTStamp,
        citizinshipStatus: citizneshipStatus,
        citizenshipTimestamp: citizneshipTStamp,
        citizenshipCode: citizenshipCode,
        coureAdmissionStatus: courseAdmStatus,
        courseAdmissionTimestamp: courseAdmTimestamp,
    })
}


function readCSVForCourse(csvName, collegeName, studentLongList, studentShortList, packet) {
    let tempData = []
    return new Promise((resolve, reject) => {
        fs.createReadStream(csvName)
            .pipe(csv())
            .on('data', (row) => {
                let didFind = 0;
                //get student ID
                let searchedID = row.internalId.split(":")[4]

                if (!searchedID) { row.studentCode = row["student_code"] } else {


                    let studentId = globalStudentCodes.filter(obj => { return obj.internalId === searchedID })
                    if (studentId.length == 1) {
                        row.studentCode = studentId[0].studentCode
                        didFind++
                    }
                    else {
                        studentId = studentLongList.filter(obj => { return obj.CustomerId === searchedID })
                        row.studentCode = "NA"
                        if (studentId.length > 0) {
                            row.studentCode = studentId[0].StudentId
                            didFind++
                        }
                    }
                }

                //get course ID

                let courseInternal = row.internalId.split(":")[2]
                row.internalEnrolment = courseInternal
                let courseCode = globalCourseCodes.filter(obj => { return obj.id === courseInternal })
                if (courseCode.length > 0) {
                    row.courseCode = courseCode[0].code
                }
                row.college = collegeName.split("/")[1]
                countIt(packet, collegeName, row.status)

                if (row.status != "success") {
                    errorPerStudent.push({
                        "student": row['student_code'],
                        "packet": packet,
                        "error": row["messageDescriptions"],
                        "uid": row.uuid,
                        "college": row.college,
                        "internalIds": row.internalId,
                        "status": row.status
                    })
                }

                tempData.push(row)
            })
            .on('end', () => {
                courseAdmissions = tempData;
                resolve(tempData)
                console.log(`${collegeName} contains ${tempData.length} records`)
                console.log(`read 1 file for ${collegeName}`)
            });
    })
};

function countIt(packet, college, status) {
    college = college.split("/")[1]
    if (!count[college]) { count[college] = {} }
    if (!count[college][packet]) { count[college][packet] = {} }
    if (!count[college][packet][status]) { count[college][packet][status] = 1 } else {
        count[college][packet][status]++
    }
}


function readCSVForStudent(csvName, collegeName, packet) {
    let csvtempData = []
    return new Promise((resolve, reject) => {
        fs.createReadStream(csvName)
            .pipe(csv())
            .on('data', (row) => {
                let thisStudent = {
                    internalId: row.internalId.split(":")[2],
                    studentCode: row.student_identification_code
                }
                row.college = collegeName.split("/")[1]
                csvtempData.push(row)
                globalStudentCodes.push(thisStudent)
                if (row.status != "success") {
                    errorPerStudent.push({
                        "student": row['student_code'],
                        "packet": packet,
                        "error": row["messageDescriptions"],
                        "uid": row.uuid,
                        "college": row.college,
                        "internalIds": row.internalId,
                        "status": row.status
                    })
                }
                countIt(packet, collegeName, row.status)
            })
            .on('end', () => {

                resolve(csvtempData)
            });
    })
};
