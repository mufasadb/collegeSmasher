const fs = require('fs');

const csv = require('csv-parser');
const converter = require('json-2-csv');

let json = require("./output/count.json");

let schoolNames = Object.keys(json)
let packetNames = []
let statusNames = []
let schoolList = Object.values(json)
// console.log(schoolList[0]);
for (let school of schoolList) {
    for (packet of Object.keys(school)) {
        // console.log(packet);
        if (!packetNames.includes(packet)) { packetNames.push(packet) }

        let packets = Object.values(school)
        for (stati of Object.values(packets)) {
            for (let status of Object.keys(stati)) {
                if (!statusNames.includes(status)) { statusNames.push(status) }
            }
        }
    }
}

let res = []

for (school of schoolNames) {
    for (packet of packetNames) {
        if (!json[school][packet]) { json[school][packet] = {} }
        for (status of statusNames) {
            if (!json[school][packet][status]) { json[school][packet][status] = 0 }
        }
        let item = json[school][packet]
        item.school = school
        item.packet = packet
        res.push(item)
    }
}

// console.log(res.length);

// let expected = []
// fs.createReadStream('./ids/expected.csv')
//     .pipe(csv())
//     .on('data', (row) => {
//         expected.push(row);
//     })
//     .on('end', () => {
//         console.log(expected);
//     })
// console.log(schoolNames)
// console.log(packetNames)
// console.log(statusNames)
// console.log(array);

converter.json2csv(res, (err, csv) => {
    fs.writeFileSync(`./output/total.csv`, csv);
})