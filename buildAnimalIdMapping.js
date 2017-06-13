const bw_1 = require('../realjson/nutreco-study1-bw-sheet1.csv.json')
const bw_2 = require('../realjson/nutreco-study1-bw-sheet2.csv.json')
const bw_3 = require('../realjson/nutreco-study1-bw-sheet3.csv.json')

const fs = require('fs')

var obj = {}
bw_1.forEach((row) => {
  obj[row.volg_num] = row.alt_id;
})
bw_2.forEach((row) => {
  obj[row.volg_num] = row.alt_id;
})
bw_3.forEach((row) => {
  obj[row.volg_num] = row.alt_id;
})
console.log(obj)
fs.writeFileSync('animalIdMapping.json', JSON.stringify(obj))
