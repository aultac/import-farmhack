const study1_sheet1 = require('../realjson/nutreco-study1-bw-sheet1.csv.json')
//const study1_sheet2 = require('../realjson/nutreco-study1-bw-sheet2.csv.json')
//const study1_sheet3 = require('../realjson/nutreco-study1-bw-sheet3.csv.json')
const study2 = require('../realjson/nutreco-study2-bw.csv.json')
const study3 = require('../realjson/nutreco-study3-slaughterhouse.csv.json')

const fs = require('fs')

var obj = {}
study1_sheet1.forEach((row) => {
  if (!row.alt_id) console.log('study 1', row)
  if (row.alt_id !=='NN') obj[row.volg_num] = row.alt_id
})
//study 1 sheets 2 and 3 have no relational information to contribute, only the alt_id as key "DIER"
//study1_sheet2.forEach((row) => {
//  obj[row.volg_num] = row.alt_id;
//})
//study1_sheet3.forEach((row) => {
//  obj[row.volg_num] = row.alt_id;
//})
study2.forEach((row) => {
  if (row.animal_id.indexOf('NN') > 0) console.log('study2', row)
  obj[row.animal] = row.animal_id;
})
study3.forEach((row) => {
  if (!row.alt_id) console.log('study 3', row)
  if (row.alt_id.indexOf('NN') > 0) console.log('study 3', row)
  obj[row.volg_num] = row.alt_id;
})
fs.writeFileSync('animalIdMapping.json', JSON.stringify(obj))
