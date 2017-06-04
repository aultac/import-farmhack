const _ = require('lodash');
const moment = require('moment');
const librandomstring = require('randomstring');
const librandomdate = require('random-date');
const jsonstream = require('JSONStream');
const eventstream = require('event-stream');
const Promise = require('bluebird');
const fs = Promise.promisifyAll(require('fs'));
const fileexists = Promise.promisify(require('file-exists'));

// Generates randomized versions of everything in ./json 
// and stores them in ./random-json
process.env.DEBUG='info,*TODO*,trace';
const debug = require('debug');
const info = debug('info');
const trace = debug('trace');

//-----------------------------------------------------
// random helpers
const randomDigit = () => librandomstring.generate({ length: 1, charset: 'numeric' });
const randomChar = () => librandomstring.generate({ length: 1, charset: 'alphabetic' });
const randomBoolStr = () => librandomstring.generate({ length: 1, charset: '01' });
const randomMoment = () => moment(librandomdate('-2000d')); // anything in past 2000 days
const randomizeCharacters = str => {
  let newstr = '';
  for (let i=0; i<str.length; i++) {
    let c = str[i];
         if (c === '/')  newstr += '/';
    else if (c === '\\') newstr += '/';
    else if (c === '?')  newstr += '?';
    else if (c === '-')  newstr += '-';
    else if (c === '_')  newstr += '_';
    else if (c === '.')  newstr += '-';
    else if (c === ')')  newstr += ')';
    else if (c === '(')  newstr += '(';
    else if (c === ' ')  newstr += ' ';
    else if (c === ',')  newstr += ',';
    else if (c === ';')  newstr += ';';
    else if (c === ':')  newstr += ':';
    else if (c === '>')  newstr += '>';
    else if (c === '<')  newstr += '<';
    else if (c === '%')  newstr += '%';
    else if (c === '+')  newstr += '+';
    else if (c === '@')  newstr += '@';
    else if (c === '!')  newstr += '!';
    else if (c === "'")  newstr += "'";
    else if (c.match(/[0-9]/))    newstr += randomDigit();
    else if (c.match(/[A-Za-z]/)) newstr += randomChar();
    else {
      info('WARNING: character '+c+' at position '+i+' from str '+str+' was not matched.  Copying verbatim');
      newstr += c;
    }
  }
  return newstr;
}
const randomNumber = num => +(randomizeCharacters(num.toString()));


/* Examples: 
    "IDENT_RFID": "900248001297131",
    "Slaughterweight": "84,2",
    "Backfat": "010,3",
    "SlaughterDate": "6/1/17",
    "BIRTH_DATE_Slachtdata": "42546",
    "DateWeightMating": "-",
    "4/10/17": ""
    "Date_1": "9-Feb",
    "Date_2": "26-Apr",
    "begintyd": "15:25:23",
    "date": "2016-02-09",
    "RFI": "19.668199080551663",
    "date": "Tue May 31 20:00:00 EDT 2016",
    "date": "Wed Jun 01 20:00:00 EDT 2016",
    "animal": "              3559",
    "carcass": ".",
    "end_dat": "08/09/2016",
    "BREED": "Maxter",
    "COST_DESCR": "Aankoop Speenbig",
    "IND_BREEDING": "1" // boolean I think
    "DESCR": "?????",
    "LOCATION_ID_IN": "K/04/006",
    "PHONE": "+31 (0)570 664111",
*/

const randomizeValue = val => {
  if (typeof val === 'number') return randomNumber();
  else if (typeof val !== 'string') throw new Error('ERROR: type of value ('+(typeof val)+') is not number or string!');

  if (val === "0" || val === "1") return randomBoolStr();
  const vt = val.trim();

  // 9-Feb 26-Apr
  if (vt.match(/^[0-9]{1,2}-[A-Za-z]{3}$/)) return randomMoment().format('D-MMM');

  // 6/1/17 or 6/1/2017
  if (vt.match(/[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{2}/)) return randomMoment().format('M/D/YY');
  if (vt.match(/[0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4}/)) return randomMoment().format('M/D/YYYY');

  // 15:25:23
  if (vt.match(/[0-9]{1,2}:[0-9]{2}:[0-9]{2}/)) return randomMoment().format('HH:mm:ss');

  // Tue May 31 21:00:00 EDT 2016 
  // NOTE: this doesn't make the time zone exactly the same...
  if (vt.match(/[A-Za-z]{3} [A-Za-z]{3} [0-9]{2} [0-9]{2}:[0-9]{2}[0-9]{2}/)) return randomMoment.toString()

  return randomizeCharacters(val);
}

const randomizeObject = (filename, line, obj) => {
//  console.log(filename +':'+line+': obj = ', obj);
  return _.mapValues(obj, randomizeValue); // all our objects are just one level deep
};

//--------------------------------------------------------------
// Now just get the list of files in ./json and create
// new randomized versions in ./random-json
const overallstart = moment().unix();
return fs.readdirAsync('./json')
.map(filename => {
  const infile = './json/'+filename;
  const outfile = './rnd-json/'+filename;

  // Only produce an output file if the output file does not already exist.
  // Avoids accidently running this script and having it randomly wipe out files.
  return fileexists(outfile)
  .then(exists => {
    if (exists) {
      info('Cowardly refusing to overwrite output file '+outfile+'.  Delete it if you want to overwrite it.');
      return;
    }
    // Otherwise, on with the show
    let line = 0;
  
    info('Starting to read file ', infile);
    return new Promise((resolve,reject) => {
      const writestream = fs.createWriteStream(outfile);
      writestream.write('[\n');
      let firsttime = true;
  
      fs.createReadStream(infile)
  
      // Read each JSON object one at a time
      .pipe(
        jsonstream.parse('*')
        .on('error', err => { 
          info('ERROR: jsonstream failed on file '+infile+'.  err = ', err);
          reject();
        })
  
      // turn each json object into a string to be written to output file
      ).pipe(
        eventstream.through( function writeData(data) {
          if (!firsttime) writestream.write(','); // each object needs comma at end except last one
          else            firsttime = false;
          writestream.write(JSON.stringify(randomizeObject(infile, line++, data))+'\n');
          if (!(line % 10000)) trace('writing line '+line+' of file '+infile);
        },function end() {
          writestream.end(']');
          info('Done with file ',infile);
          resolve();
        })
  
      ).on('error', err => { info('ERROR: outer read stream error = ', err); reject(err) });
    });
  });  
}, { concurrency: 50 })
.then(() => {
  const overallend = moment().unix();
  info('Finished all files in '+ (overallend - overallstart)+' seconds.');
});
