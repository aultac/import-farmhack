const _ = require('lodash');
const Promise = require('bluebird');
const fs = Promise.promisifyAll(require('fs'));
const randomstring = require('randomstring');
const md5 = require('md5');
const request = require('request-promise');
const requestorig = require('request');
const jsonstream = require('JSONStream');
const eventstream = require('event-stream');
const moment = require('moment');
const countlines = Promise.promisify(require('count-lines-in-file'));

// If the import process dies at some point, put the "resume_url" here and it will
// skip everything until it finds that one in the PUT stream.  Set it to an empty
// string to have it start at the beginning
//const resume_url = "https://localhost/bookmarks/farmhack/vicsterksel/animal";
//const resume_url = "https://localhost/bookmarks/farmhack/nutreco/study1/rawfeedrecords/rows-index/760000"; 
const resume_url = "https://localhost/bookmarks/farmhack/vicsterksel/weight_history/rows-index/430000";
let found_resume_url = false;

process.env.DEBUG='info,*TODO*,trace';
const debug = require('debug');
const todo = debug('XXX TODO XXX');
const info = debug('info');
const trace = debug('trace');


process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// Note: csvtojson works nicely on command line to get JSON files

const token = 'KwGmHSxxAWsgJlXEHDmN2Rn1yemKA_awmEzUoPZW';
const oadabase = 'https://localhost/';
const rows_per_index = 10000;
const max_put_size = 10000000; // approximate byte size limit on low-level data puts

const overallstart = moment().unix();

function createRowIndices(oadapath,_type,numrows) { 
  trace('createRowIndices: Creating '+Math.ceil(numrows/rows_per_index)+' indices for path '+oadapath);
  // First, create the info for each of the page resources
  const pages = [];
  for (let i=0; i<numrows; i+= rows_per_index) {
    const path = oadapath + '/rows-index/' + i;
    pages.push({ 
      start: i, 
      end: i+rows_per_index-1, 
      path, 
      _id: pathToId(path),
      _type
    });
  }
  // Now create the page resources themselves
  return Promise.map(pages, page => {
    trace('createRowIndices: Putting page resource '+page._id+' for path '+page.path);
    return oadaGet(page._id)
    .then(exists => {
      if (exists) {
        info('createRowIndices: page resource /resources/'+page._id+' already exists, not repeating PUT');
        return null;
      }
      trace('createRowIndices: page resource /resources/'+page._id+' does not exist, PUTting');
      return oadaPut({ _id: page._id, context: { 'rows-index': page.start }, rows: {}, _type })
    }).then(() => page);

  // now each of the page resources exist, put to the parent resource with all the links to the pages
  }, { concurrency: 1 }).then(pages => {
    const body = { 'rows-index': { }, _type };
    _.each(pages, p => {
      body['rows-index'][p.start.toString()] = { _id: p._id, _rev: '0-0' };
    });
    trace('createRowIndices: Putting all links to page resources into the parent');
    return oadaGet(oadapath+'/rows-index/0', )
    .then(exists => {
      if (exists) {
        info('createRowIndices: page resource '+oadapath+' already has rows-index/0, NOT repeating PUT');
        return;
      }
      trace('createRowIndices: page resource '+oadapath+' does not have rows-index/0, doing PUT');
      return oadaPut(body, oadapath)
    }).then(() => pages);
  });
}

function putDataChunk(data, pages, linecount, curindex) {
  curindex = curindex || 0;
  info('putDataChunk: on index '+curindex+' of '+data.length);
  const end = linecount-1; // linecount is like array length: index is -1
  const start = end - (data.length-1); // handles partial array at end;
  const page = _.find(pages, p => p.start === start);
  if (!page) info('WARNING: could not find page for start = ', start, ' in set of pages: ', pages);

  // check here for resume_url as well as the one in oadaPut to speed things up:
  if (resume_url && resume_url.length > 0) {
    // If we haven't found the resume URL yet, check if this is the right one:
    if (!found_resume_url && oadabase+page.path !== resume_url) {
      info('Have not found the resume_url yet, skipping this one until we do.');
      return Promise.try(() => { return { statusCode: 204 } });
    }
    found_resume_url = true; // otherwise, we have found it at some point so do the PUT
  }
 

  const body = { rows: { }, _type: page._type };
  const sizeofonerow = JSON.stringify(data[0]).length;
  trace('putDataChunk: sizeofonerow = ', sizeofonerow);
  let thisrowindex = 0;
  for (; curindex < data.length; curindex++) {
    thisrowindex = curindex+start;
    body.rows[(thisrowindex).toString()] = data[curindex];
    // To keep the put message size down, recursively put only 10kb of data at a time
    if (_.keys(body.rows).length * sizeofonerow > max_put_size) {
      trace('putDataChunk: putting up to index '+curindex+' of '+data.length+' rows');
      // Check if this put has already been made by getting just one row back:
      return oadaGet(page.path+'/rows/'+thisrowindex)
      .then(row => {
        if (row) {
          info('putDataChunk: chunk for path '+page.path+' already done, not repeating PUT');
          return;
        }
        info('putDataChunk: chunk for path '+page.path+' not there yet, executing PUT');
        return oadaPut(body,page.path)
      }).then(() => putDataChunk(data,pages,linecount,curindex)); // recursively put the next set of rows
    }
  } // could create separate resource for each piece of data here
  trace('putDataChunk: putting final set of rows');
  return oadaGet(page.path+'/rows/'+thisrowindex)
  .then(exists => {
    if (exists) {
      info('putDataChunk: final put, path '+page.path+' already has /rows/'+thisrowindex+', not repeating PUT');
      return true;
    }
    return oadaPut(body, page.path);
  });
}

function putFileContents(oadapath,_type,filepath) {
  // /bookmarks/farmhack/nutreco/study1/bw/sheet1/rows-index/0/rows/[0, 1, 2, 3]
  // /bookmarks/farmhack/nutreco/study1/bw/sheet1/rows-index/10000/rows/[10000,10001,]
  // /bookmarks/farmhack/nutreco/study1/bw/sheet1/rows-index/20000/
  // ....
  // /bookmarks/farmhack/nutreco/study1/bw/sheet1/rows-index/200000/
  trace('putFileContents: Putting file contents of '+filepath+' to '+oadapath+' with type '+_type);
  let data = new Array(rows_per_index);
  let index = 0;
  let linecount = 0;
  let paginate = false;
  let num_rows = 0;
  return countlines(filepath)
  .then(num_lines => {
    num_rows = num_lines - 2; // the first and last lines are brackets for arrays
    if (num_rows > rows_per_index) {
      trace('putFileContents: file '+filepath+' contains more than 10,000 rows ('+num_rows+'), creating indexes');
      return createRowIndices(oadapath,_type,num_lines);
    }
    trace('putFileContents: file '+filepath+' contains less than 10,000 rows ('+num_rows+'), no index needed');
    return [ 
      { 
        start: 0, 
        end: num_rows-1, 
        path: oadapath,
        _id: pathToId(oadapath),
        _type,
      } 
    ]; // if there is just one page, don't put it in an index
  }).then(pages => {
    const starttime = moment().unix();
    return new Promise((resolve,reject) => {
      fs.createReadStream(filepath)

      .pipe(
        jsonstream.parse('*')
        .on('error', err => { 
          info('ERROR: jsonstream failed on file '+filepath+'.  err = ', err);
          reject();
        })

      ).pipe(eventstream.through(function onData(read_data) {
        linecount++;
        data[index++] = read_data;
        if (index === rows_per_index) {
          // send off the put request(s)
          trace('send the put request for page '+Math.floor(linecount/rows_per_index)+' now that a page is full');
          this.pause();
          putDataChunk(data,pages,linecount)
          .then(() => {
            index = 0;
            this.resume();
          });
          return;
        }

      }, function onEnd() {
        let seconds = moment().unix() - starttime;
        if (seconds < 1) seconds = 1;
        info('parsed '+linecount+' lines from file '+filepath+' in '+seconds+' seconds.  Average '+(linecount/seconds)+' lines/sec'); 

        // send the remaining items if there are any:
        if (index > 0) {
          trace('send the remaining items in the last page');
          putDataChunk(data.slice(0,index),pages,linecount)
          .then(() => { index=0; resolve(); });
          return;
        }
        resolve();

      })).on('error', err => { info('ERROR: outer read stream error = ', err); reject(err) });
    });
  });
}

function oadaGet(path) {
  return request({
    uri: oadabase + path,
    method: 'GET',
    headers: { 
      authorization: 'Bearer '+token,
    },
    resolveWithFullResponse: true,
  }).then(result => {
    trace('after GET, result.statusCode = ', result.statusCode);
    return true;
  }).catch(err => {
    info('GET for '+oadabase+path+' failed.  Assuming does not exist.');
    return null;
  });
}

let putcounter = 0;
// MUST have _type in resource (res)
function oadaPut(res,path,trycounter) { // path is optional, uses res._id if no path
  if (!trycounter) trycounter = 0;
  putcounter++;
  path = path || res._id;
  info(putcounter + ' ('+(moment().unix()-overallstart)+' secs): PUT '+oadabase+path+', body size = ', JSON.stringify(res).length);
  //trace('PUT body = ',JSON.stringify(res));
  //return Promise.try(() => { return { statusCode: 204 } });
  // If there is a resume url:
  if (resume_url && resume_url.length > 0) {
    // If we haven't found the resume URL yet, check if this is the right one:
    if (!found_resume_url && oadabase+path !== resume_url) {
      info('Have not found the resume_url yet, skipping this one until we do.');
      return Promise.try(() => { return { statusCode: 204 } });
    }
    found_resume_url = true; // otherwise, we have found it at some point so do the PUT
  }
  return request({
    uri: oadabase + path,
    method: 'PUT',
    headers: { 
      authorization: 'Bearer '+token,
      'content-type': res._type,
    },
    body: JSON.stringify(res),
    resolveWithFullResponse: true,
  }).then(result => {
    trace('after PUT, result.statusCode = ', result.statusCode);
    return result;
  }).catch(err => {
    info('ERROR: Attempt '+trycounter+': PUT '+oadabase+path+' failed: err.statusCode =  ',err.statusCode,', err.toString() = ', err.toString());
    if (trycounter < 10) {
      info('Trying PUT again since we have tried less than 10 times');
      return oadaPut(res,path,trycounter+1);
    }
    info('ERROR: PUT failed 10 times in a row, throwing up');
    throw err;
  });
}



//--------------------------------------------------------------
// This is a hack to keep me from having to decide if this
// has already run or not: resourceid's are just hashes of the
// path so that if it gets run twice, you get only one copy of
// stuff in the database anyway
const pathToId = path => {
  let _key = '';
  if (typeof path === 'string') _key = md5(path+token);
  else                          _key = md5([...path, token].join('/'));
  return 'resources/'+_key;
}



//--------------------------------------------------------------
// buildPath will take an array that represents a full path, and
// create each document in the OADA cloud along the way
function buildPath(path) {
  const results = [];
  return Promise.each(path,(p,i) => {
    const fullpath = path.slice(0,i+1); // [ [ bookmarks ], [ bookmarks, farmhack ], ... ],
    trace('buildPath: index '+i+', fullpath = ', fullpath);
    let _type = 'application/vnd.'+fullpath.slice(1).join('.')+'.1+json'; // slice off the bookmarks: vnd.farmhack...
    let _id = pathToId(fullpath);
    if (p === 'bookmarks') {
      _id = 'bookmarks';
      _type = 'application/vnd.oada.bookmarks.1+json';
    }
    // If this is not the last one, add the child link to the body:
    const body = { _id, _type };
    if (i+1 < path.length) {
      const childfullpath = path.slice(0,i+2);
      const childkey = path[i+1];
      const childid = pathToId(childfullpath);
      body[childkey] = { _id: childid, _rev: '0-0' };
    }
    return oadaGet(body._id+'/_type')
    .then(exists => {
      if (exists) {
        info('buildPath: resource /resources/'+body._id+'/_type already exists, not PUTting resource');
        results.push({ _type }); // snd back the _type so outer thing can set the proper type
      }
      trace('buildPath: resource /resources/'+body._id+' does not exist, doing PUT');
      return oadaPut(body)
      .then(result => {
        if (result.statusCode > 299) throw new Error('Failed to build path '+ fullpath + ', err = ' + result.body);
        info('Successfully built path ', fullpath.join('/'));
        results.push(body);
      });
    });
  }).then(() => results);
};


//--------------------------------------------------------------
// Get list of files from json directory, construct paths from
// their filenames
const jsondir = './rnd-json';
return fs.readdirAsync(jsondir)
.map(filename => {
  const namePath = filename.replace(/\.csv\.json/,'').split('-');
  const path = [ 'bookmarks', 'farmhack', ...namePath ];
  const companyName = namePath[0];

  // Also check for resume_url here and in oadaPut and putDataChunk to speed things up
  if (resume_url && resume_url.length > 0) {
    if (!found_resume_url && resume_url.indexOf(companyName) < 0) {
      info('Company '+companyName+' files are not part of resume_url, skipping to next one.');
      return;
    }
    info('Company name ('+companyName+') IS part of resume_url ('+resume_url+').  Checking next part of path.');
    // Otherwise, company is part of it, check the next one too:
    if (!found_resume_url && resume_url.indexOf(namePath[1]) < 0) {
      info('Company '+companyName+' files ARE part of resume_url, but next part of filename IS NOT part of resume_url.  Skipping to next one.');
      return;
    }
  }

  trace('main: Building path '+path);
  // Create all the resources along the path
  return buildPath(path)

  // Then put each file at it's created path
  .then(all_path_results => {
    trace('main: Paths built for file '+filename+', now putting file');
    const _type = all_path_results[all_path_results.length-1]._type; // last one is the longest path
    return putFileContents(path.join('/'),_type,jsondir+'/'+filename);
  }).then(() => {
    info('Done with file '+filename);
    //throw new Error('Stopping prematurely for testing');
  });
}, { concurrency: 1 }) // we'll do one file at a time
.then(() => {
  const overallend = moment().unix();
  info('Finished all files in '+ (overallend - overallstart)+' seconds.');
});
