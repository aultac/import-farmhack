const _ = require('lodash');
const Promise = require('bluebird');
const fs = Promise.promisifyAll(require('fs'));
const randomstring = require('randomstring');
const md5 = require('md5');
const request = require('request-promise');
const jsonstream = require('JSONStream');
const eventstream = require('event-stream');
const moment = require('moment');
const countlines = Promise.promisify(require('count-lines-in-file'));
const mkdirp = Promise.promisify(require('mkdirp'));
const argv = require('minimist')(process.argv.slice(2));

if (argv._.length !== 1 || (argv._[0] !== 'frank' && argv._[0] !== 'randy')) {
  console.log('USAGE: node file-import.js <frank|randy>');
  process.exit();
}

process.env.DEBUG='info,*TODO*';
const debug = require('debug');
const todo = debug('XXX TODO XXX');
const info = debug('info');
const trace = debug('trace');

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// Note: csvtojson works nicely on command line to get JSON files

const users = {
  frank: {
    _id: 'users/default:users_frank_123',
    name: 'frank',
    authorizationid: "authorizations/default:authorization-123",
    json: './json',
    bookmarksid: 'resources/default:resources_bookmarks_123',
  },
  randy: {
    _id: 'users/default:users_frank_123',
    name: 'randy',
    authorizationid: "authorizations/default:authorization-333",
    json: './rnd-json',
    bookmarksid: 'resources/default:resources_bookmarks_333',
  },
};
const user = users[argv._[0]];
const outputdir = 'generateImport_'+user.name+'_'+moment().format('YYYY-MM-DD_HH:mm:ss');
const rows_per_index = 10000;

const overallstart = +moment();
//-------------------------------------------------------
// Super convenient hack: given path, functionally compute
// both resource id and type
const pathToId = path => {
  if (typeof path === 'string') path = path.split('/');
  if (path.length === 1 && path[0] === 'bookmarks') return user.bookmarksid;
  return 'resources/' + md5(path.join('/') + user.authorizationid);
};

// the .slice(1) gets rid of bookmarks, keeps from farmhack on down
const pathToType = path => {
  if (typeof path === 'string') path = path.split('/');
  if (path.length === 1 && path[0] === 'bookmarks') return 'application/vnd.oada.bookmarks.1+json';
  let indexfree_path = [];
  for (let i=0; i<path.length; i++) {
    if (!path[i].match(/.*-index/) && (!path[i.match(/.*animals/))) indexfree_path.push(path[i]);
    else i++; // skip this one and the next one
  }
  indexfree_path = indexfree_path.slice(1); // get rid of bookmarks
  return 'application/vnd.'+indexfree_path.join('.')+'.1+json';
};



function putDataChunk(pagestart, rows, path, total_rows) {
  const body = { rows };
  // use an index if total lines are going to be greater than rows_per_index
  if (total_rows > rows_per_index) {
    trace('putDataChunk: using index, on page '+pagestart+' for total rows '+total_rows);
    path = [ ...path, 'rows-index', pagestart ];
    body.context = {
      'rows-index': pagestart.toString(),
    };
  }
  info('putDataChunk: PUT '+path.join('/'));
  return ensurePathPut(path, body);
}

function putFileContents(oadapath,filepath) {
  // /bookmarks/farmhack/nutreco/study1/bw/sheet1/rows-index/0/rows/[0, 1, 2, 3]
  // /bookmarks/farmhack/nutreco/study1/bw/sheet1/rows-index/10000/rows/[10000,10001,]
  // /bookmarks/farmhack/nutreco/study1/bw/sheet1/rows-index/20000/
  // ....
  // /bookmarks/farmhack/nutreco/study1/bw/sheet1/rows-index/200000/
  info('putFileContents: Putting file contents of '+filepath+' to '+oadapath.join('/')+' with type '+pathToType(oadapath));
  let rows = {};
  let pagerowcount = 0;
  let totalrowcount = 0;
  let pagestart = 0;
  let num_total_rows = 0;
  let num_pages = 0;
  return countlines(filepath)
  .then(num_lines => {
    num_total_rows = num_lines - 2; // the first and last lines are brackets for arrays
    num_pages = Math.ceil(num_total_rows / rows_per_index);
    if (num_total_rows < 1) {
      trace('putFileContents: file '+filepath+' has nothing to insert, ensuring path and skipping data');
      return ensurePathPut(oadapath, { rows: {} });
    }
    const starttime = +moment();
    // Start Reading
    return new Promise((resolve,reject) => {
      fs.createReadStream(filepath)
      .pipe(
        jsonstream.parse('*')
        .on('error', err => { 
          info('ERROR: jsonstream failed on file '+filepath+'.  err = ', err);
          reject();
        })
      ).pipe(eventstream.through(function onData(read_data) {
        pagerowcount++;
        rows[totalrowcount++] = read_data;
        if (pagerowcount === rows_per_index) {
          this.pause();
          trace('putFileContents: page full, putting '+oadapath.join('/'));
          putDataChunk(pagestart,rows,oadapath,num_total_rows)
          .then(() => {
            pagestart = totalrowcount;
            pagerowcount = 0;
            rows = {};
            this.resume();
          });
        }
      }, function onEnd() {
        let mseconds = +moment() - starttime;
        info('parsed '+totalrowcount+' lines from file '+filepath+' in '+mseconds+' ms.  Average '+(totalrowcount/mseconds)+' lines/ms'); 

        // send the remaining items if there are any:
        if (pagerowcount > 0) {
          trace('putFileContents: final page, putting '+oadapath.join('/'));
          putDataChunk(pagestart,rows,oadapath,num_total_rows)
          .then(() => resolve());
        }
      })).on('error', err => { info('ERROR: outer read stream error = ', err); reject(err) });
    });
  });
}

let putcounter = 0;
// MUST have _type in resource (res)
const graphNodesAlreadyCreated = { ['graphNodes/'+pathToId(['bookmarks']).replace('/',':')]: true }; // bookmarks graph node always exists
const edgesAlreadyCreated = {};
function oadaPut(res) {
  // 1: look for any links in the resource
  const links = [];
  const findLinks = (obj,path_prefix) => _.each(obj, (v,k) => {
    path_prefix = path_prefix || [];
    if (typeof v !== 'object') return; // if not object, can't be link
    const path = [ ...path_prefix, k ];
    if (v._id) { // if this object is a link
      links.push({ path, link: v, versioned: !!v._rev });
      return;
    }
    // otherwise, it's an object but not a link, recurse:
    return findLinks(v,path);
  });
  findLinks(res);
  trace('oadaPut: links = ', links);

  // 2: add OADA keys to resource (_meta, _meta._changes, _oada_rev), and put _key for arango
  res._key = res._id.replace('resources/','');
  res._oada_rev = '1-'+md5(res._id); // so it's predictable in rest of this script
  res._meta = {
    '_id': res._id + '/_meta',
    '_type': res._type,
    '_rev': res._oada_rev,
    '_owner': user._id,
    'stats': {
      'createdBy': user._id,
      'created': moment().unix(),
      'modifiedBy': user._id,
      'modified': moment().unix(),
    },
  };
  const putresource = _.cloneDeep(res);
  putresource._meta._changes = {
    _id: res._id+'/_meta/_changes',
    _rev: res._oada_rev,
  };
  putresource._meta._changes[res._oada_rev] = {
    merge: res,
    userId: user._id,
    authorizationID: user.authorizationid,
  };
  trace('oadaPut: PUT '+putresource._id);

  // 3: for each of the links, make the necessary graphNodes and edges
  const resourceGraphNodeid = 'graphNodes/'+res._id.replace('/',':');
  let graphNodes = [
    { // first, graphNode for this resource:
      _id: resourceGraphNodeid,
      resource_id: res._id,
      is_resource: true,
    }
  ];

  // 4: build graph nodes necessary for any links to the graphNodes file
  _.each(links, l => {
    // make one graph node for each path in the link
    graphNodes = _.concat(graphNodes,
      _.reduce(l.path, (acc,p,i) => {
        if (i === l.path.length-1) return acc; // last name in link path will be edge to resource
        const path_prefix = '/'+l.path.slice(0,i+1).join('/');
        trace('oadaPut: PUT '+resourceGraphNodeid+path_prefix.replace('/',':'));
        acc.push({
          _id: resourceGraphNodeid + path_prefix.replace('/',':'),
          resource_id: res._id,
          is_resource: false,
          path: path_prefix,
        });
        return acc;
      },[])
    );
  });
  // 5: filter out any graphNodes that we've already created and update created list with new ones
  graphNodes = _.filter(graphNodes, g => !graphNodesAlreadyCreated[g._id]);
  _.each(graphNodes, g => { graphNodesAlreadyCreated[g._id] = true });
  // add _key for arango:
  graphNodes = _.map(graphNodes, g => { g._key = g._id.replace('graphNodes/',''); return g; });

  // 6: build edge nodes necessary for any links to the edges file
  let edges = [];
  _.each(links, l => {
    // make edge for each path in the link
    edges = _.concat(edges,
      _.map(l.path, (p,i) => {
        const from_path_prefix = l.path.slice(0,i);
        let _from = resourceGraphNodeid;
        if (from_path_prefix.length > 0) { // first one was from resource itself
          _from += ':'+from_path_prefix.join(':');
        }
        const to_path_prefix = l.path.slice(0,i+1);
        let _to = resourceGraphNodeid+':'+to_path_prefix.join(':');
        if (i === l.path.length-1) { // last one goes to the link's _id
          _to = 'graphNodes/'+l.link._id.replace('/',':');
        }
        trace('oadaPut: PUT edges/'+md5(_from+_to));
        return {
          _id: 'edges/'+md5(_from+_to),
          _from,
          _to,
          name: p,
          versioned: l.versioned,
        };
      })
    );
  });
  // 7: filter out any edges we've already created and update created list with new ones
  edges = _.filter(edges, e => !edgesAlreadyCreated[e._id]);
  _.each(edges, e => { edgesAlreadyCreated[e._id] = true; });
  edges = _.map(edges, g => { g._key = g._id.replace('edges/',''); return g; });

  // 6: in parallel, write to all three files:
  return Promise.all([
     streams.resources.writeAsync(JSON.stringify(putresource)+'\n'),
    streams.graphNodes.writeAsync(_.map(graphNodes, g => JSON.stringify(g)+'\n').join('')),
         streams.edges.writeAsync(_.map(     edges, e => JSON.stringify(e)+'\n').join('')),
  ]);
}



//--------------------------------------------------------
// Make sure path exists so it can be put to.  Create
// parents as necessary.
const paths_already_ensured = { 'bookmarks': true };
function ensurePathPut(path, body) {
  body = body || {};
  const pathstr = path.join('/');
  trace('ensurePath: path '+pathstr);
  if (JSON.stringify(body).length < 1000) trace('ensurePath: and body = ', body);

  const lastkey = ''+path.slice(-1)[0];
  const isindex = !!lastkey.match(/-index/);
  const parent_path = path.slice(0,-1);
  return Promise.try(() => {
    // If this is not an index, we need to put the body as a resource itself,
    // and just put a link to ourself in the parent instead of the whole body
    if (!isindex) {
      paths_already_ensured[pathstr] = true; // only ensure non-index paths so that they'll always get put into parent
      body._id = pathToId(path);
      body._type = pathToType(path);
      trace('ensurePathPut: path '+pathstr+' not an index, putting resource');
      return oadaPut(body)
      .return({ _id: body._id, _rev: '1-'+md5(body._id) }); // change the body for a link
    }
    trace('ensurePathPut: path '+pathstr+' IS and index, moving on to parent');
    // otherwise, put the given body in the parent as-is
    return body;

  // Now put the link/data into the parent at the proper key in parent.  If 
  // we're not assured of the parent's existence, do an ensurePathPut.  If
  // we know parent exists, just do oadaPut.
  }).then(body => {
    if (paths_already_ensured[parent_path.join('/')]) {
      trace('ensurePathPut: parent path '+parent_path.join('/')+' already exists, doing oadaPut');
      return oadaPut({ 
        _id: pathToId(parent_path), 
        _type: pathToType(parent_path), 
        [lastkey]: body, // body is either original body for index put, or link to resource
      });
    }
    // otherwise, parent is not already ensured, create it first and then put body:
    trace('ensurePathPut: parent path '+parent_path.join('/')+' does NOT exist, doing ensurePathPut');
    return ensurePathPut(parent_path, { [lastkey]: body });
  });
}



//--------------------------------------------------------------
// Get list of files from json directory, construct paths from
// their filenames
const streams = {
  resources: null,
  edges: null,
  graphNodes: null,
};
return mkdirp(outputdir)
.then(() => {
  streams.resources = fs.createWriteStream(outputdir+'/resources.json');
  streams.edges = fs.createWriteStream(outputdir+'/edges.json');
  streams.graphNodes = fs.createWriteStream(outputdir+'/graphNodes.json');
}).then(() => fs.readdirAsync(user.json))
.map(filename => {
  const namePath = filename.replace(/\.csv\.json/,'').split('-');
  const path = [ 'bookmarks', 'farmhack', ...namePath ];
  const companyName = namePath[0];

  return putFileContents(path,user.json+'/'+filename)
  .then(() => info('Done with file '+filename));
}, { concurrency: 1 }) // we'll do one file at a time
.then(() => {
  const overallend = +moment();
  info('Finished all files in '+ (overallend - overallstart)+' ms.');
});
