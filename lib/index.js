var Busboy, DataSource, LargeObjectManager, PostgresStorage, _, async, debug, getDefaultSettings, pg;

_ = require('lodash');

async = require('async');

Busboy = require('busboy');

DataSource = require('loopback-datasource-juggler').DataSource;

debug = require('debug')('loopback:storage:postgres');

pg = require('pg');

LargeObjectManager = require('pg-large-object').LargeObjectManager;

if (typeof Promise === "undefined" || Promise === null) {
  global.Promise = require('bluebird');
}

getDefaultSettings = function(settings) {
  var defaultSettings;
  defaultSettings = {
    host: settings.hostname || 'localhost',
    port: 5432,
    database: 'test',
    table: 'files',
    idleTimeoutMillis: 1000,
    max: 10,
    Promise: Promise
  };
  return _.extend(defaultSettings, settings);
};

PostgresStorage = (function() {
  function PostgresStorage(settings) {
    this.settings = getDefaultSettings(settings);
    this.db = new pg.Pool(this.settings);
  }

  PostgresStorage.prototype.getContainers = function(callback) {
    this.db.query("select distinct container from " + this.settings.table, [], function(err, res) {
      return callback(err, res != null ? res.rows : void 0);
    });
  };

  PostgresStorage.prototype.getContainer = function(name, callback) {
    this.db.query("select * from " + this.settings.table + " where container = $1", [name], function(err, res) {
      return callback(err, {
        container: name,
        files: res != null ? res.rows : void 0
      });
    });
  };

  PostgresStorage.prototype.destroyContainer = function(name, callback) {
    var closeConnection, currentClient, self;
    self = this;
    currentClient = null;
    closeConnection = function(err, res) {
      if (currentClient != null) {
        currentClient.release();
      }
      return callback(err, res);
    };
    self.db.connect().then(function(client) {
      currentClient = client;
      return currentClient.query('BEGIN TRANSACTION', function(err) {
        if (err) {
          return closeConnection(err);
        }
        return async.waterfall([
          function(done) {
            var sql;
            sql = "select lo_unlink(objectid) from " + self.settings.table + " where container = $1";
            return currentClient.query(sql, [name], function(err) {
              return done(err);
            });
          }, function(done) {
            var sql;
            sql = "delete from " + self.settings.table + " where container = $1";
            return currentClient.query(sql, [name], done);
          }
        ], function(err, res) {
          if (err) {
            return currentClient.query('ROLLBACK TRANSACTION', function() {
              return closeConnection(err);
            });
          } else {
            return currentClient.query('COMMIT TRANSACTION', function(err) {
              return closeConnection(err, res);
            });
          }
        });
      });
    })["catch"](closeConnection);
  };

  PostgresStorage.prototype.upload = function(container, req, res, callback) {
    var busboy, promises, self;
    self = this;
    busboy = new Busboy({
      headers: req.headers
    });
    promises = [];
    busboy.on('file', function(fieldname, file, filename, encoding, mimetype) {
      return promises.push(new Promise(function(resolve, reject) {
        var options;
        options = {
          container: container,
          filename: filename,
          mimetype: mimetype
        };
        return self.uploadFile(container, file, options, function(err, res) {
          if (err) {
            return reject(err);
          }
          return resolve(res);
        });
      }));
    });
    busboy.on('finish', function() {
      return Promise.all(promises).then(function(res) {
        return callback(null, res);
      })["catch"](callback);
    });
    req.pipe(busboy);
  };

  PostgresStorage.prototype.uploadFile = function(container, file, options, callback) {
    var closeConnection, currentClient, handleError, self;
    if (callback == null) {
      callback = (function() {});
    }
    self = this;
    currentClient = null;
    closeConnection = function(err, res) {
      if (currentClient != null) {
        currentClient.release();
      }
      return callback(err, res);
    };
    handleError = function(err) {
      return currentClient.query('ROLLBACK TRANSACTION', function() {
        return closeConnection(err);
      });
    };
    self.db.connect().then(function(client) {
      currentClient = client;
      return currentClient.query('BEGIN TRANSACTION', function(err) {
        var bufferSize, man;
        if (err) {
          return closeConnection(err);
        }
        bufferSize = 16384;
        man = new LargeObjectManager(currentClient);
        return man.createAndWritableStream(bufferSize, function(err, objectid, stream) {
          if (err) {
            return handleError(err);
          }
          stream.on('finish', function() {
            return currentClient.query("insert into " + self.settings.table + " (container, filename, mimetype, objectid) values ($1, $2, $3, $4) RETURNING *", [options.container, options.filename, options.mimetype, objectid], function(err, res) {
              if (err) {
                return handleError(err);
              }
              return currentClient.query('COMMIT TRANSACTION', function(err) {
                if (err) {
                  return handleError(err);
                }
                return closeConnection(null, res.rows[0]);
              });
            });
          });
          stream.on('error', handleError);
          return file.pipe(stream);
        });
      });
    })["catch"](closeConnection);
  };

  PostgresStorage.prototype.getFiles = function(container, callback) {
    this.db.query("select * from " + this.settings.table + " where container = $1", [container], callback);
  };

  PostgresStorage.prototype.removeFile = function(container, filename, callback) {
    var self;
    self = this;
    self.getFile(container, filename, function(err, file) {
      if (err) {
        return callback(err);
      }
      return self.removeFileById(file.id, callback);
    });
  };

  PostgresStorage.prototype.removeFileById = function(id, callback) {
    var closeConnection, currentClient, self;
    self = this;
    currentClient = null;
    closeConnection = function(err, res) {
      if (currentClient != null) {
        currentClient.release();
      }
      return callback(err, res);
    };
    self.db.connect().then(function(client) {
      currentClient = client;
      return currentClient.query('BEGIN TRANSACTION', function(err) {
        if (err) {
          return closeConnection(err);
        }
        return async.waterfall([
          function(done) {
            var sql;
            sql = "select lo_unlink(objectid) from " + self.settings.table + " where id = $1";
            return currentClient.query(sql, [id], function(err) {
              return done(err);
            });
          }, function(done) {
            var sql;
            sql = "delete from " + self.settings.table + " where id = $1";
            return currentClient.query(sql, [id], done);
          }
        ], function(err, res) {
          if (err) {
            return currentClient.query('ROLLBACK TRANSACTION', function() {
              return closeConnection(err);
            });
          } else {
            return currentClient.query('COMMIT TRANSACTION', function(err) {
              return closeConnection(err, res);
            });
          }
        });
      });
    })["catch"](closeConnection);
  };

  PostgresStorage.prototype.getFileById = function(currentClient, id, callback) {
    currentClient.query("select * from " + this.settings.table + " where id = $1", [id], function(err, res) {
      if (err) {
        return callback(err);
      }
      if (!res || !res.rows || res.rows.length === 0) {
        err = new Error('File not found');
        err.status = 404;
        return callback(err);
      }
      return callback(null, res.rows[0]);
    });
  };

  PostgresStorage.prototype.getFile = function(container, filename, callback) {
    this.db.query("select * from " + this.settings.table + " where container = $1 and filename = $2", [container, filename], function(err, res) {
      if (err) {
        return callback(err);
      }
      if (!res || !res.rows || res.rows.length === 0) {
        err = new Error('File not found');
        err.status = 404;
        return callback(err);
      }
      return callback(null, res.rows[0]);
    });
  };

  PostgresStorage.prototype._stream = function(client, file, res, callback) {
    var bufferSize, man;
    bufferSize = 16384;
    man = new LargeObjectManager(client);
    return man.openAndReadableStream(file.objectid, bufferSize, function(err, size, stream) {
      if (err) {
        return callback(err);
      }
      stream.on('error', callback);
      stream.on('end', callback);
      res.set('Content-Disposition', "attachment; filename=\"" + file.filename + "\"");
      res.set('Content-Type', file.mimetype);
      res.set('Content-Length', size);
      return stream.pipe(res);
    });
  };

  PostgresStorage.prototype.downloadById = function(id, res, callback) {
    var closeConnection, currentClient, self;
    if (callback == null) {
      callback = (function() {});
    }
    self = this;
    currentClient = null;
    closeConnection = function(err, res) {
      if (currentClient != null) {
        currentClient.release();
      }
      return callback(err, res);
    };
    self.db.connect().then(function(client) {
      currentClient = client;
      return currentClient.query('BEGIN TRANSACTION', function(err) {
        if (err) {
          return closeConnection(err);
        }
        return self.getFileById(currentClient, id, function(err, file) {
          if (err) {
            return closeConnection(err);
          }
          return self._stream(currentClient, file, res, closeConnection);
        });
      });
    })["catch"](closeConnection);
  };

  PostgresStorage.prototype.download = function(container, filename, res, callback) {
    var closeConnection, currentClient, self;
    if (callback == null) {
      callback = (function() {});
    }
    self = this;
    currentClient = null;
    closeConnection = function(err, res) {
      if (currentClient != null) {
        currentClient.release();
      }
      return callback(err, res);
    };
    self.db.connect().then(function(client) {
      currentClient = client;
      return currentClient.query('BEGIN TRANSACTION', function(err) {
        if (err) {
          return closeConnection(err);
        }
        return self.getFile(container, filename, function(err, file) {
          if (err) {
            return closeConnection(err);
          }
          return self._stream(currentClient, file, res, closeConnection);
        });
      });
    })["catch"](closeConnection);
  };

  return PostgresStorage;

})();

PostgresStorage.modelName = 'storage';

PostgresStorage.prototype.getContainers.shared = true;

PostgresStorage.prototype.getContainers.accepts = [];

PostgresStorage.prototype.getContainers.returns = {
  arg: 'containers',
  type: 'array',
  root: true
};

PostgresStorage.prototype.getContainers.http = {
  verb: 'get',
  path: '/'
};

PostgresStorage.prototype.getContainer.shared = true;

PostgresStorage.prototype.getContainer.accepts = [
  {
    arg: 'container',
    type: 'string'
  }
];

PostgresStorage.prototype.getContainer.returns = {
  arg: 'containers',
  type: 'object',
  root: true
};

PostgresStorage.prototype.getContainer.http = {
  verb: 'get',
  path: '/:container'
};

PostgresStorage.prototype.destroyContainer.shared = true;

PostgresStorage.prototype.destroyContainer.accepts = [
  {
    arg: 'container',
    type: 'string'
  }
];

PostgresStorage.prototype.destroyContainer.returns = {};

PostgresStorage.prototype.destroyContainer.http = {
  verb: 'delete',
  path: '/:container'
};

PostgresStorage.prototype.upload.shared = true;

PostgresStorage.prototype.upload.accepts = [
  {
    arg: 'container',
    type: 'string'
  }, {
    arg: 'req',
    type: 'object',
    http: {
      source: 'req'
    }
  }, {
    arg: 'res',
    type: 'object',
    http: {
      source: 'res'
    }
  }
];

PostgresStorage.prototype.upload.returns = {
  arg: 'result',
  type: 'object'
};

PostgresStorage.prototype.upload.http = {
  verb: 'post',
  path: '/:container/upload'
};

PostgresStorage.prototype.getFiles.shared = true;

PostgresStorage.prototype.getFiles.accepts = [
  {
    arg: 'container',
    type: 'string'
  }
];

PostgresStorage.prototype.getFiles.returns = {
  arg: 'file',
  type: 'array',
  root: true
};

PostgresStorage.prototype.getFiles.http = {
  verb: 'get',
  path: '/:container/files'
};

PostgresStorage.prototype.getFile.shared = true;

PostgresStorage.prototype.getFile.accepts = [
  {
    arg: 'container',
    type: 'string'
  }, {
    arg: 'file',
    type: 'string'
  }
];

PostgresStorage.prototype.getFile.returns = {
  arg: 'file',
  type: 'object',
  root: true
};

PostgresStorage.prototype.getFile.http = {
  verb: 'get',
  path: '/:container/files/:file'
};

PostgresStorage.prototype.removeFile.shared = true;

PostgresStorage.prototype.removeFile.accepts = [
  {
    arg: 'container',
    type: 'string'
  }, {
    arg: 'file',
    type: 'string'
  }
];

PostgresStorage.prototype.removeFile.returns = {};

PostgresStorage.prototype.removeFile.http = {
  verb: 'delete',
  path: '/:container/files/:file'
};

PostgresStorage.prototype.download.shared = true;

PostgresStorage.prototype.download.accepts = [
  {
    arg: 'container',
    type: 'string'
  }, {
    arg: 'file',
    type: 'string'
  }, {
    arg: 'res',
    type: 'object',
    http: {
      source: 'res'
    }
  }
];

PostgresStorage.prototype.download.http = {
  verb: 'get',
  path: '/:container/download/:file'
};

exports.initialize = function(dataSource) {
  var connector, k, m, method, opt, ref, settings;
  settings = dataSource.settings || {};
  connector = new PostgresStorage(settings);
  dataSource.connector = connector;
  dataSource.connector.pg = dataSource.connector.db;
  dataSource.connector.dataSource = dataSource;
  connector.DataAccessObject = function() {};
  ref = PostgresStorage.prototype;
  for (m in ref) {
    method = ref[m];
    if (_.isFunction(method)) {
      connector.DataAccessObject[m] = method.bind(connector);
      for (k in method) {
        opt = method[k];
        connector.DataAccessObject[m][k] = opt;
      }
    }
  }
  connector.define = function(model, properties, settings) {};
};
