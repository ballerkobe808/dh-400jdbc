// Module Dependencies.
var _ = require('underscore');
var EventEmitter = require('events').EventEmitter;
var java = require('java');
var sys = require('sys');
var path = require('path');
var async = require('async');

// Driver Information.
var driverPath = path.join(__dirname + '/../driver/jt400.jar');
var driverName = 'com.ibm.as400.access.AS400JDBCDriver';
var connectionPoolDataSourceName = 'com.ibm.as400.access.AS400JDBCConnectionPoolDataSource';
var connectionPoolName = 'com.ibm.as400.access.AS400JDBCConnectionPool';
var pooledConnectionName = 'javax.sql.PooledConnection';

/**
 * Helper function.
 **/
function trim1(str) {
  var result = null;

  if (str) {
    result = str.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
  }

  return result;
}

/**
 * Converts a results set into an array or result objects.
 * @param rs - The result set object.
 * @param finished - The finished callback function.
 */
function getResultsFromResultSet(rs, finished) {
  // store the results.
  var results = [];

  // make sure the result set isnt empty.
  if (!rs) {
    return finished(null, []);
  }

  // get the meta data from the result set.
  rs.getMetaData(function (err, rsmd) {
    // check if an error occurred.
    if (err) {
      return finished(err);
    }

    try {
      // build a results array object.
      var cc = rsmd.getColumnCountSync();
      var next = rs.nextSync();
      var rowNumber = 0;

      // loop over all the result rows.
      while (next) {
        // create an object out of the row.
        var row = {};

        for (var i = 1; i <= cc; i++) {
          var colname = rsmd.getColumnNameSync(i);
          row[colname] = trim1(rs.getStringSync(i));
        }

        // add the row object to the results array.
        results.push(row);

        // increment the pointer to the next row.
        next = rs.nextSync();
        rowNumber++;
      }

      // fire the callback to return the result set.
      return finished(null, results);
    }
    catch (ex) {
      return finished(ex);
    }
  });
}

/**
 * Builds the insert statement with placeholders using a key value parameter list.
 * @param tableName - The name of the table to insert into.
 * @param parameters - The key value parameter array.
 * @returns {string} - The sql string.
 */
JDBCConn.prototype.buildInsertStatement  = function (tableName, parameters) {
  var sql = 'INSERT INTO ' + tableName;

  // check that there are parameters.
  if (Object.keys(parameters).length) {
    // build the parameter lists.
    var columnNamesString = '';
    var valuePlaceHolders = '';

    // keep track of the current index.
    var index = 0;

    // loop over all the keys.
    for (var key in parameters) {
      columnNamesString += key;
      valuePlaceHolders += '?';

      // increment the index.
      index++;

      // if its not the last parameter, add a comma.
      if (index != Object.keys(parameters).length) {
        columnNamesString += ', ';
        valuePlaceHolders += ', ';
      }
    }

    // connect it to the initial statement.
    sql += ' (' + columnNamesString + ') VALUES (' + valuePlaceHolders + ')';
  }

  return sql;
};

/**
 * Converts a key value array or object into an array of just the values.
 * Note: use this in combination with the buildInsertStatement function to keep the values and columns in order.
 * @param obj
 * @returns {Array}
 */
JDBCConn.prototype.objectToValueArray = function (obj) {
  var results = [];

  for (var key in obj) {
    results.push(obj[key]);
  }

  return results;
};

/**
 * Constructor for the JDBCConn object.
 * @constructor
 */
function JDBCConn() {
  EventEmitter.call(this);
  this._config = {};
  this._connectionPoolDataSource = null;
  this._connectionPool = null;
  this._debug = false;
}

sys.inherits(JDBCConn, EventEmitter);

JDBCConn.prototype.setDebugMode = function (debug) {
  this._debug = debug;
};

/**
 * Kills the jdbc connection.
 */
JDBCConn.prototype.kill = function () {
  // set config and connection to null.
  this._config = null;
  this._connectionPoolDataSource = null;
  this._connectionPool = null;
};

/**
 * Checks if the driver has already been initialized yet.
 * @returns {{}|*|null}
 */
JDBCConn.prototype.isInitialized = function() {
  return (this._config && this._connectionPoolDataSource && this._connectionPool);
};

/**
 * Initializes the AS400 JDBC Connection Pool.
 * @param config
 */
JDBCConn.prototype.initialize = function (config, cb) {
  // if the driver has already been initialized. Skip this.
  if (this.isInitialized()) {
    return cb();
  }

  // add the driver classpath to the java runtime environment.
  java.classpath.push(driverPath);

  // save the config info.
  this._config = config;

  // save a reference to the this object so we can access it inside the callback function.
  var self = this;
  var callback = cb;

  // initialize the pooled datasource.
  // create a new instance of the connection pool datasource class.
  java.newInstance(connectionPoolDataSourceName, config.serverName, config.user, config.password, function(err, connectionPoolDataSource) {
    if (err) {
      return callback(err);
    }

    // save the connection pool data source reference.
    self._connectionPoolDataSource = connectionPoolDataSource;

    try {
      self._connectionPoolDataSource.setLibrariesSync(config.libraries);
      self._connectionPoolDataSource.setSecureSync(self._config.secure);

      java.newInstance(connectionPoolName, self._connectionPoolDataSource, function (err, connectionPool) {
        // check for an error.
        if (err) {
          return callback(err);
        }

        // save the connection pool.
        self._connectionPool = connectionPool;

        // set the initial pool size.
        try {
          self._connectionPool.fillSync(self._config.initialPoolCount);
          return callback();
        }
        catch(ex) {
          return callback(ex);
        }

      });
    }
    catch(ex) {
      return callback(ex);
    }
  });
};

/**
 * Closes the connection.
 */
JDBCConn.prototype.close = function (callback) {
  var self = this;
  self.removeAllListeners();

  // if the connection pool isnt set. Skip closing all connections.
  if (!self._connectionPool) {
    return callback();
  }

  try {
    self._connectionPool.closeSync();
    callback();
  }
  catch (ex) {
    console.log(ex);
    callback(ex);
  }
};

/**
 * Create and runs a prepared sql select statement.
 * @param sql - The SQL statement.
 * @param parameters - The parameters to set on the sql statement.
 * @param finished - The finished callback function.
 */
JDBCConn.prototype.prepareStatementWithParameterArray = function (sql, parameters, finished) {
  // save a reference to the instance object.
  var self = this;

  // make sure the driver is initialized before executing sql.
  if (!self.isInitialized()) {
    return finished(new Error('Driver not initialized.'));
  }

  // get a pooled connection.
  self._connectionPool.getConnection(function (err, connection) {
    // check if an error occurred.
    if (err) {
      return finished(err);
    }

    //console.log(self._connectionPool.getActiveConnectionCountSync());

    // fire the prepared statement.
    connection.prepareStatement(sql, function (err, statement) {
      // if an error occurred.
      if (err) {
        return finished(err);
      }

      // loop over the parameters and run set object on them.
      for (var i = 1; i <= parameters.length; i++) {
        try {
          statement.setObjectSync(i, parameters[i - 1]);
        }
        catch (ex) {
          return finished(ex);
        }
      }

      // execute the query.
      statement.executeQuery(function (err, resultset) {
        // check if an error occurred.
        if (err) {
          return finished(err);
        }

        // get the results back.
        getResultsFromResultSet(resultset, function (err) {
          // close the connection to return it to the pool.
          try {
            connection.closeSync();

            // if debug flag is set, show print statements.
            if (self._debug) {
              console.log('closing connection.');
              console.log(self._connectionPool.getActiveConnectionCountSync());
            }
          }
          catch(ex) {
            console.log('Failed to close jdbc connection: ' + JSON.stringify(ex));
          }

          return finished(err);
        });
      });
    });
  });
};

/**
 * Executs a sql statment.
 * @param sql - The sql query.
 * @param finishhed - The finished callback funciton.
 * @returns {*}
 */
JDBCConn.prototype.execute = function (sql, rowCallback, finishhed) {
  // save references to access in callbacks later.
  var self = this;

  // check that the driver is initialized.
  if (!self.isInitialized()) {
    return finishhed(new Error('Driver not initialized.'));
  }

  // get a pooled connection.
  self._connectionPool.getConnection(function (err, connection) {
    // check if an error occurred.
    if (err) {
      return finishhed(err);
    }

    connection.createStatement(function (err, statement) {
      if (err) {
        return finishhed(err);
      }

      // execute the sql query.
      statement.executeQuery(sql, function (err, rs) {
        // check for an error.
        if (err) {
          return finishhed(err);
        }

        // get the results back.
        getResultsFromResultSet(rs, function (err, results) {
          // close the connection to return it to the pool.
          try {
            connection.closeSync();

            // if debug flag is set, show print statements.
            if (self._debug) {
              console.log('closing connection.');
              console.log(self._connectionPool.getActiveConnectionCountSync());
            }
          }
          catch(ex) {
            console.log('Failed to close jdbc connection: ' + JSON.stringify(ex));
          }

          return finishhed(err, results);
        });
      });
    });
  });
};

/**
 * Creates and fires an update, delete, insert statement.
 * @param sql - The sql statement.
 * @param parameters - The parameters for the statement.
 * @param cb - The callback function.
 */
JDBCConn.prototype.prepareStatementUpdateWithParameterArray = function (sql, parameters, cb) {
  // save references so you can use them in the callbacks later.
  var self = this;
  var callback = cb;

  // make sure the driver is initialized.
  if (!self.isInitialized()) {
    return callback(new Error('Driver not initialized.'));
  }

  // get a pooled connection.
  self._connectionPool.getConnection(function (err, connection) {
    if (err) {
      return callback(err);
    }

    try {
      // loop over the parameters and run the set object command to add them to the sql statement.
      for (var i = 1; i <= parameters.length; i++) {
        statement.setObjectSync(i, parameters[i - 1]);
      }

      // execute the update statement.
      statement.executeUpdate(function (err) {
        // close the connection to return it to the pool.
        try {
          connection.closeSync();

          // if debug flag is set, show print statements.
          if (self._debug) {
            console.log('closing connection.');
            console.log(self._connectionPool.getActiveConnectionCountSync());
          }
        }
        catch(ex) {
          console.log('Failed to close jdbc connection: ' + JSON.stringify(ex));
        }

        return callback(err);
      });
    }
    catch (ex) {
      return callback(ex);
    }
  });
};

// TODO: change this to return singleton instance.
module.exports = function() {
  return new JDBCConn();
};