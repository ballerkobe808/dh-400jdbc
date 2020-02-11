// Module Dependencies.
const _ = require('lodash');
const EventEmitter = require('events').EventEmitter;
const java = require('java');
const async = require('async');
const parameterTypes = require('./constants/parameter-types').parameterTypes;
const util = require('util');

// Driver Information.
const driverPath = `${__dirname}/../driver/jt400.jar`;
const driverName = 'com.ibm.as400.access.AS400JDBCDriver';
const connectionPoolDataSourceName = 'com.ibm.as400.access.AS400JDBCConnectionPoolDataSource';
const connectionPoolName = 'com.ibm.as400.access.AS400JDBCConnectionPool';
const pooledConnectionName = 'javax.sql.PooledConnection';

/**
 * Helper function.
 **/
function trimValue(str) {
  // default result to null.
  let result = null;

  // if the value is set.
  if (str) {
    result = str.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
  }

  // return the result.
  return result;
}

/**
 * Converts a result set into an array or result objects.
 * @param rs - The result set object.
 * @param callback - The finished callback function.
 */
function getResultsFromResultSet(rs, callback) {
  // store the results.
  let results = [];
  let currentIndex = 0;

  // make sure the result set isn't empty.
  if (!rs) {
    return callback(null, results);
  }

  // get the meta data from the result set.
  rs.getMetaData((err, rsmd) => {
    // check if an error occurred.
    if (err) {
      return callback(err);
    }

    try {
      // build a results array object.
      let cc = rsmd.getColumnCountSync();
      let next = rs.nextSync();

      // loop over all the result rows.
      while (next) {
        // create an object out of the row.
        let row = {};

        for (let i = 1; i <= cc; i++) {
          let colname = rsmd.getColumnNameSync(i);
          row[colname] = trimValue(rs.getStringSync(i));
        }

        // add the current index.
        row['index'] = currentIndex;
        currentIndex++;

        // add the row object to the results array.
        results.push(row);

        // increment the pointer to the next row.
        next = rs.nextSync();
      }
    }
    catch (ex) {
      return callback(ex);
    }

    // return results.
    return callback(null, results);
  });
}

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

util.inherits(JDBCConn, EventEmitter);

/**
 * Builds the insert statement with placeholders using a key value parameter list.
 * @param tableName - The name of the table to insert into.
 * @param parameters - The key value parameter array.
 * @returns {string} - The sql string.
 */
JDBCConn.prototype.buildInsertStatement  = function (tableName, parameters) {
  let sql = `INSERT INTO ${tableName}`;

  // check that there are parameters.
  if (Object.keys(parameters).length) {
    // build the parameter lists.
    let columnNamesString = '';
    let valuePlaceHolders = '';

    // keep track of the current index.
    let index = 0;

    // loop over all the keys.
    for (let key in parameters) {
      columnNamesString += key;
      valuePlaceHolders += '?';

      // increment the index.
      index++;

      // if its not the last parameter, add a comma.
      if (index !== Object.keys(parameters).length) {
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
JDBCConn.prototype.objectToValueArray = (obj) => {
  let results = [];

  for (let key in obj) {
    if (obj.hasOwnProperty(key)) {
      results.push(obj[key]);
    }
  }

  return results;
};

/**
 * Sets debug mode.
 * @param debug - The flag.
 */
JDBCConn.prototype.setDebugMode = (debug) => {
  this._debug = debug;
};

/**
 * Kills the jdbc connection.
 */
JDBCConn.prototype.kill = () => {
  // set config and connection to null.
  this._config = null;
  this._connectionPoolDataSource = null;
  this._connectionPool = null;
};

/**
 * Checks if the driver has already been initialized yet.
 * @returns {{}|*|null}
 */
JDBCConn.prototype.isInitialized = () => {
  return (this._config && this._connectionPoolDataSource && this._connectionPool);
};

/**
 * Initializes the AS400 JDBC Connection Pool.
 * @param config
 * @param cb
 */
JDBCConn.prototype.initialize =  (config, cb) => {
  // if the driver has already been initialized. Skip this.
  if (this.isInitialized()) {
    return cb();
  }

  // add the driver classpath to the java runtime environment.
  java.classpath.push(driverPath);

  // save the config info.
  this._config = config;

  // save a reference to the this object so we can access it inside the callback function.
  let self = this;
  let callback = cb;

  // initialize the pooled data source.
  // create a new instance of the connection pool data source class.
  java.newInstance(connectionPoolDataSourceName, config.serverName, config.user, config.password, (err, connectionPoolDataSource) => {
    if (err) {
      self.kill();
      return callback(err);
    }

    // save the connection pool data source reference.
    self._connectionPoolDataSource = connectionPoolDataSource;

    try {
      self._connectionPoolDataSource.setLibrariesSync(config.libraries);
      self._connectionPoolDataSource.setSecureSync(self._config.secure);

      java.newInstance(connectionPoolName, self._connectionPoolDataSource, (err, connectionPool) => {
        // check for an error.
        if (err) {
          self.kill();
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
          self.kill();
          return callback(ex);
        }

      });
    }
    catch(ex) {
      self.kill();
      return callback(ex);
    }
  });
};

/**
 * Closes the connection.
 */
JDBCConn.prototype.close = (callback) => {
  let self = this;
  self.removeAllListeners();

  // if the connection pool isn't set. Skip closing all connections.
  if (!self._connectionPool) {
    self.kill();
    return callback();
  }

  try {
    self._connectionPool.closeSync();
    self.kill();
    callback();
  }
  catch (ex) {
    self.error(ex);
    self.kill();
    callback(ex);
  }
};

/**
 * Re-initializes the jdbc connection pool.
 */
JDBCConn.prototype.reInitialize = (newConfig, callback) => {
  let self = this;

  // attempt to close all the current connections.
  self.close((err) => {
    if (err) {
      self.error(err);
    }

    // reinitialize with the new config.
    self.initialize(newConfig, (er) => {
      return callback(er);
    });
  });
};

/**
 * Create and runs a prepared sql select statement.
 * @param sql - The SQL statement.
 * @param parameters - The parameters to set on the sql statement.
 * @param finished - The finished callback function.
 */
JDBCConn.prototype.prepareStatementWithParameterArray = (sql, parameters, finished) => {
  // save a reference to the instance object.
  let self = this;

  // make sure the driver is initialized before executing sql.
  if (!self.isInitialized()) {
    return finished(new Error('Driver not initialized.'));
  }

  // get a pooled connection.
  self._connectionPool.getConnection((err, connection) => {
    // check if an error occurred.
    if (err) {
      return finished(err);
    }

    // make sure auto commit is set to true.
    connection.setAutoCommitSync(true);

    // fire the prepared statement.
    connection.prepareStatement(sql, (err, statement) => {
      // if an error occurred.
      if (err) {
        // close the connection to return it to the pool.
        try {
          connection.closeSync();

          // if debug flag is set, show print statements.
          if (self._debug) {
            self.info('closing connection.');
            self.info(self._connectionPool.getActiveConnectionCountSync());
          }
        }
        catch(ex) {
          self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
        }

        return finished(err);
      }

      // loop over the parameters and run set object on them.
      for (let i = 1; i <= parameters.length; i++) {
        try {
          statement.setObjectSync(i, parameters[i - 1]);
        }
        catch (ex) {
          // close the connection to return it to the pool.
          try {
            connection.closeSync();

            // if debug flag is set, show print statements.
            if (self._debug) {
              self.info('closing connection.');
              self.info(self._connectionPool.getActiveConnectionCountSync());
            }
          }
          catch(ex) {
            self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
          }

          return finished(ex);
        }
      }

      // execute the query.
      statement.executeQuery((err, resultset) => {
        // check if an error occurred.
        if (err) {
          // close the connection to return it to the pool.
          try {
            connection.closeSync();

            // if debug flag is set, show print statements.
            if (self._debug) {
              self.info('closing connection.');
              self.info(self._connectionPool.getActiveConnectionCountSync());
            }
          }
          catch(ex) {
            self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
          }

          return finished(err);
        }

        // get the results back.
        getResultsFromResultSet(resultset, (err, results) => {
          // close the connection to return it to the pool.
          try {
            connection.closeSync();

            // if debug flag is set, show print statements.
            if (self._debug) {
              self.info('closing connection.');
              self.info(self._connectionPool.getActiveConnectionCountSync());
            }
          }
          catch(ex) {
            self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
          }

          return finished(err, results);
        });
      });
    });
  });
};

/**
 * Executes a sql statement.
 * @param sql - The sql query.
 * @param finished - The finished callback function.
 * @returns {*}
 */
JDBCConn.prototype.execute = (sql, finished) => {
  // save references to access in callbacks later.
  let self = this;

  // check that the driver is initialized.
  if (!self.isInitialized()) {
    return finished(new Error('Driver not initialized.'));
  }

  // get a pooled connection.
  self._connectionPool.getConnection((err, connection) => {
    // check if an error occurred.
    if (err) {
      return finished(err);
    }

    // make sure auto commit is set to true.
    connection.setAutoCommitSync(true);

    connection.createStatement((err, statement) => {
      if (err) {
        // close the connection to return it to the pool.
        try {
          connection.closeSync();

          // if debug flag is set, show print statements.
          if (self._debug) {
            self.info('closing connection.');
            self.info(self._connectionPool.getActiveConnectionCountSync());
          }
        }
        catch(ex) {
          self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
        }

        return finished(err);
      }

      // execute the sql query.
      statement.executeQuery(sql, (err, rs) => {
        // check for an error.
        if (err) {
          return finished(err);
        }

        // get the results back.
        getResultsFromResultSet(rs, (err, results) => {
          // close the connection to return it to the pool.
          try {
            connection.closeSync();

            // if debug flag is set, show print statements.
            if (self._debug) {
              self.info('closing connection.');
              self.info(self._connectionPool.getActiveConnectionCountSync());
            }
          }
          catch(ex) {
            self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
          }

          return finished(err, results);
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
JDBCConn.prototype.prepareStatementUpdateWithParameterArray = (sql, parameters, cb) => {
  // save references so you can use them in the callbacks later.
  let self = this;
  let callback = cb;

  // make sure the driver is initialized.
  if (!self.isInitialized()) {
    return callback(new Error('Driver not initialized.'));
  }

  // get a pooled connection.
  self._connectionPool.getConnection((err, connection) => {
    if (err) {
      return callback(err);
    }

    // fire the prepared statement.
    connection.prepareStatement(sql, (err, statement) => {
      // if an error occurred.
      if (err) {
        // close the connection to return it to the pool.
        try {
          connection.closeSync();

          // if debug flag is set, show print statements.
          if (self._debug) {
            self.info('closing connection.');
            self.info(self._connectionPool.getActiveConnectionCountSync());
          }
        }
        catch(ex) {
          self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
        }

        return callback(err);
      }

      // make sure auto commit is set to true.
      connection.setAutoCommitSync(true);

      try {
        // loop over the parameters and run the set object command to add them to the sql statement.
        for (let i = 1; i <= parameters.length; i++) {
          statement.setObjectSync(i, parameters[i - 1]);
        }

        // execute the update statement.
        statement.executeUpdate((err) => {
          // close the connection to return it to the pool.
          try {
            connection.closeSync();

            // if debug flag is set, show print statements.
            if (self._debug) {
              self.info('closing connection.');
              self.info(self._connectionPool.getActiveConnectionCountSync());
            }
          }
          catch (ex) {
            self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
          }

          return callback(err);
        });
      }
      catch (ex) {
        return callback(ex);
      }
    });
  });
};

/**
 * Creates and fires an update, delete, insert statement.
 * @param connection - The connection to use.
 * @param sql - The sql statement.
 * @param parameters - The parameters for the statement.
 * @param callback - The callback function.
 */
JDBCConn.prototype.prepareStatementUpdateWithParameterArrayInTransaction = (connection, sql, parameters, callback) => {
  // fire the prepared statement.
  connection.prepareStatement(sql, (err, statement) => {
    // if an error occurred.
    if (err) {
      return callback(err);
    }

    try {
      // loop over the parameters and run the set object command to add them to the sql statement.
      for (let i = 1; i <= parameters.length; i++) {
        statement.setObjectSync(i, parameters[i - 1]);
      }

      // execute the update statement.
      statement.executeUpdate((err) => {
        return callback(err);
      });
    }
    catch (ex) {
      return callback(ex);
    }
  });
};


/**
 * Executes a stored procedure.
 * @param sql - The statement.
 * @param parameters - The input parameters for the stored procedure.
 * @param cb - The callback function.
 */
JDBCConn.prototype.executeStoredProcedure = (sql, parameters, cb) => {
  // save references so you can use them in the callbacks later.
  let self = this;
  let callback = cb;

  // make sure the driver is initialized.
  if (!self.isInitialized()) {
    return callback(new Error('Driver not initialized.'));
  }

  // get a pooled connection.
  self._connectionPool.getConnection((err, connection) => {
    if (err) {
      return callback(err);
    }

    // make sure auto commit is set to true.
    connection.setAutoCommitSync(true);

    // fire the prepared statement.
    connection.prepareCall(sql, (err, statement) => {
      // if an error occurred.
      if (err) {
        // close the connection to return it to the pool.
        try {
          connection.closeSync();

          // if debug flag is set, show print statements.
          if (self._debug) {
            self.info('closing connection.');
            self.info(self._connectionPool.getActiveConnectionCountSync());
          }
        }
        catch(ex) {
          self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
        }

        return callback(err);
      }

      try {
        // loop over the input parameters and run the set object command to add them to the procedure statement.
        for (let i = 1; i <= parameters.length; i++) {
          if (_.isEqual(parameters[i-1].type, parameterTypes.INPUT_PARAM)) {
            statement.setObjectSync(i, parameters[i - 1].value);
          }
          else if (_.isEqual(parameters[i-1].type, parameterTypes.OUTPUT_PARAM)) {
            statement.registerOutParameterSync(i, parameters[i - 1].dataType);
          }
          else {
            return callback(new Error('Invalid parameter type specified: ' + parameters[i-1].type));
          }
        }

        // execute the update statement.
        statement.execute((err) => {
          // check for an error.
          if (err) {
            // close the connection to return it to the pool.
            try {
              connection.closeSync();

              // if debug flag is set, show print statements.
              if (self._debug) {
                self.info('closing connection.');
                self.info(self._connectionPool.getActiveConnectionCountSync());
              }
            }
            catch(ex) {
              self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
            }

            return callback(err);
          }

          // build the result object out of the output parameters.
          let resultObject = {};

          // fill the result object.
          for (let i = 0; i < parameters.length; i++) {
            // if its an output parameter. Add the field and value to the result object.
            if (_.isEqual(parameters[i].type, parameterTypes.OUTPUT_PARAM)) {
              resultObject[parameters[i].fieldName] =  statement.getObjectSync(i+1);
            }
          }

          try {
            let rs = statement.getResultSetSync();
            let moreResults = (!_.isUndefined(rs) && !_.isNull(rs));

            if (moreResults) {
              resultObject.resultSets = [];
            }

            async.whilst(
              () => {
                return moreResults;
              },

              (cb) => {
                getResultsFromResultSet(rs, (err, results) => {
                  if (err) {
                    return cb(err);
                  }

                  if (results) {
                    resultObject.resultSets.push(results);
                  }

                  moreResults = statement.getMoreResultsSync();

                  if (moreResults) {
                    rs = statement.getResultSetSync();
                  }

                  return cb();
                });
              },

              (err) => {
                // close the connection to return it to the pool.
                try {
                  connection.closeSync();

                  // if debug flag is set, show print statements.
                  if (self._debug) {
                    self.info('closing connection.');
                    self.info(self._connectionPool.getActiveConnectionCountSync());
                  }
                }
                catch (ex) {
                  self.error('Failed to close jdbc connection: ' + JSON.stringify(ex));
                }

                // return the results.
                return callback(err, resultObject);
              }
            );
          }
          catch (ex) {
            self.error('Failed to get fetch size.');
            self.error(ex);

            // close the connection to return it to the pool.
            try {
              connection.closeSync();

              // if debug flag is set, show print statements.
              if (self._debug) {
                self.info('closing connection.');
                self.info(self._connectionPool.getActiveConnectionCountSync());
              }
            }
            catch(e) {
              self.error('Failed to close jdbc connection: ' + JSON.stringify(e));
            }

            // return the results.
            return callback(err, resultObject);
          }
        });
      }
      catch (ex) {
        self.error(ex);
        return callback(ex);
      }
    });
  });
};

/**
 * Executes a function as part of a transaction.
 * @param executeFunction - A function that contains call to the db that are part of the transaction.
 * @param callback - The finished callback function.
 */
JDBCConn.prototype.runTransaction = (executeFunction, callback) => {
  // save a reference.
  let self = this;

  // make sure the driver is initialized.
  if (!this.isInitialized()) {
    return callback(new Error('Driver not initialized.'));
  }

  // get a pooled connection.
  this._connectionPool.getConnection((err, connection) => {
    if (err) {
      return callback(err);
    }

    // make sure auto commit is set to true.
    connection.setAutoCommitSync(false);

    // fire the function that contains all the statements and give it the connection to use.
    executeFunction(connection, (err) => {
      try {
        if (err) {
          self.error(err);
          connection.rollback((e) => {
            if (e) {
              self.error(e);
            }

            connection.close((er) => {
              if (er) {
                self.error(er);
              }

              return callback(err);
            });
          });
        }
        else {
          connection.commit((e) => {
            if (e) {
              self.error(e);
            }

            connection.close((er) => {
              if (er) {
                self.error(er);
              }

              return callback();
            });
          });
        }
      }
      catch (e) {
        if (e) {
          self.error('Exception Thrown: ' + e);
        }

        return callback(e);
      }
    });
  });
};

/**
 * Sets the logger reference.
 * @param loggerRef
 */
JDBCConn.prototype.setLogger = (loggerRef) => {
  this._logger = loggerRef;
};

/**
 * Logs a message to the appropriate logger.
 * @param message
 */
JDBCConn.prototype.info = (message) => {
  if (this._logger) {
    try {
      this._logger.info(message);
      return;
    }
    catch(ex) {
      util.error('Logger does not have a .info function.');
    }
  }

  util.log(message);
};

/**
 * Logs a message to the appropriate logger.
 * @param message
 */
JDBCConn.prototype.warn = (message) => {
  if (this._logger) {
    try {
      this._logger.warn(message);
      return;
    }
    catch(ex) {
      util.error('Logger does not have a .info function.');
    }
  }

  util.log(message);
};

/**
 * Logs a message to the appropriate logger.
 * @param message
 */
JDBCConn.prototype.error = (message) => {
  if (this._logger) {
    try {
      this._logger.error(message);
      return;
    }
    catch(ex) {
      util.error('Logger does not have a .info function.');
    }
  }

  util.error(message);
};

// add the constructor to the module exports.
module.exports = JDBCConn;
