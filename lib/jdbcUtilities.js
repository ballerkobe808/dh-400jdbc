// Get a reference to the jdbc driver.
var jdbc = require('./dh-400jdbc')();

// JDBC config.
var config = {};

/**
 * Configures the driver.
 * @param serverName - The server name.
 * @param libraries - The libraries to conenct to.
 * @param user - The user name.
 * @param password - The password.
 * @param secureConnection - If the connection is over SSL or not.
 * @param initialPoolCount - The initial number of connections you want to allocate in the connection pool.
 */
exports.configure = function (serverName, libraries, user, password, secureConnection, initialPoolCount) {
  config = {
    serverName: serverName,
    libraries: libraries,
    user: user,
    password: password,
    secure: secureConnection,
    initialPoolCount: initialPoolCount
  };
};

/**
 * Sets the config object to the one specified.
 * @param configObject - The driver config object.
 */
exports.setConfig = function(configObject) {
  config = configObject;
};

/**
 * Closes all connections in the pool.
 * @param callback
 */
exports.closeAll = function(callback) {
  jdbc.close(function (err) {
    return callback(err);
  });
};

/**
 * Performs data insert on AS400.
 * @param tableName - The name of the table to insert to.
 * @param parameters - The parameters object of key/value pairs.
 * @param callback - The callback function. callback(err);
 */
exports.insertData = function (tableName, parameters, callback) {
  var sql = jdbc.buildInsertStatement(tableName, parameters);
  var values = jdbc.objectToValueArray(parameters);

  // execute the statement.
  this.executeUpdatePreparedStatement(sql, values, function(err) {
    // check if an error occurred.
    if (err) {
      return callback(err);
    }

    callback(null);
  });
};

/**
 * Initializes the jdbc connection.
 * @param callback
 */
exports.initializeConnection = function(callback) {
  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(errorUtilities.AS400Error('JDBC initialize failed.', err));
    }

    callback();
  });
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param callback
 */
exports.executeSqlString = function (sql, callback) {
  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(errorUtilities.AS400Error('JDBC initialize failed.', err));
    }

    jdbc.executeSql(sql, function (err, results) {
      if (err) {
        return callback(errorUtilities.AS400Error('Prepared statement failed.', err));
      }

      return callback(null, results);
    });
  });
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param callback
 */
exports.executePreparedStatement = function (sql, parameters, callback) {
  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(errorUtilities.AS400Error('JDBC initialize failed.', err));
    }

    jdbc.prepareStatementWithParameterArray(sql, parameters, function (err, results) {
      if (err) {
        return callback(errorUtilities.AS400Error('Prepared statement failed.', err));
      }

      return callback(null, results);
    });
  });
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param callback
 */
exports.executeUpdatePreparedStatement = function (sql, parameters, callback) {
  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(errorUtilities.AS400Error('JDBC initialize failed.', err));
    }

    jdbc.prepareStatementUpdateWithParameterArray(sql, parameters, function (err) {
      if (err) {
        return callback(errorUtilities.AS400Error('Prepared statement failed.', err));
      }

      return callback();
    });
  });
};