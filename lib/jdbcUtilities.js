// Get a reference to the jdbc driver.
var JDBCDriver = require('./dh-400jdbc');
var jdbc = new JDBCDriver();
var sqlTypes = require('./constants/sql-types').types;
var parameterTypes = require('./constants/parameter-types').parameterTypes;
var util = require('util');
var testJDBC = new JDBCDriver();

// JDBC config.
var config = {};
var testDriverConfig = {};

// Prevention flag.
var preventQueries = false;
exports.setPreventQueries = function (prevent) {
  preventQueries = prevent;
};

// Reference to the logger.
// NOTE: This assumes the logger has a .info, .warn, and .error function.
var logger = null;

// add the sql types and parameter types mapping objects as an exports properties.
exports.sqlTypes = sqlTypes;
exports.parameterTypes = parameterTypes;

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
 * Configures the test driver.
 * @param serverName - The server name.
 * @param libraries - The libraries to conenct to.
 * @param user - The user name.
 * @param password - The password.
 * @param secureConnection - If the connection is over SSL or not.
 */
exports.configureTestDriver = function (serverName, libraries, user, password, secureConnection) {
  testDriverConfig = {
    serverName: serverName,
    libraries: libraries,
    user: user,
    password: password,
    secure: secureConnection,
    initialPoolCount: 2
  };
};

/**
 * Sets the logger reference.
 * @param loggerRef
 */
exports.setLogger = function (loggerRef) {
  logger = loggerRef;
  jdbc.setLogger(logger);
};

/**
 * Sets the debug mode.
 * @param debugMode - Debug mode on flag.
 */
exports.setDebugMode = function (debugMode) {
  jdbc.setDebugMode(debugMode);
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
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

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
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(err);
    }

    return callback();
  });
};

/**
 * Initializes a test jdbc connection pool.
 * @param callback
 */
exports.initializeTestConnection = function (callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  testJDBC.initialize(testDriverConfig, function(err) {
    if (err) {
      return callback(err);
    }

    return callback();
  });
};

/**
 * Tests a connection to the database using the specified config.
 * @param sql - A query to run as the validation.
 * @param config - The config object.
 * @param callback - The finished callback function. callback(connected);
 */
exports.testConnection = function (sql, config, callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  try {
    testJDBC.initialize(testDriverConfig, function(err) {
      if (err) {
        return callback(err);
      }

      // execute the sql statement using the test connection.
      testJDBC.execute(sql, function (er, results) {
        var status = false;
        if (er || !results) {
          if (logger) {
            logger.error('Not connected to database.');
            logger.error(er);
          }
          else {
            util.error('Not connected to database.');
            util.error(er);
          }
        }
        else {
          status = true;
        }

        return callback(status);
      });
    });
  }
  catch (ex) {
    if (logger) {
      logger.error('Test connection to db failed.');
      logger.error(ex);
    }
    else {
      util.error('Test connection to db failed.');
      util.error(ex);
    }

    return callback(false);
  }
};

/**
 * Initializes the jdbc connection.
 * @param conf - the config object to use.
 * @param callback
 */
exports.initializeConnectionWithConfig = function(conf, callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(conf, function(err) {
    if (err) {
      return callback(err);
    }

    callback();
  });
};

/**
 * Reinitializes the jdbc connection pool.
 * @param newConfig - The new config object.
 * @param callback
 */
exports.reInitialize = function(newConfig, callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.reInitialize(newConfig, callback);
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param callback - The finished callback function when all rows have been processed.
 */
exports.executeSqlString = function (sql, callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(err);
    }

    jdbc.execute(sql, callback);
  });
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param parameters
 * @param callback - The finished callback funciton when all rows have been processed.
 */
exports.executePreparedStatement = function (sql, parameters, callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(err);
    }

    jdbc.prepareStatementWithParameterArray(sql, parameters, callback);
  });
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param callback
 */
exports.executeUpdatePreparedStatement = function (sql, parameters, callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(err);
    }

    jdbc.prepareStatementUpdateWithParameterArray(sql, parameters, callback);
  });
};

/**
 * Executes a prepared statement on the AS400 using an existing connection.
 * @param sql
 * @param callback
 */
exports.executeUpdateStatementInTransaction = function (connection, sql, parameters, callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(err);
    }

    jdbc.prepareStatementUpdateWithParameterArrayInTransaction(connection, sql, parameters, callback);
  });
};

/**
 * Executes a stored procedure.
 * @param sql - The precedure call.
 * @param parameters - Array of parameter objects.
 * @param callback - The finished callback function.
 */
exports.executeStoredProcedure = function (sql, parameters, callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(err);
    }

    jdbc.executeStoredProcedure(sql, parameters, callback);
  });
};

/**
 * Creates an input field parameter object for the stored procedure parameters array call.
 * @param value - The actual value.
 */
exports.createSPInputParameter = function(value) {
  return {
    type: parameterTypes.INPUT_PARAM,
    value: value
  };
};

/**
 * Creates an output parameter field object for the stored procedure parameters array call.
 * @param sqlDataType - The sql data type.
 * @param fieldName - The field name string.
 */
exports.createSPOutputParameter = function(sqlDataType, fieldName) {
  return {
    type: parameterTypes.OUTPUT_PARAM,
    fieldName: fieldName,
    dataType: sqlDataType
  };
};

/**
 * Runs a transaction on the database.
 * @param executeFunction - The function containing all the statements to be run in the transaction. executeFunction(connection, callback);
 * @param callback - The finished callback function.
 */
exports.runTransaction = function (executeFunction, callback) {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, function(err) {
    if (err) {
      return callback(err);
    }

    jdbc.runTransaction(executeFunction, callback);
  });
};