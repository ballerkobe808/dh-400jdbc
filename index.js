'use strict';

// Get a reference to the jdbc driver.
const JDBC = require('./lib/jdbc');
const sqlTypes = require('./lib/constants/sql-types').types;
const parameterTypes = require('./lib/constants/parameter-types');

// driver.
let jdbc = null;

// Prevention flag.
let preventQueries = false;

// Reference to the logger. Defaults to console.
// NOTE: This assumes the logger has a .info, .warn, and .error function.
let logger = console;

/**
 * Sets the prevent queries flag.
 * @param prevent - The value.
 */
exports.setPreventQueries = (prevent) => {
  preventQueries = prevent;
};

/**
 * Gets the prevent queries flag.
 * @return {boolean}
 */
exports.getPreventQueries = () => {
  return preventQueries;
};

// add the sql types and parameter types mapping objects as an exports properties.
exports.sqlTypes = sqlTypes;
exports.parameterTypes = parameterTypes;

/**
 * Configures the driver.
 * @param serverName - The server name.
 * @param libraries - The libraries to connect to.
 * @param user - The user name.
 * @param password - The password.
 * @param initialPoolCount - The initial number of connections you want to allocate in the connection pool.
 */
exports.configure = (serverName, libraries, user, password, initialPoolCount) => {
  config = {
    serverName: serverName,
    libraries: libraries,
    user: user,
    password: password,
    initialPoolCount: initialPoolCount
  };
};

/**
 * Getter function for the jdbc driver.
 * @return {JDBCConn}
 */
exports.getDriver = () => {
  return jdbc;
};

/**
 * Sets the logger reference.
 * @param loggerRef - The logger reference.
 */
exports.setLogger = (loggerRef) => {
  logger = loggerRef;
  jdbc.setLogger(logger);
};

/**
 * Sets the config object to the one specified.
 * @param configObject - The driver config object.
 */
exports.setConfig = (configObject) => {
  config = configObject;
};

/**
 * Closes all connections in the pool.
 */
exports.closeAll = () => {
  jdbc.close();
};

/**
 * Performs data insert on AS400.
 * @param tableName - The name of the table to insert to.
 * @param parameters - The parameters object of key/value pairs.
 * @param callback - The callback function. callback(err);
 */
exports.insertData = (tableName, parameters, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  // build the statement and params.
  let sql = buildInsertStatement(tableName, parameters);
  let values = objectToValueArray(parameters);

  // execute the statement.
  exports.executeUpdatePreparedStatement(sql, values, (err) => {
    // check if an error occurred.
    if (err) {
      return callback(err);
    }

    return callback();
  });
};

/**
 * Initializes the jdbc connection.
 * @param callback
 */
exports.initializeConnection = (callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  // initialize the connection.
  jdbc.initialize(config, callback);
};

/**
 * Initializes the jdbc connection.
 * @param conf - the config object to use.
 * @param callback
 */
exports.initializeConnectionWithConfig = (conf, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  // initialize the connection.
  jdbc.initialize(conf, callback);
};

/**
 * Re-initializes the jdbc connection pool.
 * @param newConfig - The new config object.
 * @param callback
 */
exports.reInitialize = (newConfig, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  // reinitialize the connection.
  jdbc.reInitialize(newConfig, callback);
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param callback - The finished callback function when all rows have been processed.
 */
exports.executeSqlString = (sql, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, (err) => {
    if (err) {
      return callback(err);
    }

    // execute the statement.
    jdbc.execute(sql, callback);
  });
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param parameters
 * @param callback - The finished callback function when all rows have been processed.
 */
exports.executePreparedStatement = (sql, parameters, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, (err) => {
    if (err) {
      return callback(err);
    }

    jdbc.prepareStatementWithParameterArray(sql, parameters, callback);
  });
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param parameters
 * @param callback
 */
exports.executeUpdatePreparedStatement = (sql, parameters, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, (err) => {
    if (err) {
      return callback(err);
    }

    jdbc.prepareStatementUpdateWithParameterArray(sql, parameters, callback);
  });
};

/**
 * Executes a prepared statement on the AS400 using an existing connection.
 * @param connection
 * @param sql
 * @param parameters
 * @param callback
 */
exports.executeUpdateStatementInTransaction = (connection, sql, parameters, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, (err) => {
    if (err) {
      return callback(err);
    }

    jdbc.prepareStatementUpdateWithParameterArrayInTransaction(connection, sql, parameters, callback);
  });
};

/**
 * Executes a stored procedure.
 * @param sql - The procedure call.
 * @param parameters - Array of parameter objects.
 * @param callback - The finished callback function.
 */
exports.executeStoredProcedure = (sql, parameters, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, (err) => {
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
exports.createSPInputParameter = (value) => {
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
exports.createSPOutputParameter = (sqlDataType, fieldName) => {
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
exports.runTransaction = (executeFunction, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  jdbc.initialize(config, (err) => {
    if (err) {
      return callback(err);
    }

    jdbc.runTransaction(executeFunction, callback);
  });
};
