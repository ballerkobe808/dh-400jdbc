'use strict';

// Get a reference to the jdbc driver.
const _ = require('lodash');
const JDBC = require('./lib/jdbc');
const sqlTypes = require('./lib/constants/sql-types').types;
const parameterTypes = require('./lib/constants/parameter-types');
const Utils = require('./lib/utilities');

// driver.
let jdbc = null;

// Prevention flag.
let preventQueries = false;

// Reference to the logger. Defaults to console.
// NOTE: This assumes the logger has a .info, .warn, and .error function.
let logger = console;

// add the sql types and parameter types mapping objects as an exports properties.
exports.sqlTypes = sqlTypes;
exports.parameterTypes = parameterTypes;

//======================================================================================
// Getters and Setters.
//======================================================================================

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

//======================================================================================
// Configuration Functions.
//======================================================================================

/**
 * Initializes the jdbc connection.
 * @param options - The db options.
 * @param callback - The finished callback function.
 */
exports.initialize = (options, callback) => {
  // if logger is set.
  if (_.has(options, 'logger')) {
    logger = options.logger;
  }

  // set the jdbc driver.
  jdbc = new JDBC({
    host: options.host,
    libraries: options.libraries,
    username: options.username,
    password: options.password,
    logger: logger,
    initialPoolCount: options.initialPoolCount
  });

  // connect.
  jdbc.connect(callback);
};

/**
 * Closes all connections in the pool.
 * @param callback - The finished callback function.
 */
exports.closeAll = (callback) => {
  jdbc.close(callback);
};

//======================================================================================
// Query and Statement Functions.
//======================================================================================

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

  // execute the query.
  jdbc.executeQuery(sql, [], callback);
};

/**
 * Executes a sql string on the AS400.
 * @param sql
 * @param parameters
 * @param callback - The finished callback function.
 */
exports.executePreparedStatement = (sql, parameters, callback) => {
  // check the prevent queries flag.
  if (preventQueries) {
    return callback(new Error('Prevent queries flag is on. Maintenance is being performed.'));
  }

  // execute the query.
  jdbc.executeQuery(sql, parameters, callback);
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

  // execute the statement.
  jdbc.executeStatement(sql, parameters, callback);
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

  // execute the statement.
  jdbc.executeStatementInTransaction(connection, sql, parameters, callback);
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

  // execute the procedure.
  jdbc.executeStoredProcedure(sql, parameters, callback);
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

  // execute the transaction.
  jdbc.runTransaction(executeFunction, callback);
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
  let sql = Utils.buildInsertStatement(tableName, parameters);
  let values = Utils.objectToValueArray(parameters);

  // execute the insert statement.
  jdbc.executeStatement(sql, values, callback);
};

//======================================================================================
// Create Parameter Functions.
//======================================================================================

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
