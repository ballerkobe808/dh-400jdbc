'use strict';

// Module Dependencies.
const _ = require('lodash');
const java = require('java');
const { INPUT_PARAM, OUTPUT_PARAM} = require('./constants/parameter-types');
const Utils = require('./utilities');

// Driver Information.
const DRIVER_PATH             = `${__dirname}/../driver/jt400.jar`;
const POOL_DATA_SOURCE_NAME   = 'com.ibm.as400.access.AS400JDBCConnectionPoolDataSource';
const POOL_NAME               = 'com.ibm.as400.access.AS400JDBCConnectionPool';

// Java Setup.
java.classpath.push(DRIVER_PATH);

/**
 * JDBC object class.
 */
class JDBC {
  /**
   * Constructor.
   * @param options - The config options.
   */
  constructor(options) {
    // default values.
    this.poolDataSource = null;
    this.pool = null;
    this.initialPoolCount = 1;
    this.connected = false;
    this.logger = console;

    // save all the values on this object.
    _.forEach(Object.keys(options), (key) => {
      this[key] = options[key];
    });
  }

  /**
   * Initialized the connection pool to the DB.
   * @param callback - The finished callback function.
   */
  connect(callback) {
    // if already connection, skip.
    if (this.connected) {
      this.logger.info('Already connected to AS400. Skipping connect call.');
      return callback();
    }

    try {
      // initialize the pool data source.
      this.poolDataSource = java.newInstanceSync(POOL_DATA_SOURCE_NAME, this.host, this.user, this.password);
      this.poolDataSource.setLibrariesSync(this.libraries);

      // initialize the pool.
      this.pool = java.newInstanceSync(POOL_NAME, this.poolDataSource);

      // fill the pool.
      this.pool.fillSync(this.initialPoolCount);

      // return.
      return callback();
    }
    catch(ex) {
      this.logger.error(`Failed to connect to the AS400: ${ex.message}`);
      return callback(ex);
    }
  }

  /**
   * Closes the sql connections.
   * @param callback - The finished callback function.
   */
  close(callback) {
    // if we aren't connected.
    if (!this.connected) {
      this.logger.info('Not connected to the AS400. Skipping close.');
      return callback();
    }

    // close the connection pool.
    this.pool.close((err) => {
      if (err) {
        this.logger.error('Failed to close the connection pool');
      }

      // return.
      return callback(err);
    });
  }

  /**
   * Executes a prepared statement and return results.
   * @param sql - The sql string.
   * @param parameters - Parameters array.
   * @param callback - The finished callback function.
   */
  executeQuery(sql, parameters, callback) {
    // variables that need access in finally block.
    let connection = null;
    let statement = null;
    let rs = null;
    let error = null;
    let results = [];

    try {
      // get a connection.
      connection = this.pool.getConnectionSync();

      // set the auto commit to true.
      connection.setAutoCommitSync(true);

      // prepare the statement.
      statement = connection.prepareStatementSync(sql);

      // loop over the parameters and run set object on them.
      if (parameters && parameters.length > 0) {
        for (let i = 0; i < parameters.length; i++) {
          statement.setObjectSync(i + 1, parameters[i]);
        }
      }

      // run the statement and save the result set.
      rs = statement.executeQuerySync();

      // get the results.
      results = getResults(rs);
    }
    catch(ex) {
      this.logger.error(`Error in prepareStatement: ${ex.message}`);
      error = ex;
    }
    finally {
      try {
        if (rs) {
          rs.closeSync();
        }

        if (statement) {
          statement.closeSync();
        }

        if (connection) {
          connection.closeSync();
        }
      }
      catch(e) {
        // print the error.
        this.logger.error(e.message);
      }
      finally {
        // return the results.
        return callback(error, results);
      }
    }
  }

  /**
   * Executes an statement.
   * @param sql - The sql string.
   * @param parameters - The parameters array.
   * @param callback - The finished callback function.
   */
  executeStatement(sql, parameters, callback) {
    // variables that need access in finally block.
    let connection = null;
    let statement = null;
    let error = null;

    try {
      // get a connection.
      connection = this.pool.getConnectionSync();

      // set the auto commit to true.
      connection.setAutoCommitSync(true);

      // prepare the statement.
      statement = connection.prepareStatementSync(sql);

      // loop over the parameters and run set object on them.
      if (parameters && parameters.length > 0) {
        for (let i = 0; i < parameters.length; i++) {
          statement.setObjectSync(i + 1, parameters[i]);
        }
      }

      // run the statement and save the result set.
      statement.executeUpdateSync();
    }
    catch(ex) {
      this.logger.error(`Error in executeStatement: ${ex.message}`);
      error = ex;
    }
    finally {
      try {
        if (statement) {
          statement.closeSync();
        }

        if (connection) {
          connection.closeSync();
        }
      }
      catch(e) {
        // print the error.
        this.logger.error(e.message);
      }
      finally {
        // return the results.
        return callback(error);
      }
    }
  }

  /**
   * Executes a statement in a transaction.
   * @param connection - The DB connection object.
   * @param sql - The sql string.
   * @param parameters - The parameters array.
   * @param callback - the finished callback function.
   */
  executeStatementInTrasaction(connection, sql, parameters, callback) {
    // variables we need to access in finally.
    let error = null;
    let statement = null;

    try {
      // prepare the statement.
      statement = connection.prepareStatementSync(sql);

      // loop over the params and add them to the statement.
      if (parameters && parameters.length > 0) {
        for(let i = 0; i < parameters.length; i++) {
          statement.setObjectSync(i+1, parameters[i]);
        }
      }

      // execute the statement.
      statement.executeUpdateSync();

      // return.
      return callback();
    }
    catch (ex) {
      this.logger.error(`Error in executeStatementInTransaction: ${ex.message}`);
      error = ex;
    }
    finally {
      try {
        if (statement) {
          statement.closeSync();
        }
      }
      catch (e) {
        this.logger.error(e.message);
      }
      finally {
        return callback(error);
      }
    }
  }

  /**
   * Executes a stored procedure.
   * @param sql - The sql statement.
   * @param parameters - The parameters array.
   * @param callback - The finished callback function.
   */
  executeStoredProcedure(sql, parameters, callback) {
    //
  }
}

// export the class.
module.exports = JDBC;

//======================================================================================
// Helper Functions.
//======================================================================================

/**
 * Converts a result set into an array or result objects.
 * NOTE: This throws an exception. Needs to be handled in the calling function.
 * @param rs - The result set object.
 */
function getResults(rs) {
  // store the results.
  let results = [];
  let currentIndex = 0;

  // get the result set metadata.
  let rsmd = rs.getMetaDataSync();

  // build a results array object.
  let cc = rsmd.getColumnCountSync();
  let next = rs.nextSync();

  // loop over all the result rows.
  while (next) {
    // create an object out of the row.
    let row = {};

    // loop for each column.
    for (let i = 1; i <= cc; i++) {
      let key = rsmd.getColumnNameSync(i);
      row[key] = Utils.trimValue(rs.getStringSync(i));
    }

    // add the current index.
    row['index'] = currentIndex;
    currentIndex++;

    // add the row object to the results array.
    results.push(row);

    // increment the pointer to the next row.
    next = rs.nextSync();
  }

  // return results.
  return results;
}
