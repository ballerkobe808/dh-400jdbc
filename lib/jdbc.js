'use strict';

// Module Dependencies.
const _ = require('lodash');
const java = require('java');
const async = require('async');
const { INPUT_PARAM, OUTPUT_PARAM} = require('./constants/parameter-types');

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
   */
  close() {
    // if we aren't connected.
    if (!this.connected) {
      this.logger.info('Not connected to the AS400. Skipping close.');
    }

    try {
      // close the connection pool.
      this.pool.closeSync();
    }
    catch(ex) {
      this.logger.error('Failed to close the connection pool.');
    }
  }
}

// export the class.
module.exports = JDBC;

//======================================================================================
// Helper Functions.
//======================================================================================

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
function getResults(rs, callback) {
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
          let key = rsmd.getColumnNameSync(i);
          row[key] = trimValue(rs.getStringSync(i));
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
      return callback(null, results);
    }
    catch (ex) {
      return callback(ex);
    }
  });
}
