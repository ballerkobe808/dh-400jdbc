'use strict';

// Module Dependencies.
const _ = require('lodash');
const java = require('java');
const async = require('async');
const parameterTypes = require('./constants/parameter-types').parameterTypes;
const util = require('util');

// Driver Information.
const driverPath = `${__dirname}/../driver/jt400.jar`;
const connectionPoolDataSourceName = 'com.ibm.as400.access.AS400JDBCConnectionPoolDataSource';
const connectionPoolName = 'com.ibm.as400.access.AS400JDBCConnectionPool';

/**
 * JDBC object class.
 */
class JDBC {
  /**
   * Constructor.
   * @param options - The config options.
   */
  constructor(options) {
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
