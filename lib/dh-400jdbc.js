// module dependencies
var _ = require('underscore');
var EventEmitter = require('events').EventEmitter;
var java = require('java');
var sys = require('sys');
var path = require('path');

// driver info
var driverPath = path.join(__dirname + '/../driver/jt400.jar');
var driverName = 'com.ibm.as400.access.AS400JDBCDriver';


function trim1(str) {
    if (str) return str.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
    return;
}

/**
 * Constructor for the JDBCConn object.
 * @constructor
 */
function JDBCConn() {
    EventEmitter.call(this);
    this._config = {};
    this._conn = null;
}

sys.inherits(JDBCConn, EventEmitter);

/**
 * Kills the jdbc connection.
 */
JDBCConn.prototype.kill = function () {
    // set config and connection to null.
    this._config = null;
    this._conn = null;
};

/**
 * Initializes the jdbc connection.
 * @param config
 */
JDBCConn.prototype.initialize = function (config) {
    var self = this;
    self._config = config;

    var minPoolSize = self._config.minpoolsize | 5;
    java.classpath.push(driverPath);
    java.newInstance(driverName, function (err, driver) {
        if (err) {
            self.emit('init', err, null);
        } else {
            java.callStaticMethod('java.sql.DriverManager', 'registerDriver', driver, function (err, result) {
                if (err) {
                    self.emit('init', err);
                }
                else {
                    try {
                        // set a login timeout.
                        java.callStaticMethodSync('java.sql.DriverManager', 'setLoginTimeout', 3);

                        // get the connection.
                        var conn = java.callStaticMethodSync('java.sql.DriverManager', 'getConnection', self._config.url);

                        // check that connection is set.
                        if (conn) {
                            self._conn = conn;
                            self.emit('init', null);
                        }
                        else { // connections not set. emit and error.
                            self.emit('init', err);
                        }
                    }
                    catch (error) {
                        self.emit('init', error);
                    }
                }
            });
        }
    });
};

/**
 * Closes the connection.
 */
JDBCConn.prototype.close = function () {
    var self = this;
    self.emit('close', null);

    if (self._conn) {
        self._conn.close();
    }
};

JDBCConn.prototype.prepareStatement = function (sql) {
    var self = this;
    var args = arguments;
    if (self._conn === undefined) {
        self.emit('close');
    } else {

        self._conn.prepareStatement(sql, function (err, statement) {

            if (err) {
                self.emit('prepareStatement', err, null);
            }
            else {
                var index = 1;
                var setObjectCallback = function () {
                    statement.setObject(index, args[index], function () {
                        if (index + 1 == args.length) {
                            executeCallback();
                        }
                        else {
                            index++;
                            setObjectCallback();
                        }
                    });
                };

                if(args.length > 1){
                    setObjectCallback();
                }

                var executeCallback = function () {
                    statement.executeQuery(function (err, resultset) {
                        if (err) {
                            self.emit('prepareStatement', err, null);
                        }
                        else {
                            if (resultset) {
                                resultset.getMetaData(function (err, rsmd) {
                                    if (err) {
                                        self.emit('prepareStatement', err, null);
                                    } else {
                                        var cc = rsmd.getColumnCountSync();
                                        var results = [];
                                        var next = resultset.nextSync();

                                        while (next) {
                                            var row = {};

                                            for (var i = 1; i <= cc; i++) {
                                                var colname = rsmd.getColumnNameSync(i);
                                                row[colname] = trim1(resultset.getStringSync(i));
                                            }
                                            results.push(row);
                                            next = resultset.nextSync();
                                        }
                                        self.emit('prepareStatement', null, results);
                                    }
                                });
                            }
                            else {
                                self.emit('prepareStatement', null, null);
                            }
                        }
                    });
                };
            }
        });
    }
};

JDBCConn.prototype.execute = function (sql) {
    var self = this;

    if (self._conn === undefined) {
        self.emit('close');
    } else {

        self._conn.createStatement(function (err, statement) {
            if (err) {
                self.emit('execute', err, null);
            } else {
                statement.executeQuery(sql, function (err, rs) {
                    var resultset = rs;
                    if (err) {
                        self.emit('execute', err, null);
                    } else {
                        resultset.getMetaData(function (err, rsmd) {
                            if (err) {
                                self.emit('execute', err, null);
                            } else {
                                var cc = rsmd.getColumnCountSync();
                                var results = [];
                                var next = resultset.nextSync();

                                while (next) {
                                    var row = {};

                                    for (var i = 1; i <= cc; i++) {
                                        var colname = rsmd.getColumnNameSync(i);
                                        row[colname] = trim1(resultset.getStringSync(i));
                                    }
                                    results.push(row);
                                    next = resultset.nextSync();
                                }
                                self.emit('execute', null, results);
                            }
                        });
                    }
                });
            }
        });
    }
};

module.exports = function() {
    return new JDBCConn();
};