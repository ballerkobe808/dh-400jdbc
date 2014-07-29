DH-400JDBC
===========

JDBC Wrapper for the JT400 driver.


Instructions
=============

1) Require the module:

    var jdbc = require('dh-400jdbc');
    
2) Build the config object:

    var config = {
      serverName: 'myserver',
      libraries: 'MYLIB',
      user: 'usernmame',
      password: 'password',
      secure: false,
      initialPoolCount: 10
    };
    
    jdbc.configure(config);
    
3) Initialize the connection:

    jdbc.initialize(config, function (err) {
      if (err) {
        // HANDLE THE ERROR.
      }
    });
    
4) Execute a SQL query:

    jdbc.executeSqlString('SELECT * FROM TABLENAME', 
      // row processor.
      function (row) {

      },

      // finished handler
      function(err) {
        if (err) {
          // HANDLE THE ERROR
        }
      }
    );

5) Execute a prepared statement query:

    jdbc.executePreparedStatement(sql, parameters, 
      // row processor.
      function (row) {

      },

      // finished handler
      function(err) {
        if (err) {
          // HANDLE THE ERROR
        }
      }
    );
    
6) Execute an update prepared statement:

    jdbc.executeUpdatePreparedStatement(sql, parameters, function (err) {
      if (err) {
        // HANDLE THE ERROR
      }
    });
    
    