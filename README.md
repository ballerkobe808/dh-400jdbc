DH-400JDBC-SP
==============

JDBC Wrapper for the JT400 driver to connect to an AS400 using JDBC, resolving issues with original package (npmjs.com/package/dh-400jdbc) when calling Store Procedures.


Original code
=============

Repository: https://github.com/ballerkobe808/dh-400jdbc.git

License: https://github.com/adaligaji/dh-400jdbc/blob/master/LICENSE


Usage
=============

1. Require the module:

    ```
    const jdbc = require('dh-400jdbc');
    ``` 

2. Initialize the connection:
    ```
    // build the config.
    let config = {
      host: 'String',
      libraries: <String>,
      username: <String>,
      password: <String>,
      initialPoolCount: <Number>, // Optional, Defaults to 1.
      logger: <Logger Reference> // Optional, Defaults to console.
    };
   
    // initialize the module.
    jdbc.initialize(config, (err) => {
      // Do Something.
    });
    ```
   
3. Execute a SQL query:
    ```
    jdbc.executeSqlString('SELECT * FROM TABLENAME', (err, results) => {
      // Do Something.
    });
    ````
   
4. Execute a prepared statement query. Note: parameters is an array of values:
    ```
    jdbc.executePreparedStatement(sql, parameters, (err, results) => {
      // Do Something.
    });
    ```
   
5. Execute an update prepared statement. Note: parameters is an array of values:
    ```
    jdbc.executeUpdatePreparedStatement(sql, parameters, (err, results) => {
      // Do Something.
    });
    ```
   
6. Executing a stored procedure. Note: the parameters array is an array of stored procedure parameter objects.
    
    - You can create the objects in this format:
        ```
        {
          type: <'in' or 'out'>,
          fieldName: <String>,
          dataType: <String from sql types constants property>,
          value: <any type>
        }
        ```
   
    - Or use the convenience functions:
        ```
        let inputParameter = jdbc.createSPInputParameter(value);
        let outputParameter = jdbc.createSPOutputParameter(sqlDataType, fieldName);
        ```
   
    - Execute the statement:
        ```
        jdbc.executeStoredProcedure(sql, parameters, (err, result) => {
          // Do Something.
        });
        ```
  
    - Note: The result object is a key value object where the keys are the output parameter field names.
        ```
        {
          <field name 1> : <output param value 1>,
          <field name 2> : <output param value 2>,
          <field name 3> : <output param value 3>,
        }
        ```
      
    - Note: If your stored procedure returns 1 or more result sets you can access them through the result objects resultSets property. The resultsSetsProperty is an array of arrays where each array is a single result set:
        ```
        {
          outputParam1: <value>,
          outputParam2: <value>,
          resultSets: [
            [],
            []
          ]
        }
        ```
