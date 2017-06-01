# generic_logging_service
#Logging Service for Transaction class project
To run the project you need some softwares
=============================================
Install Apache geode
Install Tomcat server
Install Mongodb and start mongodb in the shell

URL for accessing the log service
===================================================================================
1. begin tran - http://localhost:8080/generic_logging_service/rest/logging/begin
2. write log - http://localhost:8080/generic_logging_service/rest/logging/write
3. query - http://localhost:8080/generic_logging_service/rest/logging/query
4. delete - http://localhost:8080/generic_logging_service/rest/logging/delete
5. commit tran - http://localhost:8080/generic_logging_service/rest/logging/commit

Sample write log data:

"payload": "<Employee><firstName>reeta</firstName><lastName>g</lastName><salary>8</salary></Employee>",
"logtype" :"POLICY",
"tid": "cf07068a-d746-4e1f-ae06-d7b43a4c090c"
 
