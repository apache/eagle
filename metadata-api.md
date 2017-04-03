---
layout: doc
title:  "Policy API"
permalink: /docs/metadata-api.html
---

Apache Eagle Provide RESTful APIs for create/update/query/delete policy for alert

* Policy Definition API  
* Stream Definition API  

------  

### Policy Definition API  

------  

#### **Create/Update Policy Example**      

URL               |||    http://host:port/eagle-service/rest/entities?serviceName=AlertDefinitionService   
METHOD            |||    POST
HEADERS           |||    "Content-Type:application/json"   
                  |||    "Authorization:Basic encodedusrpwd"  (encodedusrpwd is base64 encoded string for "user:password")  
DATA              |||    [{  
                  |||    &nbsp;&nbsp;"tags": {  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "site": "sandbox",  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "dataSource": "hdfsAuditLog",  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "policyId": "testPolicy",  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "alertExecutorId": "hdfsAuditLogAlertExecutor",  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "policyType": "siddhiCEPEngine"  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp;},  
                  |||    &nbsp;&nbsp;"desc": "test alert policy",  
                  |||    &nbsp;&nbsp;"policyDef": "{\"type\":\"siddhiCEPEngine\",\"expression\":\"from hdfsAuditLogEventStream[src =='/tmp/private'] select * insert into outputStream;\"}",  
                  |||    &nbsp;&nbsp;"notificationDef": "[{
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "sender":"noreply-eagle@company.com",
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "recipients":"user@company.com",
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "subject":"test alert policy",
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "flavor":"email",
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "id":"email_1"
                  |||    &nbsp;&nbsp;&nbsp;&nbsp;}]",  
                  |||    &nbsp;&nbsp;"enabled": true  
                  |||    }]  

**Field Specification**  

Tags             |||    All Tags form the key for alert policy  
                 |||    1) site: Which site is the policy for? e.g. sandbox  
                 |||    2) dataSource: From which dataSource the policy consume from; e.g. hdfsAuditLog  
                 |||    3) policyId  
                 |||    4) alertExecutorId: Within which executor will the policy be executed e.g. hdfsAuditLog  
                 |||    5) policyType: Which engine should the policy be executed with e.g. siddhiCEPEngine  
policyDef        |||    Definition for the policy, tell  
                 |||    1) which engine the policy should be executed with  
                 |||    2) The policy expression to be evaluated  
notificationDef  |||    Currently we only support email notification for alert, below are fields of alert definition  
                 |||    1) sender: Email Sender  
                 |||    2) recipients: Email Receipent  
                 |||    3) subject: Email Subject  
                 |||    4) flavor: way of notification, currently only supprot "email"  
                 |||    5) id: notification id  
enabled          |||    If the alert is enabled, true/false  
desc             |||    Description of the policy  
  
**Response Body**  
{  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"meta": {  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;     "elapsedms": 11,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;     "totalResults": 1  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"success": true,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"obj": [  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;     "YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe1hk6BkS9hcZXeJk1Je-7-Mrq0lGQ"  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;],  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "java.lang.String"  
}  

------ 

#### **Get Policy Example**  

URL               |||    http://host:port/eagle-service/rest/list?query=AlertDefinitionService[@dataSource="hdfsAuditLog" AND @site="sandbox"]{*}&pageSize=100  
METHOD            |||    GET
HEADERS           |||    "Content-Type:application/json"   
                  |||    "Authorization:Basic encodedusrpwd"  (encodedusrpwd is base64 encoded string for "user:password")  

**Response Body**   
{  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;prefix: "alertdef",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;tags: {  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;site: "sandbox",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;dataSource: "hdfsAuditLog",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;policyId: "testPolicy",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;alertExecutorId: "hdfsAuditLogAlertExecutor",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;policyType: "siddhiCEPEngine"  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;encodedRowkey: "YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe1hk6BkS9hcZXeJk1Je-7-Mrq0lGQ",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;desc: "nope alert for test",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;policyDef: "{"type":"siddhiCEPEngine","expression":"from hdfsAuditLogEventStream[src=='/tmp/private'] select * into outputStream;"}",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;notificationDef: "[{"sender":"noreplay-eagle@company.com","recipients":"user@company.com","subject":"testPolicy","flavor":"email","id":"email_1"}]",  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;enabled: true  
}  

------  

#### **Delete Policy Example**    

Delete policy by encodedRowkey

URL               |||    http://host:port/eagle-service/rest/entities/delete?serviceName=AlertDefinitionService&byId=true  
METHOD            |||    POST  
HEADERS           |||    "Content-Type:application/json"  
                  |||    "Authorization:Basic encodedusrpwd"  (encodedusrpwd is base64 encoded string for "user:password")  
DATA              |||    [  
                  |||       "YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe1hk6BkS9hcZXeJk1Je-7-Mrq0lGQ"  
                  |||    ]  

**Delete Request Response Body**  

The folloing is the response body of a sucessfully delete request  
{  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"meta": {  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;     "elapsedms": 5,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;     "totalResults": 1  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;},  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"success": true,  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"obj": [  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;     "YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe1hk6BkS9hcZXeJk1Je-7-Mrq0lGQ"  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;],  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"type": "java.lang.String"  
}  

-----

### Stream Definition API  

In the policy defintion, if the policyType is "siddhiCEPEngine" we need specify from which stream the query is against , like "from hdfsAuditLogEventStream"   

So we need further define the stream schema along with the policy

The response body of stream schema api is similar to policy api, we don't duplicate it in stream definition api  

------  

#### **Create/Update Stream Shceme Example**   

URL               |||    http://host:port/eagle-service/rest/entities?serviceName=AlertStreamSchemaService   
METHOD            |||    POST
HEADERS           |||    "Content-Type:application/json"   
                  |||    "Authorization:Basic encodedusrpwd"  (encodedusrpwd is base64 encoded string for "user:password")  
DATA              |||    [{  
                  |||    &nbsp;&nbsp;"tags": {  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "dataSource": "hiveQueryLog",  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "attrName": "user",  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; "streamName": "hiveAccessLogStream"  
                  |||    &nbsp;&nbsp;&nbsp;&nbsp; },  
                  |||    &nbsp;&nbsp;"attrType": "string",  
                  |||    &nbsp;&nbsp;"attrDescription": "process user"  
                  |||    }]                  

**Field Specification**  

Tags             |||    All Tags form the key for alert policy  
                 |||    1) dataSource: From which dataSource the policy consume from, e.g. "hdfsAuditLog"  
                 |||    2) attrName: Attribute's name, e.g. "user"  
                 |||    3) streamName: Stream's name, e.g.  "hiveAccessLogStream"  
attrType         |||    Attribute's type, e.g. string, boolean, int, long  
attrDescription  |||    Description for the attribute
  
------  

#### **Get Stream Shceme Example**  

URL               |||    http://host:port/eagle-service/rest/list?query=AlertStreamSchemaService[@dataSource="hdfsAuditLog" AND @streamName="hiveAccessLogStream"]{*}&pageSize=100  
METHOD            |||    GET
HEADERS           |||    "Content-Type:application/json"   
                  |||    "Authorization:Basic encodedusrpwd"  (encodedusrpwd is base64 encoded string for "user:password")  

------  
   
#### **Delete Stream Shceme Example**    

Delete stream shceme by encodedRowkey

URL               |||    http://host:port/eagle-service/rest/entities/delete?serviceName=AlertStreamSchemaService&byId=true  
METHOD            |||    POST  
HEADERS           |||    "Content-Type:application/json"  
                  |||    "Authorization:Basic encodedusrpwd"  (encodedusrpwd is base64 encoded string for "user:password")  
DATA              |||    [ "YEktKX_____62aP_6x97yoSv3B0ANd9Hby--xyCZKe1hk6BkS9hcZXeJk1Je-7-Mrq0lGQ" ]    
