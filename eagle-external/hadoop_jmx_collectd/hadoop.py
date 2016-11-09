#! /usr/bin/python

import urllib2
import json
import os

CurRoleTypeInfo= {}
HadoopConfigs= []
LogSwitch= False
JsonPath = None

def CfgCallback(conf):
    global JsonPath
    CurRoleType = None
    Role = None
    Port = None
    Host = None
    OutputFlag = LogSwitch

    for EachRoleConfig in conf.children:
        collectd.info('hadoop pluginx : %s' % EachRoleConfig.key)
        if EachRoleConfig.key == 'HDFSNamenodeConfigHost':
            Host = EachRoleConfig.values[0]
            CurRoleType = "ROLE_TYPE_NAMENODE"
        elif EachRoleConfig.key == 'HDFSDatanodeConfigHost':
            Host = EachRoleConfig.values[0]
            CurRoleType = "ROLE_TYPE_DATANODE"
        elif EachRoleConfig.key == 'YarnNodeManagerHost':
            Host = EachRoleConfig.values[0]
            CurRoleType = "ROLE_TYPE_NODEMANAGER"
        elif EachRoleConfig.key == 'YarnResourceManagerHost':
            Host = EachRoleConfig.values[0]
            CurRoleType ="ROLE_TYPE_RESOURCEMANAGER"
        elif EachRoleConfig.key == 'HbaseMasterHost':
            Host = EachRoleConfig.values[0]
            CurRoleType = "ROLE_TYPE_HBASE_MASTER"
        elif EachRoleConfig.key == 'HbaseRegionserverHost':
            Host = EachRoleConfig.values[0]
            CurRoleType = "ROLE_TYPE_HBASE_REGIONSERVER"
        elif EachRoleConfig.key == 'HDFSJournalEachRoleConfigHost':
            Host = EachRoleConfig.values[0]
            CurRoleType = "ROLE_TYPE_HDFS_JOURNALNODE"
        elif EachRoleConfig.key == 'Port':
            Port = EachRoleConfig.values[0]
        elif EachRoleConfig.key == 'Instance':
            Role = EachRoleConfig.values[0]
        elif EachRoleConfig.key == 'Verbose':
            OutputFlag = bool(EachRoleConfig.values[0])
        elif EachRoleConfig.key == 'JsonPath':
            collectd.info('hadoop plugin cfg: %s' % EachRoleConfig.values[0])
            JsonPath = EachRoleConfig.values[0]
        else:
            collectd.warning('hadoop plugin: Unsupported key: %s.' % EachRoleConfig.key)
        #collectd.info('hadoop plugin cfg: %s.' % JsonPath)

    if not Host or not Role or not CurRoleType or not Port:
        collectd.error('hadoop plugin error: *Host, Port, and CurRoleType should not be empty.')
    else:
        CurrentConfigMap = {
            'RoleInstance': Role,
            'port': Port,
            'host': Host,
            'RoleType': CurRoleType ,
            'OutputFlag': OutputFlag 
        }

        HadoopConfigs.append(CurrentConfigMap)

def GetdataCallback():
    GetJsonConfig() 
    for EachConfig in HadoopConfigs:
        Host = EachConfig['host'] 
        Port = EachConfig['port'] 
        RoleInstance = EachConfig['RoleInstance']
        RoleType = EachConfig['RoleType'] 
        OutputFlag = EachConfig['OutputFlag'] 
        
        if isinstance(Port,int) == False:    
            MyLog("Host Port is not number error",True)

        JmxUrl = "http://" + Host + ":" + Port.__str__() + "/jmx"

        try:
            Contents = json.load(urllib2.urlopen(JmxUrl, timeout=5))

        except urllib2.URLError as e:
            if MyDebug == 1:
                print(JmxUrl,e)
            else:
                collectd.error('hadoop plugin: can not connect to %s - %r' % (JmxUrl, e))

        if MyDebug == 1:
            print(RoleType)
        else:
            collectd.info('hadoop pluginx [testheju]: %s' % RoleType)

        for RoleInfo in Contents["beans"]:
            for RoleKey, RoleValue in CurRoleTypeInfo[RoleType].iteritems():
                if RoleInfo['name'].startswith(RoleValue):
                    for k, v in RoleInfo.iteritems():
# Due to the limite of dispatch interface in collectd, the appropriate type is int and float  
                        if isinstance(v, int) or isinstance(v, float):
# gauge is type defined in collectd
                            Submit2Collectd('gauge', '.'.join((RoleKey, k)), v, RoleInstance, RoleType, OutputFlag)

def Submit2Collectd(type, name, value, instance, instance_type, OutputFlag):
    if value is None:
        #value = ''
        collectd.warning('hadoop pluginx : value is None of the key %s' % name)
    else:
        plugin_instance = '.'.join((instance, instance_type))
        MyLog('%s [%s]: %s=%s' % (plugin_instance, type, name, value), OutputFlag)

        if MyDebug == 0:
            SendValue = collectd.Values(plugin='hadoop')
            SendValue.type = type
            SendValue.type_instance = name
            SendValue.values = [value]
            SendValue.plugin_instance = plugin_instance
            SendValue.meta = {'0': True}
            SendValue.dispatch()

def MyLog(msg, OutputFlag):
    if OutputFlag:
        if MyDebug == 1:
            print(msg)
            pass
        else:
            collectd.info('hadoop pluginx output: %s' % msg)

def GetJsonConfig():
    global JsonPath
    #JsonPath = "/opt/collectd/lib/collectd/plugins/hadooproleconfig.json"
    MyLog("pwd:%s" % os.getcwd(),True)
    if JsonPath == None or JsonPath == "":
        with open("./hadooproleconfig.json",'r') as f:
            data = json.load(f) 
            for k,v in data.iteritems():
                if k.startswith("ROLE_TYPE"):
                    CurRoleTypeInfo[k]=v 
    else:
        with open(JsonPath,'r') as f:
            data = json.load(f) 
            for k,v in data.iteritems():
                if k.startswith("ROLE_TYPE"):
                    CurRoleTypeInfo[k]=v 

if __name__ == "__main__":
    MyDebug = 1
    
# You can config you Role configuration like example below
    ConfigMap = {
            'RoleInstance': "instance",
            'port': 8042,
            'host': "lujian",
            'RoleType': "ROLE_TYPE_NODEMANAGER",
            'OutputFlag': bool(True)
        }

    HadoopConfigs.append(ConfigMap)
    #print HadoopConfigs
    GetdataCallback()
else:
    import collectd
    MyDebug = 0
    collectd.register_config(CfgCallback)
    collectd.register_read(GetdataCallback)
