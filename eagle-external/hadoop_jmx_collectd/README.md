#**A manual for hadoop plugin of collectd**

Collectd website referto [collectd.org](collectd.org)

###Description
The plugin collect information from http://hostname:port/jmx , according to the hadoop role configuration, it support roles as below:

	HDFS NameNode
	HDFS DataNode
	HDFS JournalNode
	HBase Master
	Hbase RegionServer
	Yarn NodeManager
	Yarn ResourceManager

###Install
>1)
Deploy hadoop.py in your collectd plugins path , in my environment, it is like : /opt/collectd/lib/collectd/plugins/hadoop.py(It assume that you install collectd in the directory of /opt/collectd), maybe you should create the plugins directory ahead.

>2)
Configure the python plugin in file collectd.conf , and you should trun on the python switch first. 

>A snippet of collectd.conf for showing hadoop python plugin configuration:<br/>
>
    LoadPlugin python
	<Plugin "python">                                                                                                                                                                                                                           
	ModulePath "/opt/collectd/lib/collectd/plugins/"
    LogTraces true 
    Import "hadoop"
    <Module "hadoop">
        HDFSDatanodeHost "YourHostName"
        Port "50075"
        Verbose true 
        Instance "192.168.xxx.xxx" 
        JsonPath "/xxx/xxx/hadooproleconfig.json"
    </Module>
    <Module "hadoop">
        YARNResourceManager "YourHostName"
        Port "8088"
        Verbose true 
        Instance "192.168.xxx.xxx" 
    </Module>
    </Plugin>

>**Notification**:
>Instance , port and host(role) fileds must be set.

>3)Two way to cite hadooproleconfig.json , either is ok.<br/>
>>a)Place hadooproleconfig.json path in one <Module "hadoop">  </Module> pair in collectd.conf.<br/>
>>b)Place hadooproleconfig.json file in you BaseDir which defined in collectd.conf.

>4)
If you update your collectd.conf or hadooproleconfig.json, you should restart your collectd application.

###Dependency
	collectd:	I test in version 5.6.0.
	Hadoop:		I test in version 2.6.0-cdh5.4.3.
	Hbase:		I test in version 1.0.0-chd5.4.3.

###Testing
>You can use hadoop.py as a single independent python file for debugging jmx, and you can also use it as plugin for collectd, a variable MyDebug is used as a swtich when used in the two different environments.


