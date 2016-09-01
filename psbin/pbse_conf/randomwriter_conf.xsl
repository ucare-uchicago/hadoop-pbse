<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
   <property>
     <name>test.randomwrite.min_key</name>
     <value>10</value>
   </property>
   <property>
     <name>test.randomwrite.max_key</name>
     <value>10</value>
   </property>
   <property>
     <name>test.randomwrite.min_value</name>
     <value>90</value>
   </property>
   <property>
     <name>test.randomwrite.max_value</name>
     <value>90</value>
   </property>
   <property>
     <name>test.randomwrite.total_bytes</name>
     <!--<value>189097920000</value> scriptTest-8MB-->
     <!--<value>189100560000</value> scriptTest-16MB-->
     <!--<value>189101550000</value> scriptTest-64MB-->
     <value>378203100000</value> <!-- Facebook 2010, 6600 partitions-->
   </property>
   <property>
     <name>test.randomwrite.bytes_per_map</name>
     <!--<value>7162800</value> scriptTest-8MB-->
     <!--<value>14325800</value> scriptTest-16MB-->
     <value>57303500</value> <!--scriptTest-64MB-->
   </property>
</configuration>
