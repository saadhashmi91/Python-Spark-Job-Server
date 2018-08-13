#!/usr/bin/env python

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import SocketServer
import json
import requests
import smtplib
import os
import subprocess
from email.mime.text import MIMEText


class S(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        self.wfile.write("<html><body><h1>hi!</h1></body></html>")

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        self._set_headers()
        print "in post method"
        self.data_string = self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200)
        self.end_headers()
        data = json.loads(self.data_string)
        print "{}".format(data)
        
        print "Getting framework id for reported app"
        app_name=data['app_name']
        timestamp=data['timestamp']
        query_name=data['query_name']
        query_error=data['error']
        
        print "Starting a new Job"
        
        spark_submit_base_path="/opt/spark/dist/bin/spark-submit "
        spark_environment_vars=os.getenv('SPARK_ENV_VARS',"").split(",")
        for line in spark_environment_vars:
          # add environment vars line by line
          if line:
            spark_submit_base_path+=" --conf spark.mesos.driverEnv."+line+" "
			
        class_name=os.getenv('APP_CLASS_NAME')
		print "class_name: ",class_name
		
        mesos_uris=os.getenv('MESOS_URIS',"")
        # mesos urils line by line
        if mesos_uris:
			spark_submit_base_path+=" --conf spark.mesos.uris="+mesos_uris + " "
			
        app_jar=os.getenv('APP_JAR_PATH')
		print "app_jar: ",app_jar
		
		driver_memory=os.getenv('SPARK_DRIVER_MEMORY',"10G")
		print "driver_memory: ",driver_memory
		
        executor_memory=os.getenv('SPARK_EXECUTOR_MEMORY',"15G")
		print "executor_memory: ",executor_memory
		
        executor_cores=str(os.getenv('SPARK_EXECUTOR_CORES',"1"))
		print "executor_cores: ",executor_cores
		
        executor_total_cores=str(os.getenv('SPARK_EXECUTOR_TOTAL_CORES',"8"))
		print "executor_cores: ", executor_total_cores
		
        spark_master=os.getenv('SPARK_MASTER_URL')
		print "spark_master: ", spark_master
		
        print "Spark submit base string is: ",spark_submit_base_path
        bashStartNewJob = spark_submit_base_path + " --conf spark.dynamicAllocation.enabled=true --conf spark.network.timeout=200s --conf spark.sql.broadcastTimeout=120s --conf spark.kryoserializer.buffer.max=10MB --conf spark.shuffle.io.connectionTimeout=60s --conf spark.shuffle.registration.timeout=2m --conf spark.local.dir=/tmp/spark --conf spark.dynamicAllocation.maxExecutors=10 --conf spark.mesos.extra.cores=1 --conf spark.executor.home=/opt/spark/dist --conf spark.mesos.executor.docker.volumes=/var/lib/tmp/spark:/tmp/spark:rw --conf spark.shuffle.registration.maxAttempts=5 --conf spark.shuffle.service.enabled=true --conf spark.network.timeout=60s --conf spark.shuffle.io.connectionTimeout=60s --conf spark.shuffle.registration.timeout=2m --conf spark.shuffle.io.backLog=8192 --conf spark.shuffle.io.serverThreads=128 --conf spark.shuffle.file.buffer=1MB --conf spark.shuffle.service.index.cache.entries=2048 --conf spark.io.compression.lz4.blockSize=512KB --conf spark.file.tranferTo=false --conf spark.shuffle.unsafe.file.output.buffer=5MB --conf spark.unsafe.sorter.spill.reader.buffer.size=1MB  --conf spark.memory.offHeap.size=1g --conf spark.executer.extraJavaOptions=-XX:ParallelGCThreads=4,-XX:+UseParallelGC --conf spark.memory.offHeap.enabled=true --conf spark.memory.fraction=0.8 --conf spark.max.fetch.failures.per.stage=10 --conf spark.rpc.io.serverThreads=64    --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=hdfs://hdfs/history  --driver-memory="+driver_memory+ " --executor-memory="+executor_memory+" --deploy-mode cluster  --conf spark.mesos.executor.docker.image=mesosphere/spark:2.1.0-2.2.1-1-hadoop-2.6  --class "+class_name+" --master "+spark_master+"  --conf spark.executor.cores="+executor_cores+"  --total-executor-cores "+executor_total_cores+" "+app_jar
		print "Bash cmd created: ",bashStartNewJob
		res = subprocess.check_output(bashStartNewJob.split())
		print "Bash response: ",res
        
		if query_error!="query stopped after trigger once":
			print "Sending response and app details by email"
			msg = MIMEText("Spark job with name " + app_name + " failed at time "+ timestamp + ". Error was: "+query_error+".\r\n Tried Starting new job with response: " + res)
			me = os.getenv('SENDER')
			to = os.getenv('RECIPIENTS')
			recipients=to.split(',')
			msg['Subject'] = 'Spark Job Error'
			msg['From'] = me
			msg['To'] =  ", ".join(recipients)
			# Send the message via our SMTP server, but don't include the
			# envelope header.
			host=os.getenv('SMTP_HOST')
			port=int(os.getenv('SMTP_PORT'))
			user=os.getenv('SMTP_USER')
			password=os.getenv('SMTP_PASSWORD')
			server = smtplib.SMTP(host,port)
			server.starttls()
			server.set_debuglevel(1)
			server.login(user,password)
			server.sendmail(me, recipients, msg.as_string())
			server.quit()	
        return


def run(server_class=HTTPServer, handler_class=S, port=80):
  print 'Starting httpd...'
  host=os.getenv('HOST','')
  print 'HOST is ',host
  port=int(os.getenv('PORT0',8080))
  print 'PORT is ',port
  server_address = (host, port)
  httpd = server_class(server_address, handler_class)
  
  httpd.serve_forever()

if __name__ == "__main__":
  run()