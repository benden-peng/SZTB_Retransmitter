#!/usr/bin/python
###########################################################################
#	Filename   : RS_server.py
#	Author     : Benden
#	Description: Receice real-time traffic data(GPS) from JiaoWei and
#		     then send the data to other App-client. the local host
#		     will keep a backup copy of the data.
###########################################################################

###########################################################################
#
#	Usage:TODO:XXXXXXXXXXXXXXXXXXX
#
###########################################################################

import threading
import traceback
import socket
import sys
import time
import logging
import os
import struct
###########################################################################
#	Def Functions
###########################################################################

##
# Init HDlogger - for local file logger
## 
def initHDlog(logfile):
	import logging
	logger = logging.getLogger()
	hdlr = logging.FileHandler(logfile)
	formatter = logging.Formatter('%(asctime)s  %(levelname)s  %(message)s')
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr)
	logger.setLevel(logging.INFO)
	return logger
# initHDlog END

# Global VAR

###########################################################################
#	Def Threads Class
###########################################################################

##
# This Thread class is used for handling every connection comming from JIAOWEI
#
#
##
class Thread_datarecvport_handle(threading.Thread):
	def __init__(self,clientsock,clientaddr,logger):
		threading.Thread.__init__(self)
		self.clientsock = clientsock
		#self.clientsockfd = clientsock.makefile('rwb')
		self.clientaddr = clientaddr
		self.logger = logger
		self.parent = ''
	def setParentThread(self,parent):
		self.parent = parent		# For this program situation parent is recvlisten_thread
	def getSendlist(self):
		if(self.parent):
			return self.parent.sendlist
		return []
	def recvAMessage(self, clientsock):	#:recv a message, BTW when error happen, it has to setConnectionCloseTime()
						#when cannot Jiexi a message, it has to addMisunderstandCount() and ***		
		misunderstand_flag = False
		tmp_flag1 = False
		tmp_flag2 = False
		while 1:
			msg_type = clientsock.recv(1)
			if( not msg_type ):
				# the connection is already close
				return
			if( msg_type==struct.pack('b',2) or msg_type==struct.pack('b',3) or msg_type==struct.pack('b',4) ):
				#msg_len = clientsock.recv(2,socket.MSG_PEEK)	#pre reading, won't delete buffer in TCP queue-buffer
				msg_len = clientsock.recv(2)
				length_number = struct.unpack('< h',msg_len)[0]
				if (length_number<20 or length_number > 300):
					if(tmp_flag1==False):
						tmp_flag1=True
						print "discard message len:",length_number
					misunderstand_flag = True
					continue
				#clientsock.recv(2)
				break
			if(tmp_flag2==False):
				tmp_flag2=True
				print "%s-%s" %(msg_type,hex(ord(msg_type)))
			misunderstand_flag = True
		msg_value = clientsock.recv(length_number)
		msg = msg_type + msg_len + msg_value
		if(misunderstand_flag == True):
			self.recvlisten_thread.addMisunderstandCount()
			self.recvlisten_thread.addDayMisunderstandCount()
		return msg
	def transmitAMessage(self,message):	#send a message to all verified client
		sendlist = self.getSendlist()
		for sendsock in sendlist:
			try:
				sendsock.sendall(message)
			except:
				self.logger.info("connection close by app client %s" %(sendsock))
				print "connection close by app client %s" %(sendsock)
				traceback.print_exc()
				self.recvlisten_thread.removeFromSendList(sendsock)
				sendsock.close()
		return

	def run(self):
		self.recvlisten_thread = self.parent
		while 1:
			try:
				self.buf = self.recvAMessage(self.clientsock)
				if ( not self.buf ):
					# the connection is already close
					self.logger.critical('Connection closed by JW')
	                                print 'Connection closed by JW'
					self.recvlisten_thread.handle_unit=''
	                                self.recvlisten_thread.setConnectionCloseTime()
        	                        break
			except socket.timeout:
				self.logger.critical('Connection timeout, the connection from JW will be disconnect')
				print 'Connection timeout, the connection from JW will be disconnect'
				self.clientsock.close()
				self.recvlisten_thread.handle_unit=''
				self.recvlisten_thread.setConnectionCloseTime()
				sys.exit(1)
			except:
				self.logger.critical('Connection closed by JW')
				print 'Connection closed by JW'
				traceback.print_exc()
				self.clientsock.close()
				self.recvlisten_thread.handle_unit=''
				self.recvlisten_thread.setConnectionCloseTime()
				sys.exit(1)
			self.recvlisten_thread.addTotalCount()
			self.recvlisten_thread.addDayTotalCount()

			self.transmitAMessage(self.buf)

##
# This Thread class is used for handling every connection comming from XJY's APP
#
#
##
class Thread_datasendport_handle(threading.Thread):
        def __init__(self,clientsock,clientaddr,logger):
		threading.Thread.__init__(self)
                self.clientsock = clientsock
		self.clientsockfd = clientsock.makefile('rw')	# make socket to be used as file
                self.clientaddr = clientaddr
                self.logger = logger
		self.parent = ''
	def setParentThread(self,parent):
		self.parent = parent
	def addToSendList(self,sendsock):
		if(self.parent):
			return self.parent.addToSendList(sendsock)
		return False
        def run(self):
		# verify client's password
		global client_pwd
		try:
			self.buf = self.clientsockfd.readline()
		except socket.timeout:
			self.logger.info("Senddata connection timeout. %s connection will be closed" %self.clientaddr[0])
			print "Senddata connection timeout. %s connection will be closed" %self.clientaddr[0]
			self.clientsock.close()
			return
		except:
			self.logger.critical("An Error happend at Thread_datasendport_handle when self.clientsock.readline()")
			print "An Error happend at Thread_datasendport_handle when self.clientsock.readline()"
			self.clientsockfd.close()
			traceback.print_exc()
			sys.exit(1)
		if(0==cmp(self.buf,client_pwd)):
			self.addToSendList(self.clientsock)
			return
		self.logger.info("Senddata %s can not be verify, RS_server will close the connection" %self.clientaddr[0])
		print "Senddata %s can not be verify, RS_server will close the connection" %self.clientaddr[0]
		try:
			self.clientsock.close()
		except:
			self.logger.critical("An Error happend at Thread_datasendport_handle when self.clientsock.close()")
			traceback.print_exc()
			sys.exit(1)
		return

##
# This Thread class is used for handling every connection comming from monitoring APPs
#
#
##
class Thread_monitoringport_handle(threading.Thread):	
	def __init__(self,clientsock,clientaddr,logger):
		threading.Thread.__init__(self)
		self.clientsock = clientsock
		self.clientsockfd = clientsock.makefile('rw')
		self.clientaddr = clientaddr
		self.logger = logger
		self.parent = ''
	def setParentThread(self,parent):
		self.parent = parent	# for this situation, parent's parent is the Thread_datarecvport_listen 
	def run(self):
		self.recvlisten_thread = self.parent.parent
		if(not self.recvlisten_thread):
			self.logger.info("Monitoring thread cannot get recvlisten_thread handle" %self.recvlisten_thread)
			print "Monitoring thread cannot get recvlisten_thread handle %s" %self.recvlisten_thread
		# verify monitor's password
                global monitor_pwd
                try:
                        self.buf = self.clientsockfd.readline()
			if( not self.buf ):
				# the connection is already close
				self.logger.info("Monitoring connection closed by monitor %s." %self.clientaddr[0])
	                        print "Monitoring connection closed by monitor %s." %self.clientaddr[0]
				return
                except socket.timeout:
                        self.logger.info("Monitoring connection timeout. %s connection will be closed" %self.clientaddr[0])
			print "Monitoring connection timeout. %s connection will be closed" %self.clientaddr[0]
                        self.clientsock.close()
                        return
                except:
			self.logger.critical("An Error happend at Thread_monitoringport_handle when self.clientsock.readline()")
                        traceback.print_exc()
                        sys.exit(1)
                if(0!=cmp(self.buf,monitor_pwd)):
			# cannot be verify
                	self.logger.info("Monitoring %s can not be verify, RS_server will close the connection" %self.clientaddr[0])
                	print "Monitoring %s can not be verify, RS_server will close the connection" %self.clientaddr[0]
                	try:
                        	self.clientsock.close()
                	except:
				self.logger.critical("An Error happend at Thread_monitoringport_handle when self.clientsock.close()")
                        	traceback.print_exc()
                        	sys.exit(1)
                	return
		while 1:
			try:
				self.buf = self.clientsockfd.readline()
				if( not self.buf ):
	                                # the connection is already close
        	                        self.logger.info("Monitoring connection closed by monitor %s." %self.clientaddr[0])
                	                print "Monitoring connection closed by monitor %s." %self.clientaddr[0]
                        	        return

			except socket.timeout:
				self.logger.info("Monitoring connection timeout. %s connection will be closed" %self.clientaddr[0])
	                        self.clientsock.close()
        	                return
			except:
				self.logger.critical("An Error happend at Thread_monitoringport_handle when self.clientsock.readline()")
				self.clientsock.close()
				traceback.print_exc()
				sys.exit(1)
			
			if(0==cmp(self.buf,"total_count\r\n")):
				self.sendbuf = self.recvlisten_thread.getTotalCount()
				self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.flush()
			elif(0==cmp(self.buf,"day_total_count\r\n")):
				self.sendbuf = self.recvlisten_thread.getDayTotalCount()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"misunderstand_count\r\n")):
				self.sendbuf = self.recvlisten_thread.getMisunderstandCount()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"day_misunderstand_count\r\n")):
				self.sendbuf = self.recvlisten_thread.getDayMisunderstandCount()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"transmit_list\r\n")):
				self.sendbuf = self.recvlisten_thread.getSendList()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"connection_timebuckets\r\n")):
				self.sendbuf = self.recvlisten_thread.getConnectionTimebuckets()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"server_starttime\r\n")):
				self.sendbuf = self.recvlisten_thread.getServerStarttime()
                                self.clientsockfd.write("%s\n" %self.sendbuf)
                                self.clientsockfd.flush()
			elif(0==cmp(self.buf,"exit()\r\n")):
				self.sendbuf = "connection will be closed"
                                self.clientsockfd.write("%s\n" %self.sendbuf)
                                self.clientsockfd.flush()
				self.clientsockfd.close()
				 # the connection is already close
                                self.logger.info("Monitoring connection closed by monitor %s." %self.clientaddr[0])
                                print "Monitoring connection closed by monitor %s." %self.clientaddr[0]
				break
			else:
				self.sendbuf = "unrecognized command!!"
				self.clientsockfd.write("%s\n" %self.sendbuf)
				self.clientsockfd.flush()
##
# This Thread class is used for Listening 
#	data-recv port (this port is used by JiaoWei)
#
##
class Thread_datarecvport_listen(threading.Thread):
	def __init__(self,port,logger):
		threading.Thread.__init__(self)
		self.port = port
		self.host = ""
		self.logger = logger
		self.handle_unit = ''
		self.parent = ''

	        self.sendlist = []
                self.sendlist_lock = threading.Lock()

		# variable for monitoring
		self.server_starttime = time.localtime()
		self.currentday = time.strftime('%Y%j',time.localtime())
                self.total_count = 0
                self.day_total_count =0
                self.misunderstand_count = 0
                self.day_misunderstand_count = 0
		self.connection_timebuckets = [] 
	def addTotalCount(self):
		self.total_count = self.total_count + 1
	def getTotalCount(self):
		return self.total_count
	def addDayTotalCount(self):
		self.tmpday = time.strftime('%Y%j',time.localtime())
		if( self.tmpday > self.currentday ):
			self.currentday = self.tmpday
			self.day_total_count = 0
			self.day_misunderstand_count = 0
		self.day_total_count = self.day_total_count + 1
	def getDayTotalCount(self):
		return self.day_total_count
	def addMisunderstandCount(self):
		self.misunderstand_count = self.misunderstand_count + 1
	def getMisunderstandCount(self):
		return self.misunderstand_count
	def addDayMisunderstandCount(self):
		self.tmpday = time.strftime('%Y%j',time.localtime())
                if( self.tmpday > self.currentday ):
                        self.currentday = self.tmpday
                        self.day_total_count = 0
                        self.day_misunderstand_count = 0
		self.day_misunderstand_count = self.day_misunderstand_count + 1
	def getDayMisunderstandCount(self):
		return self.day_misunderstand_count
	def getServerStarttime(self):
		return self.server_starttime
	def getConnectionTimebuckets(self):
		return self.connection_timebuckets
        def addToSendList(self,sendsock):
		self.logger.info("%s:%s--%s is add to the SendList" %(sendsock.getpeername()[0],sendsock.getpeername()[1],sendsock))
		print "%s:%s--%s is add to the SendList" %(sendsock.getpeername()[0],sendsock.getpeername()[1],sendsock)
                self.sendlist_lock.acquire()
                self.sendlist.append(sendsock)
                self.sendlist_lock.release()
                return True
	def removeFromSendList(self,sendsock):
                flag = False
                for sock in self.sendlist:
                        if(sock==sendsock):
                                flag = True
                if(flag):
			self.logger.info('%s is remove from the SendList' %(sendsock))
			print '%s is remove from the SendList' %(sendsock)
                        self.sendlist_lock.acquire()
                        self.sendlist.remove(sendsock)
                        self.sendlist_lock.release()
                        return True
                return False

        def getSendList(self):
                return self.sendlist


	###########################################
	def setConnectionCloseTime(self):
		for timebucket in self.connection_timebuckets:
			if(0==timebucket[1]):
				timebucket[1] = time.localtime()
	def setPeerThreadInfo(self,peer_thread):
                self.sendlisten_thread = peer_thread
	def run(self):
		try:
			self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
			self.sock.bind((self.host,self.port))
			self.sock.listen(1)	# at most 1 concurrent connection
			self.logger.info("Start listen to port %d ..." %self.port)
			print "Start listen to port %d ..." %self.port
		except:
			self.logger.critical("Cannot listen to the port %d" %self.port)
			print "Cannot listen to the port %d" %self.port
			traceback.print_exc()
			sys.exit(1)
		
		# start monitoring thread
		global monitoring_port
		self.monitoringlisten_thread = Thread_monitoringport_listen(monitoring_port,logger)
		self.monitoringlisten_thread.setDaemon(True)
		self.monitoringlisten_thread.setParentThread(threading.currentThread())
		self.monitoringlisten_thread.start()
		self.logger.info('Start monitoring port listen thread')
		print 'Start monitoring port listen thread'
		########################
		while 1:
			try:
				self.clientsock, self.clientaddr = self.sock.accept()
				#self.clientsock.settimeout(10)
				self.logger.info("Recv port accept a connection from client : %s" %self.clientaddr[0])
				print "Recv port accept a connection from client : %s" %self.clientaddr[0]
				#self.logger.info("Recv port accept a connection from client : %s" %self.clientaddr)
				self.handle_unit = Thread_datarecvport_handle(self.clientsock,self.clientaddr,self.logger)
				self.handle_unit.setDaemon(True)
				self.handle_unit.setParentThread(threading.currentThread())
				self.handle_unit.start()

				# For monitoring
				self.timebucket = [time.localtime(),0]
				self.connection_timebuckets.append(self.timebucket)
			except:
				self.logger.critical("Error happend at accept() - Thread_datarecvport_listen")
				print "Error happend at accept() - Thread_datarecvport_listen"
				traceback.print_exc()
				sys.exit(1)
##
# This Thread class is used for Listening 
#       data-send port (this port is used by JiaoWei)
#
##
class Thread_datasendport_listen(threading.Thread):
        def __init__(self,port,logger):
                threading.Thread.__init__(self)
                self.port = port
                self.host = ""
		self.parent = ''
                self.logger = logger
	def setPeerThreadInfo(self,peer_thread):
		self.recvlisten_thread = peer_thread
	def addToSendList(self,sendsock):
		if(self.recvlisten_thread):
			return self.recvlisten_thread.addToSendList(sendsock)
		return False
        def run(self):
                try:
                        self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
                        self.sock.bind((self.host,self.port))
                        self.sock.listen(10)    # at most 10 concurrent connection
			self.logger.info("Listen to port %d ..." %self.port)
			print "Listen to port %d ..." %self.port
                except:
                        self.logger.critical("Cannot listen to the port %d" %self.port)
			print "Cannot listen to the port %d" %self.port
			traceback.print_exc()
                        sys.exit(1)
                while 1:
                        try:
                                self.clientsock, self.clientaddr = self.sock.accept()
				#self.clientsock.settimeout(10)	
                                self.logger.info("Send port accept a connection from client : %s" %self.clientaddr[0])
				print "Send port accept a connection from client : %s" %self.clientaddr[0]
				self.handle_unit = Thread_datasendport_handle(self.clientsock,self.clientaddr,self.logger)
				self.handle_unit.setDaemon(True)
				self.handle_unit.setParentThread(threading.currentThread())
				self.handle_unit.start()
                        except:
                                self.logger.critical("Error happend at accept() - Thread_datasendport_listen")
				print "Error happend at accept() - Thread_datasendport_listen"
				traceback.print_exc()
                                sys.exit(1)


##
# This Thread class is used for listening
#	monitoring port (this port is used by monitor-APP
#
##
class Thread_monitoringport_listen(threading.Thread):
	def __init__(self,port,logger):
		threading.Thread.__init__(self)
		self.port = port
		self.host = ""
		self.parent = ''
		self.logger = logger
		self.recvlisten_thread = recvlisten_thread
	def setParentThread(self,parent):
                self.parent = parent
	def run(self):
		try:
			self.sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR,1)
			self.sock.bind((self.host,self.port))
			self.sock.listen(10)    # at most 10 concurrent connection
			self.logger.info("Listen to port %d ..." %self.port)
			print "Listen to port %d ..." %self.port
                except:
                        self.logger.critical("Cannot listen to the port %d" %self.port)
                        print "Cannot listen to the port %d" %self.port
                        traceback.print_exc()
                        sys.exit(1)
                while 1:
                        try:
                                self.clientsock, self.clientaddr = self.sock.accept()
                                #self.clientsock.settimeout(10)
                                self.logger.info("Monitoring port accept a connection from client : %s" %self.clientaddr[0])
                                print "Monitoring port accept a connection from client : %s" %self.clientaddr[0]
                                self.handle_unit = Thread_monitoringport_handle(self.clientsock,self.clientaddr,self.logger)
                                self.handle_unit.setDaemon(True)
                                self.handle_unit.setParentThread(threading.currentThread())
				self.handle_unit.start()
                        except:
                                self.logger.critical("Error happend at accept() - Thread_monitoringport_listen")
                                print "Error happend at accept() - Thread_monitoringport_listen"
                                traceback.print_exc()
                                sys.exit(1)

#######################################################################################
#
#	Main this program starts here
#
#######################################################################################
client_pwd = 'siatxdata\r\n'
monitor_pwd = 'siatmonitor\r\n'
os.system('mkdir Log')
logfile = './Log/log.log'
logger = initHDlog(logfile)
recv_port = 5566
send_port = 5567
monitoring_port = 5568

recvlisten_thread = Thread_datarecvport_listen(recv_port,logger)
recvlisten_thread.setDaemon(True)
#recvlisten_thread.setPeerThreadInfo(sendlisten_thread)
#recvlisten_thread.start()
logger.info('Start recv port listen thread')
print 'Start recv port listen thread'

sendlisten_thread = Thread_datasendport_listen(send_port,logger)
sendlisten_thread.setDaemon(True)
#sendlisten_thread.setPeerThreadInfo(recvlisten_thread)
#sendlisten_thread.start()
logger.info('Start send port listen thread')
print 'Start send port listen thread'

recvlisten_thread.setPeerThreadInfo(sendlisten_thread)
recvlisten_thread.start()
sendlisten_thread.setPeerThreadInfo(recvlisten_thread)
sendlisten_thread.start()


recvlisten_thread.join()
sendlisten_thread.join()

