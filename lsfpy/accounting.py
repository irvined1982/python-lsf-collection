#!python
#
# This file is part of the python lsf collection.
#
# The python LSF collection is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# The python LSF collection is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with The python LSF collection.  If not, see <http://www.gnu.org/licenses/>.
#
# Copyright 2011 David Irvine
#


import datetime
import sys
import os
from optparse import OptionParser
import csv
import unittest

TERMINFO={
		"-1":{
			"name":"TERM_UNTERMINATED",
			"desc":"Job was not terminaed",
			"number":-1,
			},
		"0":{
			"name":"TERM_UNKNOWN",
			"desc":"LSF cannot determine a termination reason.0 is logged but TERM_UNKNOWN is not displayed (0)			",
			"number":0,
			},
		"1":{
			"name":"TERM_PREEMPT",
			"desc":"Job killed after preemption (1)",
			"number":1,
			},
		"2":{
			"name":"TERM_WINDOW",
			"desc":"Job killed after queue run window closed (2)",
			"number":2,
			},
		"3":{
			"name":"TERM_LOAD",
			"desc":"Job killed after load exceeds threshold (3)",
			"number":3,
			},
		"4":{
			"name":"TERM_OTHER",
			"desc":"NOT SPECIFIED",
			"number":4,
			},
		"5":{
			"name":"TERM_RUNLIMIT",
			"desc":"Job killed after reaching LSF run time limit (5)",
			"number":5,
			},
		"6":{
			"name":"TERM_DEADLINE",
			"desc":"Job killed after deadline expires (6)",
			"number":6,
			},
		"7":{
			"name":"TERM_PROCESSLIMIT",
			"desc":"Job killed after reaching LSF process limit (7)",
			"number":7,
		},
		"8":{
			"name":"TERM_FORCE_OWNER",
			"desc":"Job killed by owner without time for cleanup (8)",
			"number":8,
			},
		"9":{
			"name":"TERM_FORCE_ADMIN",
			"desc":"Job killed by root or LSF administrator without time for cleanup (9)",
			"number":9,
			},
		"10":{
			"name":"TERM_REQUEUE_OWNER",
			"desc":"Job killed and requeued by owner (10)",
			"number":10,
			},
		"11":{
			"name":"TERM_REQUEUE_ADMIN",
			"desc":"Job killed and requeued by root or LSF administrator (11)",
			"number":11,
			},
		"12":{
			"name":"TERM_CPULIMIT",
			"desc":"Job killed after reaching LSF CPU usage limit (12)",
			"number":12,
		},
		"13":{
			"name":"TERM_CHKPNT",
			"desc":"Job killed after checkpointing (13)",
			"number":13,
			},
		"14":{
			"name":"TERM_OWNER",
			"desc":"Job killed by owner (14)",
			"number":14,
		},
		"15":{
			"name":"TERM_ADMIN",
			"desc":"Job killed by root or LSF administrator (15)",
			"number":15,
			},
		"16":{
			"name":"TERM_MEMLIMIT",
			"desc":"Job killed after reaching LSF memory usage limit (16)",
			"number":16,
			},
		"17":{
			"name":"TERM_EXTERNAL_SIGNAL",
			"desc":"Job killed by a signal external to LSF (17)",
			"number":16,
			},
		"18":{
			"name":"TERM_RMS",
			"desc":"NOT SPECIFIED",
			"number":18,
			},
		"19":{
			"name":"TERM_ZOMBIE",
			"desc":"Job exited while LSF is not available (19)",
			"number":19,
				},
		"20":{
			"name":"TERM_SWAP",
			"desc":"Job killed after reaching LSF swap usage limit (20)",
			"number":20,
			},
		"21":{
			"name":"TERM_THREADLIMIT",
			"desc":"Job killed after reaching LSF thread limit (21)",
			"number":21,
			},
		"22":{
			"name":"TERM_SLURM",
			"desc":"Job terminated abnormally in SLURM (node failure) (22)",
			"number":22,
			},
		"23":{
			"name":"TERM_BUCKET_KILL",
			"desc":"Job killed with bkill -b (23)",
			"number":23,
			},
		}

## When a job is terminated, LSF stores details on the reason why the job was terminated, this
#  class takes the error number and provides an error name and description as attributes.
class TermInfo:
	def __init__(self, id):
		id=str(id)
		if (not id in TERMINFO):
			id="0"
		## The name of the error as specified in lsbatch.h, for example TERM_RUNLIMIT
		self.name=TERMINFO[id]['name']
		## Description of the error as specified in the LSF documenation.
		#  For example: "Job killed after reaching LSF run time limit"
		self.description=TERMINFO[id]['desc']
		## The error number, for example: 5.
		self.number=int(TERMINFO[id]['number'])



## Represents a JOB_FINISH event in the lsb accounting file and provides attributes and methods
#  to access and manipulate the data accordingly.
class JobFinishEvent:
	def __init__(self,row=[]):
		# If the first entry isn't JOB_FINISH, then its the wrong type of accounting entry
		if not row[0]=="JOB_FINISH":
			raise ValueError

		## The eventype is set to JOB_FINISH for this class, and is used to 
		#  identify the name of the event.  There are multiple types of event
		#  stored in the lsb accounting file, this attribute can be used to 
		#  determine which type of event has been logged.
		#\returns The type of event as a string, for JobFinishEvent this is 
		#  always JOB_FINISH
		self.eventType=row.pop(0)

		## Version number of the log file format.  This corresponds the the 
		#  version of LSF used on the cluster.  
		#\returns The LSF Version number as a string.
		self.version=row.pop(0)

		## The epoch time the event was generated (The time the job finished.)
		#\returns The time the event was generated in seconds since epoch as 
		# an integer.
		self.eventTimeEpoch=float(row.pop(0))

		## A datetime object for the time the event was generated (The time 
		#  the job finished)
		#\returns A datetime object.
		self.eventTime=datetime.datetime.utcfromtimestamp(self.eventTimeEpoch)

		## The ID number of the job.
		#\returns Job ID as integer
		self.jobID=int(row.pop(0))

		## The numeric user ID of the user who owned the job.
		#\returns The numeric user id as an integer
		self.userId=int(row.pop(0))

		## Bit flags for job processing
		self.options=row.pop(0)

		## Number of processors initially requested for execution.
		self.numProcessors=int(row.pop(0))

		## The epoch time when the job was submitted.
		self.submitTimeEpoch=float(row.pop(0))

		## A datetime object for when the job was submitted.
		self.submitTime=datetime.datetime.utcfromtimestamp(self.submitTimeEpoch)

		## The epoch time when the job can be started, Job start time . the job should be started at or after this time
		self.beginTimeEpoch=float(row.pop(0))


		## A datetime object for the job start time, the job should be started at or after this time.
		self.beginTime=datetime.datetime.utcfromtimestamp(self.beginTimeEpoch)

		## The epoch time of the Job termination deadline. the job should be terminated by this time.
		self.termTimeEpoch=float(row.pop(0))

		self.termTime=datetime.datetime.utcfromtimestamp(self.termTimeEpoch)

		self.startTimeEpoch=float(row.pop(0))
		self.startTime=datetime.datetime.utcfromtimestamp(self.startTimeEpoch)
		## The user name of the submitter.
		#\returns User Name as string.
		self.userName=row.pop(0)

		## Name of the job queue to which the job was submitted
		#\returns Queue name as string
		self.queue=row.pop(0)

		## Needs some more work...
		self.resReq=row.pop(0)

		self.dependCond=row.pop(0)
		
		self.preExecCmd=row.pop(0)
		
		self.fromHost=row.pop(0)
		
		self.cwd=row.pop(0)
		
		self.inFile=row.pop(0)
		
		self.outFile=row.pop(0)
		
		self.errFile=row.pop(0)
		
		self.jobFile=row.pop(0)
		
		## Number of host names to which job dispatching will be limited
		self.numAskedHosts=int(row.pop(0))
		
		## List of host names to which job dispatching will be limited. Nothing 
		#  is logged to the record for this value if the value of numAskedHosts
		#  is 0. 
		# 
		#\returns Array of hostnames.
		self.askedHosts=[]
		
		# If there is more than one host name, then each additional host name 
		# will be returned in its own field, therefore pop as many times as 
		# numAskedHosts
		i=self.numAskedHosts
		while i>0:
			i-=1
			self.askedHosts.append(row.pop(0))

		## Number of processors used for execution.  If 
		#  LSF_HPC_EXTENSIONS="SHORT_EVENTFILE" is specified in lsf.conf, the 
		#  value of this field is the number of .hosts listed in the execHosts 
		#  field.
		self.numExHosts=int(row.pop(0))

		## List of execution host names (%s for each).  Nothing is logged to the
		#  record for this value if the last field value is 0.
		#
		#  If LSF_HPC_EXTENSIONS="SHORT_EVENTFILE" is specified in lsf.conf, 
		#  the value of this field is logged in a shortened format.
		#
		#  The logged value reflects the allocation at job finish time
		self.execHosts=[]

		# If there is more than one host name, then each additional host name
		# will be returned in its own field, therefore pop as many times as
		# numExHosts
		i=self.numExHosts
		while i>0:
			i-=1
			self.execHosts.append(row.pop(0))

		## Job status. The number 32 represents EXIT, 64 represents DONE.
		self.jStatus=int(row.pop(0))

		## CPU factor of the first execution host.
		self.hostFactor=float(row.pop(0))
		
		## Job name (up to 4094 characters).
		self.jobName=row.pop(0)

		## Complete batch job command specified by the user (up to 4094
		#  characters for UNIX or 512 characters for Windows).
		self.command=row.pop(0)

		sec=float(row.pop(0))
		if (sec<0):
			sec=0
		## User time used in seconds.  If the value of some field is 
		#  unavailable (due to job exit or the difference among the operating 
		#  systems), -1 will be logged. Times are measured in seconds, and sizes 
		#  are measured in KB.
		self.utime=datetime.timedelta(seconds=sec)

		sec=float(row.pop(0))
		if (sec<0):
			sec=0
		## System time used in seconds.  If the value of some field is 
		#  unavailable (due to job exit or the difference among the operating 
		#  systems), -1 will be logged. Times are measured in seconds, and sizes 
		#  are measured in KB.
		self.stime=datetime.timedelta(seconds=sec)

		## Maximum shared text size in KB.  If the value of some field is 
		#  unavailable (due to job exit or the difference among the operating 
		#  systems), -1 will be logged. Times are measured in seconds, 
		#  and sizes are measured in KB.
		self.maxrss=row.pop(0)

		## Integral of the shared text size over time. (in KB Seconds)  If the 
		#  value of some field is unavailable (due to job exit or the difference 
		#  among the operating systems), -1 will be logged. Times are measured 
		#  in seconds, and sizes are measured in KB.
		self.ixrss=row.pop(0)
		
		## Integral of the shared memory size over time. (valid only on Ultrix)
		#  If the value of some field is unavailable (due to job exit or the
		#  difference among the operating systems), -1 will be logged. Times 
		#  are measured in seconds, and sizes are measured in KB. 
		self.ismrss=row.pop(0)
		
		## Integral of the unshared data size over time.  If the value of some
		#  field is unavailable (due to job exit or the difference among the
		#  operating systems), -1 will be logged. Times are measured in seconds,
		#  and sizes are measured in KB. 
		self.idrss=row.pop(0)
		
		## Integral of the unshared stack size over time.  If the value of some
		#  field is unavailable (due to job exit or the difference among the
		#  operating systems), -1 will be logged. Times are measured in seconds,
		#  and sizes are measured in KB. 
		self.isrss=row.pop(0)
		
		## Number of page reclaims.  If the value of some field is unavailable 
		#  (due to job exit or the difference among the operating systems),
		#  -1 will be logged. Times are measured in seconds, and sizes are
		#  measured in KB. 
		self.minflt=row.pop(0)
		
		## Number of page faults.  If the value of some field is unavailable 
		#  (due to job exit or the difference among the operating systems), -1 
		#  will be logged. Times are measured in seconds, and sizes are measured
		#  in KB. 
		self.majflt=row.pop(0)

		## Number of times the process was swapped out.  If the value of some
		#  field is unavailable (due to job exit or the difference among the
		#  operating systems), -1 will be logged. Times are measured in seconds,
		#  and sizes are measured in KB. 
		self.nswap=row.pop(0)

		## Number of block input operations.  If the value of some field is
		#  unavailable (due to job exit or the difference among the operating
		#  systems), -1 will be logged. Times are measured in seconds, and sizes
		#  are measured in KB. 
		self.inblock=row.pop(0)
		
		## Number of block output operations.  If the value of some field is
		#  unavailable (due to job exit or the difference among the operating
		#  systems), -1 will be logged. Times are measured in seconds, and sizes
		#  are measured in KB. 
		self.oublock=row.pop(0)
		
		## Number of characters read and written. (valid only on HP-UX)  If the
		#  value of some field is unavailable (due to job exit or the difference
		#  among the operating systems), -1 will be logged. Times are measured
		#  in seconds, and sizes are measured in KB. 
		self.ioch=row.pop(0)
		
		## Number of System V IPC messages sent.  If the value of some field is
		#  unavailable (due to job exit or the difference among the operating
		#  systems), -1 will be logged. Times are measured in seconds, and sizes
		#  are measured in KB. 
		self.msgsnd=row.pop(0)
		
		## Number of messages received.  If the value of some field is
		#  unavailable (due to job exit or the difference among the operating
		#  systems), -1 will be logged. Times are measured in seconds, and sizes
		#  are measured in KB. 
		self.msgrcv=row.pop(0)
		
		## Number of signals received.  If the value of some field is
		#  unavailable (due to job exit or the difference among the operating
		#  systems), -1 will be logged. Times are measured in seconds, and sizes
		#  are measured in KB. 
		self.nsignals=row.pop(0)
		
		## Number of voluntary context switches.  If the value of some field is
		#  unavailable (due to job exit or the difference among the operating
		#  systems), -1 will be logged. Times are measured in seconds, and sizes
		#  are measured in KB. 
		self.nvcsw=row.pop(0)
		
		## Number of involuntary context switches.  If the value of some field
		#  is unavailable (due to job exit or the difference among the operating
		#  systems), -1 will be logged. Times are measured in seconds, and sizes
		#  are measured in KB. 
		self.nivcsw=row.pop(0)
		
		## Exact user time used. (valid only on ConvexOS)  If the value of some
		#  field is unavailable (due to job exit or the difference among the
		#  operating systems), -1 will be logged. Times are measured in seconds,
		#  and sizes are measured in KB. 
		self.exutime=row.pop(0)

		## Name of the user to whom job related mail was sent
		self.mailUser=row.pop(0)

		## LSF project name.
		self.projectName=row.pop(0)

		## UNIX exit status of the job
		self.exitStatus=int(row.pop(0))

		## Maximum number of processors specified for the job.
		self.maxNumProcessors=int(row.pop(0))

		## Login shell used for the job
		self.loginShell=row.pop(0)

		self.timeEvent=row.pop(0)
		
		self.idx=row.pop(0)
		
		self.maxRMem=row.pop(0)
		
		self.maxRSwap=row.pop(0)
		
		self.inFileSpool=row.pop(0)
		
		self.commandSpool=row.pop(0)
		
		self.rsvId=row.pop(0)
		
		self.sla=row.pop(0)
		
		self.exceptMask=row.pop(0)
		
		self.additionalInfo=row.pop(0)
		
		i=row.pop(0)
		
		self.termInfo=TermInfo(i)
		
		self.warningAction=row.pop(0)
		
		self.warningTimePeriod=row.pop(0)
		
		## The Share Attribute Account Path (SAAP) that was charged for the job
		#  under fair share scheduling.
		self.chargedSAAP=row.pop(0)
		
		self.licenseProject=row.pop(0)

		if self.startTimeEpoch<1:
			# job never started
			# Possibly change to raise exception
			self.runTime=datetime.timedelta(0)
		else:
			self.runTime=self.eventTime-self.startTime

		if self.startTimeEpoch <1:
			# Job never started
			self.waitTime=self.eventTime-self.submitTime
			self.startTime=self.termTime
		else:
			self.waitTime=self.startTime-self.submitTime
		## The time the job was pending.  
		self.pendTime=self.waitTime


## Parses the LSB accounting file, and returns an iterator that can be used to
#  get the details for each job entry.  Each job that is successfully submitted
#  into LSF has an entry created in the LSF accounting file.  This stores 
#  accounting information about the job that can be used at a later date to 
#  build usage information. 
#
#  The file itself is a CSV file containing a number of possible entries, the
#  primary entry being JOB_FINISHED events that store information about jobs
#  that have been through the queuing system.
#
#  To use the class, pass an open file handle to the lsb accounting file as 
#  the argument to the initializer, irerate over this to go through each entry
#  in the LSB accounting file. 
#
#  Each iteration will return an appropriate object based upon the row in the 
#  file. 
#
# Example Usage:
#
# The following code prints out the queue for each job found.
#\code
#from lsfpy.accounting import AcctFile
#for i in AcctFile(open('lsb.acct','r')):
#    print i.queue
#\endcode

class AcctFile:
	## Initializer is called with an open file handle object opened to the 
	#  lsb accounting file.
	#\param fh An open file object to the accounting file.
	def __init__(self, fh):
		self.reader=csv.reader(fh,delimiter=' ', quotechar='"')

	def __iter__(self):
		return self

	## Iterator function is called each iteration and parses the next line of
	#  the config file returning an event object for the specific event.
	#\return An event object corresponding to the type of event in the log file.
	def next(self):
		try:
			j=JobFinishEvent(self.reader.next())
		except:
			j=JobFinishEvent(self.reader.next())
		return j
