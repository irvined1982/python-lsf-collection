#!/usr/bin/python
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

from optparse import OptionParser
import os
import csv
import datetime
from lsfpy.accounting import *

p=OptionParser()
p.add_option("-f", "--file", dest="filename",help="Read from lsb accounting FILE", metavar="FILE")

(options, args)=p.parse_args()

if not options.filename:
	print "No filename specified."
	sys.exit(253)
	
if not os.path.isfile(options.filename):
	print "File does not exist: %s" % options.filename
	sys.exit(255)
	
try:
	acctf=open(options.filename,'r')
except IOError as e:
	print 'File cannot be opened.'
	sys.exit(254)
	

# Dictionary containing an entry for each queue, which is in itself a dictionary 
# containing the stats for the queue
qs={}
us={}

for i in AcctFile(acctf):
	# If the queue does not have an entry in the dictionary, then create
	# one now.
	if not i.queue in qs:
		qs[i.queue]={
				'name':i.queue,
				'numJobs':0,
				'numFJobs':0,
				'waitTime':datetime.timedelta(0),
				'runTime':datetime.timedelta(0),
				'wallTime':datetime.timedelta(0),
				'wasteTime':datetime.timedelta(0),
				}
	# Based on the queue, increment the timers and counters accordingly
	# increment the number of jobs
	qs[i.queue]['numJobs']+=1
	# Add the time the job had to wait before it was started
	qs[i.queue]['waitTime']+=i.waitTime
	# Work out the CPU time, this is the wall clock time multiplied by the 
	# number of slots.
	qs[i.queue]['runTime']+=(i.numProcessors*i.runTime)
	# Add the wall clock time
	qs[i.queue]['wallTime']+=i.runTime
	# If the terminfo number is >0, then it was not a normal exit status.  Add
	# the cpu time to the wasted time.
	if i.termInfo.number>0:
		qs[i.queue]['wasteTime']+=(i.numProcessors*i.runTime)
		qs[i.queue]['numFJobs']+=1

	# Do the same based on the user name.
	if not i.userName in us:
		us[i.userName]={
				'name':i.userName,
				'numJobs':0,
				'numFJobs':0,
				'waitTime':datetime.timedelta(0),
				'runTime':datetime.timedelta(0),
				'wallTime':datetime.timedelta(0),
				'wasteTime':datetime.timedelta(0),
				}
	us[i.userName]['numJobs']+=1
	us[i.userName]['waitTime']+=i.waitTime
	us[i.userName]['runTime']+=(i.numProcessors*i.runTime)
	us[i.userName]['wallTime']+=i.runTime
	if i.termInfo.number>0:
		us[i.userName]['wasteTime']+=(i.numProcessors*i.runTime)
		us[i.userName]['numFJobs']+=1


# Print out a summary per queue.
for q in qs.values():
	print "Name: %s" % q['name']
	print " Total Jobs:      %d" % q['numJobs']
	print " Failed Jobs:     %d" % q['numFJobs']
	print " Total Wait Time: %s" % q['waitTime']
	print " Total Wall Time: %s" % q['wallTime']
	print " Total CPU Time:  %s" % q['runTime']
	print " Total Terminated CPU Time: %s" %q['wasteTime']


# Print out a summary per user, highest abuser at the top.
for u in sorted(us.values(), key=lambda k: k['wasteTime'], reverse=True):
	print "Name: %s" % u['name']
	print " Total Jobs:      %d" % u['numJobs']
	print " Failed Jobs:     %d" % u['numFJobs']
	print " Total Wait Time: %s" % u['waitTime']
	print " Total Wall Time: %s" % u['wallTime']
	print " Total CPU Time:  %s" % u['runTime']
	print " Total Terminated CPU Time: %s" %u['wasteTime']
