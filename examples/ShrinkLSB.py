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
#

from optparse import OptionParser
import os
import csv
import datetime
from lsfpy.accounting import *

p=OptionParser()
p.add_option("-f", "--file", dest="filename",help="Read from lsb accounting FILE", metavar="FILE")
p.add_option("-o", "--output", dest="output",help="Write CSV data to file ", metavar="OUTPUT")

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
	
if not options.output:
	print 'No output filename specified.  You must specify a filename to write the data to.'
	sys.exit(252)

try:
	of=csv.writer(open(options.output, 'wb'), dialect='excel')
	of.writerow([
		'User Name',
		'Project Name',
		'Num Processors',
		'Queue Name',
		'Job Exit Status',
		'Termination Reason',
		'Command',
		'Submit Month',
		'SAAP Name',
		'Number of Jobs',
		'Total Pend Time',
		'Total CPU Time',
		'Total Wallclock Time',
   ])
except IOError as e:
	print 'Output file cannot be opened.'
	sys.exit(253)








buckets={}



for job in AcctFile(acctf):
	# The following fields are stored:
	#  userName
	#  project
	#  numProcessors
	#  queue
	#  exitStatus
	#  termInfo
	#  command without arguments
	#  SAAP
	fields=[
			job.userName,
			job.projectName,
			job.numProcessors,
			job.queue,
			job.jStatus,
			job.termInfo.name,
			job.command.split(" ")[0],
			"%s-%s" % (job.submitTime.year,job.submitTime.month),
			job.chargedSAAP,
			]
	# Iterate through each value of vield, if there is no bucket
	# for that value yet, then create it.
	bucket=buckets
	for f in fields:
		if not f in bucket:
			bucket[f]={}
		bucket=bucket[f]
	# If there are no entries for the metrics in the bucket, create them
	if not "numJobs" in bucket:
		bucket['numJobs']=0
		bucket['pendTime']=datetime.timedelta(0)
		bucket['CPUTime']=datetime.timedelta(0)
		bucket['wallTime']=datetime.timedelta(0)
		bucket['__END__']=True

	bucket['numJobs']+=1
	bucket['pendTime']+=job.waitTime
	bucket['CPUTime']+=(job.numProcessors*job.runTime)
	bucket['wallTime']+=job.runTime




def iterBuckets(items, bucket):
	if '__END__' in bucket:
		line=[]
		for i in items:
			line.append(str(i))
		for i in (bucket['numJobs'],bucket['pendTime'].total_seconds(),bucket['CPUTime'].total_seconds(),bucket['wallTime'].total_seconds()):
			line.append(str(i))
		of.writerow(line)
	else:
		for key in bucket.keys():
			ritems=items[:]
			ritems.append(key)
			iterBuckets(ritems,bucket[key])


items=[]
iterBuckets(items, buckets)


