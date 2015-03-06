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
	


for i in AcctFile(acctf):
	print i.queue
