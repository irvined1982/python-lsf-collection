The Python LSF collection is a set of python classes that can be used to pull information out of LSF.  It includes a parser for the LSB accounting file that can be used to pull information easily.  It doesn’t have any third party dependencies and is a pure python solution.  Its written with LSF 7.x in mind, but should work identically on 6.x and 8.x too.

#Synopsis

Use the AcctFile class to read in the lab accounting file, pass it a file object open on the lab accounting file then iterate over it to get an object for each entry in the file.

For example to print a summary of each queue, the number of jobs, and the total wall clock time, cpu time, and time spent waiting on the job use the following:


```
#Open the accounting file
acctf=open('lsb.acct','r')

# Iterate over it using the AcctFile object
for i in AcctFile(acctf):
# i is a JobFinishEvent object that has an attribute for each value in the LSB entry.
    if not i.queue in qs:
        qs[i.queue]={
                'name':i.queue,
                'numJobs':0,
                'waitTime':datetime.timedelta(0),
                'runTime':datetime.timedelta(0),
                'wallTime':datetime.timedelta(0),
                }
    qs[i.queue]['numJobs']+=1
    qs[i.queue]['waitTime']+=i.waitTime
    qs[i.queue]['runTime']+=(i.numProcessors*i.runTime)
    qs[i.queue]['wallTime']+=i.runTime

for q in qs.values():
    print "Name: %s" % q['name']
    print " Jobs: %d" % q['numJobs']
    print " Total Wait Time: %s" % q['waitTime']
    print " Total Wall Time: %s" % q['wallTime']
    print " Total CPU Time:  %s" % q['runTime']
```
Running this code would produce output similar to the following:
```
Name: Batch
 Jobs: 1996
 Total Wait Time: 3 days, 0:13:41
 Total Wall Time: 51 days, 14:49:21
 Total CPU Time:  67 days, 12:51:41
Name: Interactive
 Jobs: 4
 Total Wait Time: 1:08:32
 Total Wall Time: 0:04:10
 Total CPU Time:  0:04:10
```
