#!/usr/bin/env python
# encoding: utf-8
'''
interrupts -- Deals with affinity and flow throttling using the proc file system and tc, respectively.

It defines classes_and_methods

@author:     Nathan Hanford

@contact:    nhanford@es.net
@deffield    updated: Updated
'''

import sys,os,re,subprocess,math,socket,sched,time,threading
from pyroute2 import IPRoute
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

__all__ = []
__version__ = 0.1
__date__ = '2015-06-22'
__updated__ = '2015-07-01'

DEBUG = 0
TESTRUN = 0

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

#Here are the functions that poll the proc filesystem, etc.
def checkibalance():
    p = subprocess.Popen(["service","irqbalance","status"], stdout=subprocess.PIPE)
    out,err = p.communicate()
    if "stop" in out or "inactive" in out:
        print "irqbalance is off\n"
        return True
    else:
        print "Please stop irqblance with \n [sudo] service irqbalance stop\n"
        return False

def pollcpu():
    try:
        file = open('/proc/cpuinfo','r')
    except IOError:
        print 'It appears that this system is incompatible with the proc file system\n'
    numcpus=0
    for line in file:
        line.strip()
        if re.search('processor',line):
            numcpus +=1
    file.close
    return numcpus

def pollaffinity(irqlist):
    affinity = dict()
    for i in irqlist:
        openstring = "/proc/irq/"+i+"/smp_affinity"
        file = open(openstring)
        affinity[i]=file.read().strip()
    return affinity

def pollirq(iface):
    file = open('/proc/interrupts','r')
    irqlist=[]
    for line in file:
        line.strip()
        line = re.search('.+'+iface,line)
        if(line):
            line = re.search('\d+:',line.group(0))
            line = re.search('\d+',line.group(0))
            irqlist.append(line.group(0))
    file.close()
    if any(irqlist):
        return irqlist
    else:
        print "Cannot find this interface's irq numbers."
        exit()
    #yeah I know that's sloppy; will fix by raising an exception when I get my exception hierarchy figured out.

def setaffinity(affy,numcpus):
    numdigits = numcpus/4
    mask = 1
    irqcount = 0
    for key in affy:
        if irqcount > numcpus - 1:
            mask = 1
            irqcount = 0
        strmask = "%x" % mask
        while len(strmask)<numdigits:
            strmask = "0"+strmask
        mask = mask << 1
        try:
            smp = open("/proc/irq/"+key+"/smp_affinity","w")
            smp.write(strmask)
            smp.close()
        except IOError:
            print("Could not write the smp_affinity file")
        irqcount +=1
    return

def getlinerate(iface):
    p = subprocess.Popen(["ethtool",iface], stdout=subprocess.PIPE)
    out,err = p.communicate()
    speed = re.search('.+Speed:.+',out)
    speed = re.sub('.+Speed:\s','',speed.group(0))
    speed = re.sub('Mb/s','',speed)
    return speed

def throttleoutgoing(iface,linerate):
    ip = IPRoute()
    interface = ip.link_lookup(ifname=iface)[0]
    ip.tc('del','htb',interface,0x10000, default=0x200000)
    ip.tc('add','htb',interface,0x10000, default=0x200000)
    ip.tc('add-class','htb',interface,0x10001,parent=0x10000,rate=linerate+'mbit',burst=1024*6)
    return

def pollconnections(iface):
    pass

def throttleincoming(connection):
    pass

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)
    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s


USAGE
''' % (program_shortdesc)

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("interface", metavar="interface", action="store", help="specify the interface name of your network controller (i.e. eth1)")
        # Process arguments
        args = parser.parse_args()
        interface = args.interface

    except KeyboardInterrupt:
        print "Operation Cancelled\n"
        return 0
    except Exception, e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help"+"\n")
        return 2
    if checkibalance():
        numcpus = pollcpu()
        print "The number of cpus is:", numcpus
        irqlist = pollirq(interface)
        affinity = pollaffinity(irqlist)
        print(affinity)
        setaffinity(affinity,numcpus)
        linerate = getlinerate(interface)
        print linerate
        if(int(linerate)<=10000):
            throttleoutgoing(interface,linerate)


if __name__ == "__main__":
    if DEBUG:
        sys.argv.append("-h")
    if TESTRUN:
        import doctest
        doctest.testmod()
    sys.exit(main())
