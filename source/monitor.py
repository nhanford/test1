#!/usr/bin/env python
# encoding: utf-8
'''
interrupts -- Deals with affinity and flow throttling using the proc file system and tc, respectively.

It defines classes_and_methods

@author:     Nathan Hanford

@contact:    nhanford@es.net
@deffield    updated: Updated
'''

#print 'debug'
import sys,os,re,subprocess,socket,sched,time,datetime,threading,sqlite3,struct
#from argparse import ArgumentParser
#from argparse import RawDescriptionHelpFormatter

__all__ = []
__version__ = 0.1
__date__ = '2015-06-22'
__updated__ = '2015-08-24'

SPEEDCLASSES = [(900,'1:2'),(4900,'1:3'),(9900,'1:4')]

DEBUG = 0
TESTRUN = 0

#class DB(object):
#    _db_conn = None
#    _db_c = None
#
#    def __init__(self):
#        self._db_conn = sqlite3.connect('connections.db')
#        self._db_c = self.conn.cursor()
#
#    def query(self, query, params):
#        return self._db_c.execute(query, params)
#
#    def __del__(self):
#        self._db_conn.close()
#
class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = 'E: %s' % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg
#Here are the functions that poll the proc filesystem, etc.
#So there are a lot of issues with the below, but I'm not going to address them just yet..
def checkibalance():
    try:
        stat = subprocess.check_call(['service','irqbalance','stop'])
    except subprocess.CalledProcessError:
        print 'Cound not stop irqbalance'
        return 1
    return 0


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
        file = open('/proc/irq/'+i+'/smp_affinity','r')
        thisAffinity=file.read().strip()
        affinity[i]=thisAffinity
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
    if any(irqlist):
        return irqlist
    driver = subprocess.check_output(['ethtool','-i',iface])
    if 'mlx4' in driver:
        file.seek(0)
        for line in file:
            line.strip()
            line = re.search('.+'+'mlx4',line)
            if(line):
                line = re.search('\d+:',line.group(0))
                line = re.search('\d+',line.group(0))
                irqlist.append(line.group(0))
        return irqlist
    print 'Cannot find this interface\'s irq numbers.'
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
        strmask = '%x' % mask
        while len(strmask)<numdigits:
            strmask = '0'+strmask
        mask = mask << 1
        try:
            smp = open('/proc/irq/'+key+'/smp_affinity','w')
            smp.write(strmask)
            smp.close()
        except IOError:
            print 'Could not write the smp_affinity file'
        irqcount +=1
    return

def setperformance(numcpus):
    for i in range(numcpus):
        try:
            throttle = open('/sys/devices/system/cpu/cpu'+i+'/cpufreq/scaling_governor', 'w')
            throttle.write('performance')
            throttle.close()
        except IOError:
            print 'Could not set CPUs to performance'
    return

def getlinerate(iface):
    out = subprocess.check_output(['ethtool',iface])
    speed = re.search('.+Speed:.+',out)
    speed = re.sub('.+Speed:\s','',speed.group(0))
    speed = re.sub('Mb/s','',speed)
    if 'Unknown' in speed:
        print 'Line rate for this interface is unknown: you probably need to enable it.'
        exit()
    return speed

def setthrottles(iface):
    try:
        stat = subprocess.check_call(['tc','qdisc','del','dev',iface,'root'])
    except subprocess.CalledProcessError as e:
        if e.returncode != 2:
            raise e
    try:
        subprocess.check_call(['tc','qdisc','add','dev',iface,'handle','1:','root','htb'])
    except subprocess.CalledProcessError as e:
        print e
        print e.returncode
        return
    for speedclass in SPEEDCLASSES:
        print speedclass[0]
        print speedclass[1]
        try:
            subprocess.check_call(['tc','class','add','dev',iface,'parent','1:','classid',speedclass[1],'htb','rate',str(speedclass[0])+'mbit'])
        except:
            print 'Could not interface with os to initialize tc settings.'
            return
    return

def parseconnections(connections):
    #print connections
    conn = conn = sqlite3.connect('connections.db')
    c = conn.cursor()
    for connection in connections:
        connection = connection.strip()
        ordered = re.sub(':|,|/|Mbps',' ',connection)
        ordered = connection.split()
        #print 'State is: ',ordered[0]
        ips = re.findall('\d+\.\d+\.\d+\.\d+',connection)
        #if ips:
        #    print 'Source IP is:',ips[0]
        #    print 'Destination IP is:', ips[1]
        #else:
        #    print 'Source IP is: None'
        #    print 'Destination IP is None'
        ports = re.findall('\d:\w+',connection)
        #if ports:
        #    print 'Source port is:',ports[0][2:]
        #    print 'Destination port is',ports[1][2:]
        #else:
        #    print 'Source port is: None'
        #    print 'Destination port is None'
        #rtt: first value is the average rtt; the second value is the variance
        rtt = re.search('rtt:\d+[.]?\d+',connection)
        if rtt:
            rtt = rtt.group(0)[4:]
            #print 'Average RTT is:',rtt
        else:
            rtt = '-1'
        wscaleavg = re.search('wscale:\d+',connection)
        if wscaleavg:
            wscaleavg = wscaleavg.group(0)[7:]
        else:
            wscaleavg = '-1'
        cwnd = re.search('cwnd:\d+',connection)
        if cwnd:
            cwnd = cwnd.group(0)[5:]
            #print 'cwnd is',cwnd
        else:
            cwnd = '-1'
        retrans = re.search('retrans:\d+\/\d+',connection)
        if retrans:
            retrans = retrans.group(0)
            retrans = re.sub('retrans:\d+\/','',retrans)
        #    print 'Number of retransmits is:',retrans
        else:
            #print 'Number of retransmits is not available'
            retrans = '0'
        sendrate = re.search('send \d+.\d+',connection)
        if sendrate:
            sendrate = sendrate.group(0)[5:]
        else:
            sendrate = '-1'
        if len(ips) > 1 and len(ports) > 1 and rtt and cwnd and retrans and sendrate:
        #Assemble Query String
            query = 'INSERT INTO conns (sourceip, destip, sourceport, destport, rttavg, wscaleavg, cwnd, sendrate, retrans) VALUES(\''+ips[0]+'\', \''+ips[1]+'\', '+ports[0][2:]+', '+ports[1][2:]+', '+rtt+', '+wscaleavg+', '+cwnd+', '+sendrate+', '+retrans+')'
            #print query
            try:
                c.execute(query)
            except sqlite3.IntegrityError:
                #print 'found a duplicate'
                #this will be where I do a comparison and throttle appropriately...
                query = 'REPLACE INTO conns (sourceip, destip, sourceport, destport, rttavg, wscaleavg, cwnd, sendrate, retrans) VALUES(\''+ips[0]+'\', \''+ips[1]+'\', '+ports[0][2:]+', '+ports[1][2:]+', '+rtt+', '+wscaleavg+', '+cwnd+', '+sendrate+', '+retrans+')'
                c.execute(query)
            #c.execute('SELECT * FROM conns')
            #print c.fetchall()
    conn.commit()
    conn.close()

def dbinit():
	#print 'debug'
	conn = sqlite3.connect('connections.db')
	c = conn.cursor()
	try:
		c.execute('''SELECT * FROM conns''')
		#print 'worked'
	except sqlite3.OperationalError:
		print 'Table doesn\'t exist; Creating table...'
	#c.execute('''CREATE TABLE conns (state text,
	#    recvq       int,
	#    sendq       int,
	#    sourceip    text    NOT NULL,
	#    sourceport  text    NOT NULL,
	#    destip      text    NOT NULL,
	#    destport    text    NOT NULL,
	#    iface       text,
	#    tcp         text,
	#    wscaleavg   int,
	#    wscalemax   int,
	#    rto         int,
	#    rttavg      real,
	#    ato         int,
	#    mss         int,
	#    cwnd        int,
	#    ssthresh    int,
	#    sendrate    real,
	#    pacrate     real,
	#    retrans     int,
	#    rcvrtt      int,
	#    rcvspace    int,
	#    PRIMARY KEY (sourceip, sourceport, destip, destport));''')
		try:
			c.execute('''CREATE TABLE conns (
				sourceip    text    NOT NULL,
				destip      text    NOT NULL,
				sourceport  int    NOT NULL,
				destport    int    NOT NULL,
				rttavg      real,
                wscaleavg   real,
				cwnd		int,
				sendrate    real,
				retrans     int,
				PRIMARY KEY (sourceip, sourceport, destip, destport));''')
		except sqlite3.OperationalError:
			print 'I don\'t know you. Recreating Database.'
			c.execute('''DROP TABLE conns''')
			c.execute('''CREATE TABLE conns (
				sourceip    text    NOT NULL,
				destip      text    NOT NULL,
				sourceport  int    NOT NULL,
				destport    int    NOT NULL,
				rttavg      real,
                wscaleavg   real,
				cwnd		int,
				sendrate    real,
				retrans     int,
				PRIMARY KEY (sourceip, sourceport, destip, destport));''')
	conn.commit()
	conn.close()

def throttleoutgoing(iface,ipaddr,speedclass):
    success = subprocess.check_call(['tc','filter','add',iface,'parent','1:','protocol','ip','prio','1','u32','match','ip','dst',ipaddr+'/32','flowid',speedclass[1]])
    return success

def pollss():
    out = subprocess.check_output(['ss','-i','-t','-n'])
    out = re.sub('\A.+\n','',out)
    out = re.sub('\n\t','',out)
    out = out.splitlines()
    return out

def polltcp():
    tcp = open('/proc/net/tcp','r')
    out = tcp.readlines()
    out = out[1:]
    tcp.close()
    #tcp6 = open('/proc/net/tcp6','r')
    #out6 = tcp6.readlines()
    #out6 = out6[1:]
    #tcp6.close()
    #out += out6
    return out

def parsetcp(connections):
    conn = sqlite3.connect('connections.db')
    c = conn.cursor()
    for connection in connections:
        connection = connection.strip()
        connection = connection.split()
        if connection[1] != '00000000:0000' and connection[2] != '00000000:0000':
            #print connection
            sourceip = connection[1].split(':')[0]
            sourceport = connection[1].split(':')[1]
            sourceip = int(sourceip,16)
            sourceip = struct.pack('<L',sourceip)
            sourceip = socket.inet_ntoa(sourceip)
            sourceport = int(sourceport,16)
            destip = connection[2].split(':')[0]
            destport = connection[2].split(':')[1]
            destip = int(destip,16)
            destip = struct.pack('<L',destip)
            destip = socket.inet_ntoa(destip)
            destport = int(destport,16)
            retrans = int(connection[6],16)
            #print sourceip, sourceport, destip, destport, retrans
            query = 'SELECT retrans FROM conns WHERE sourceip = \'{sip}\' AND sourceport = {spo} AND destip = \'{dip}\' AND destport = {dpo}'.format(sip=sourceip, spo=sourceport, dip=destip, dpo=destport)
            c.execute(query)
            tempretr = c.fetchall()
            #print tempretr
            if len(tempretr)==0:
                tempretr = 0
            else:
                tempretr = int(tempretr[0][0])
            retrans += tempretr
            query = 'UPDATE conns SET retrans={retr} WHERE sourceip=\'{sip}\' AND sourceport=\'{spo}\' AND destip=\'{dip}\' AND destport=\'{dpo}\''.format(retr=int(retrans), sip=str(sourceip), spo=str(sourceport), dip=str(destip), dpo=str(destport))
            #print query
            c.execute(query)
            #c.execute('SELECT * FROM conns')
            #print c.fetchall()
            #print '-----------'
    conn.commit()
    conn.close()

def doconns():
    connections = pollss()
    parseconnections(connections)
    tcpconns = polltcp()
    parsetcp(tcpconns)
    print datetime.datetime.now()
    threading.Timer(5, doconns).start()

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)
    program_name = os.path.basename(sys.argv[0])
    program_version = 'v%s' % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split('\n')[1]
    program_license = '''%s


USAGE
''' % (program_shortdesc)

#    try:
        # Setup argument parser
#        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        #parser.add_argument('interface', metavar='interface', action='store', help='specify the interface name of your network controller (i.e. eth1)')
        # Process arguments
#        args = parser.parse_args()
        #interface = args.interface

#    except KeyboardInterrupt:
#        print 'Operation Cancelled\n'
#        return 0
#    except Exception, e:
#        if DEBUG or TESTRUN:
#            raise(e)
#        indent = len(program_name) * ' '
#        sys.stderr.write(program_name + ': ' + repr(e) + '\n')
#        sys.stderr.write(indent + '  for help use --help'+'\n')
#        return 2
	#print 'debug'
    dbinit()
    #checkibalance()
    #numcpus = pollcpu()
    #print 'The number of cpus is:', numcpus
    #irqlist = pollirq(interface)
    #affinity = pollaffinity(irqlist)
    #print affinity
    #setaffinity(affinity,numcpus)
    #linerate = getlinerate(interface)
    #setthrottles(interface)
    #threading.Timer(5, doconns, [interface]).start()
    doconns()
    #conns = pollss(interface)
    #conns = polltcp()

if __name__ == '__main__':
    if DEBUG:
        sys.argv.append('-h')
    if TESTRUN:
        import doctest
        doctest.testmod()
    sys.exit(main())
#    with daemon.DaemonContext():
#        sys.exit(main())
