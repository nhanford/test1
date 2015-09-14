#!/usr/bin/env python
# encoding: utf-8
'''
monitor -- Monitors DTN flow data and stores the results in a sqlite database.

monitor is designed for Ubuntu and CentOS Linux running Python 2.7

@author:     Nathan Hanford

@contact:    nhanford@es.net
@deffield    updated: Updated
'''

import sys,os,re,subprocess,socket,sched,time,datetime,threading,sqlite3,struct,argparse,json

__all__ = []
__version__ = 0.8
__date__ = '2015-06-22'
__updated__ = '2015-09-01'

SPEEDCLASSES = [(800,'1:2',1000),(4500,'1:3',5000),(9500,'1:4',10000)]

DEBUG = 0
TESTRUN = 0
SKIPAFFINITY = 1

class CLIError(Exception):
    '''generic exception to raise and log different fatal errors'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = 'E: {}'.format(msg)
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def checkibalance():
    '''attempts to disable irqbalance'''
    try:
        stat = subprocess.check_call(['service','irqbalance','stop'])
    except subprocess.CalledProcessError as e:
        global SKIPAFFINITY = 0
        if DEBUG or TESTRUN:
            raise e
    return 0

def pollcpu():
    '''determines the number of cpus in the system'''
    try:
        pfile = open('/proc/cpuinfo','r')
    except IOError as e:
        raise ProcError(e,'Unable to read /proc/cpuinfo')
    numcpus=0
    for line in pfile:
        line.strip()
        if re.search('processor',line):
            numcpus +=1
    pfile.close()
    return numcpus

def pollaffinity(irqlist):
    '''determines the current affinity scenario'''
    affinity = dict()
    for i in irqlist:
        try:
            pfile = open('/proc/irq/{}/smp_affinity'.format(i),'r')
        except IOError as e:
            raise ProcError(e,'unable to read /proc/irq/{}/smp_affinity'.format(i))
        thisAffinity=pfile.read().strip()
        affinity[i]=thisAffinity
    pfile.close()
    return affinity

def pollirq(iface):
    '''determines the irq numbers of the given interface'''
    try:
        irqfile = open('/proc/interrupts','r')
    except IOError as e:
        raise ProcError(e,'unable to read /proc/interrupts')
    irqlist=[]
    for line in irqfile:
        line.strip()
        line = re.search('.+'+iface,line)
        if(line):
            line = re.search('\d+:',line.group(0))
            line = re.search('\d+',line.group(0))
            irqlist.append(line.group(0))
    if any(irqlist):
        irqfile.close()
        return irqlist
    driver = subprocess.check_output(['ethtool','-i',iface])
    if 'mlx4' in driver:
        irqfile.seek(0)
        for line in irqfile:
            line.strip()
            line = re.search('.+'+'mlx4',line)
            if(line):
                line = re.search('\d+:',line.group(0))
                line = re.search('\d+',line.group(0))
                irqlist.append(line.group(0))
        irqfile.close()
        return irqlist
    raise ProcError(Exception,'unable to process /proc/interrupts')

def setaffinity(affy,numcpus):
    '''naively sets the affinity based on industry best practices for a multiqueue NIC'''
    numdigits = numcpus/4
    mask = 1
    irqcount = 0
    for key in affy:
        if irqcount > numcpus - 1:
            mask = 1
            irqcount = 0
        strmask = '%x'.format(mask)
        while len(strmask)<numdigits:
            strmask = '0'+strmask
        mask = mask << 1
        try:
            smp = open('/proc/irq/{}/smp_affinity'.format(key),'w')
            smp.write(strmask)
            smp.close()
        except IOError as e:
            raise ProcError(e,'Could not write /proc/irq/{}/smp_affinity'.format(key))
        irqcount +=1
    return

def setperformance(numcpus):
    '''sets all cpus to performance mode'''
    for i in range(numcpus):
        try:
            throttle = open('/sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor'.format(i), 'w')
            throttle.write('performance')
            throttle.close()
        except IOError as e:
            raise ProcError(e, 'Could not write to /sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor'.format(i))
    return

def getlinerate(iface):
    '''uses ethtool to determine the linerate of the selected interface'''
    try:
        out = subprocess.check_output(['ethtool',iface])
    except subprocess.CalledProcessError as e:
        raise TCError(e,'Could not get linerate for {}'.format(iface))
    speed = re.search('.+Speed:.+',out)
    speed = re.sub('.+Speed:\s','',speed.group(0))
    speed = re.sub('Mb/s','',speed)
    if 'Unknown' in speed:
        raise TCError(Exception,'Line rate for this interface is unknown: interface could be disabled')
    return speed

def setthrottles(iface):
    '''sets predefined common throttles in tc'''
    try:
        stat = subprocess.check_call(['tc','qdisc','del','dev',iface,'root'])
    except subprocess.CalledProcessError as e:
        if e.returncode != 2:
            raise TCError(e,'Could not execute qdisc management on {}'.format(iface))
    try:
        subprocess.check_call(['tc','qdisc','add','dev',iface,'handle','1:','root','htb'])
    except subprocess.CalledProcessError as e:
        raise TCError(e,'Could not add root qdisc for {}'.format(iface))
    for speedclass in SPEEDCLASSES:
        print speedclass[0]
        print speedclass[1]
        try:
            subprocess.check_call(['tc','class','add','dev',iface,'parent','1:','classid',speedclass[1],'htb','rate',str(speedclass[0])+'mbit'])
        except subprocess.CalledProcessError as e:
            raise TCError(e,'could not add class {s} for interface {i}'.format(s=speedclass[2],i=iface))
    return

def loadconnections(connections):
    '''oversees pushing the connections into the database'''
    conn = conn = sqlite3.connect('connections.db')
    c = conn.cursor()
    numnew,numupdated = 0,0
    for connection in connections:
        try:
            ips, ports, rtt, wscaleavg, cwnd, retrans = parseconnection(connection)
            if rtt<0:
                print connection,'had an invalid rtt.'
            if wscaleavg<0:
                print connection,'had an invalid wscaleavg.'
            if cwnd<0:
                print connection,'had an invalid cwnd.'
            if retrans<0:
                print connection,'had an invalid retrans.'
            iface = findiface(ips[1])
        except ValueError:
            #connection was empty: usually localhost<-->localhost
            continue
        try:
            dbinsert(c,ips[0],ips[1],ports[0],ports[1],rtt,wscaleavg,cwnd,retrans,iface,0,0)
            numnew +=1
        except sqlite3.IntegrityError:
            flownum,recent = dbcheckrecent(c,ips[0],ips[1],ports[0],ports[1])
            if not recent:
                flownum+=1
                dbinsert(c,ips[0],ips[1],ports[0],ports[1],rtt,wscaleavg,cwnd,retrans,iface,0,flownum)
                numnew +=1
            else:
                intervals = int(dbselectval(c,ips[0],ips[1],ports[0],ports[1],'intervals'))
                intervals += 1
                oldrtt = int(dbselectval(c,ips[0],ips[1],ports[0],ports[1],'rttavg'))
                if 0<oldrtt<rtt:
                    rtt = oldrtt
                dbupdateconn(c,ips[0],ips[1],ports[0],ports[1],rtt,wscaleavg,cwnd,retrans,iface,intervals,flownum)
                numupdated +=1
        except Exception as e:
            print e
            msg = '''could not load connection into database:{}'''.format(connection)
            print msg
            raise DBError(e,msg)
    conn.commit()
    conn.close()
    print '{numn} new connections loaded and {numu} connections updated at time {when}'.format(numn=numnew, numu=numupdated,
        when=datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%s'))
    return

def dbcheckrecent(cur, sourceip, destip, sourceport, destport):
    '''checks to see if this flow has been recently seen'''
    query = '''SELECT flownum FROM conns WHERE
        sourceip = \'{sip}\' AND
        destip = \'{dip}\' AND
        sourceport = {spo} AND
        destport = {dpo} AND
        strftime('%s', datetime('now')) - strftime('%s', modified) <= 30
        ORDER BY flownum DESC LIMIT 1'''.format(
            sip=sourceip,
            dip=destip,
            spo=sourceport,
            dpo=destport)
    try:
        cur.execute(query)
        out = cur.fetchall()
    except Exception as e:
        raise DBError(e,'query {} failed'.format(query))
    if len(out)>0:
        return int(out[0][0]), True
    else:
        return int(dbselectval(cur, sourceip, destip, sourceport, destport, 'flownum')), False

def isip6(ip):
    '''determines if an ip address is v4 or v6'''
    try:
        socket.inet_aton(ip)
        return False
    except socket.error:
        try:
            socket.inet_pton(socket.AF_INET6,ip)
            return True
        except socket.error:
            return -1

def findiface(ip):
    '''determines the interface responsible for a particular ip address'''
    ip6 = isip6(ip)
    if ip6 == -1:
        return -1
    elif ip6:
        try:
            dev = subprocess.check_output(['ip','-6','route','get',ip])
            dev = re.search('dev\s+\S+',dev).group(0).split()[1]
        except subprocess.CalledProcessError as e:
            raise TCError(e,'unable to find interface for {}'.format(ip))
        return dev
    else:
        try:
            dev = subprocess.check_output(['ip','route','get',ip])
            dev = re.search('dev\s+\S+',dev).group(0).split()[1]
        except subprocess.CalledProcessError as e:
            raise TCError(e,'unable to find interface for {}'.format(ip))
        return dev

def parseconnection(connection):
    connection = connection.strip()
    ordered = re.sub(':|,|/|Mbps',' ',connection)
    ordered = connection.split()
    ips = re.findall('\d+\.\d+\.\d+\.\d+',connection)
    ports = re.findall('\d:\w+',connection)
    rtt = re.search('rtt:\d+[.]?\d+',connection)
    if rtt:
        rtt = float(rtt.group(0)[4:])
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
    else:
        cwnd = '-1'
    retrans = re.search('retrans:\d+\/\d+',connection)
    if retrans:
        retrans = retrans.group(0)
        retrans = re.sub('retrans:\d+\/','',retrans)
    else:
        retrans = '0'
    #sendrate = re.search('send \d+.\d+[A-z]',connection)
    #if sendrate:
    #    sendrate = sendrate.group(0)[5:]
    #else:
    #    sendrate = '-1'
    if len(ips) > 1 and len(ports) > 1 and rtt and wscaleavg and cwnd and retrans:
        ports[0] = ports[0][2:]
        ports[1] = ports[1][2:]
        return ips, ports, rtt, wscaleavg, cwnd, retrans
    else:
        raise ValueError('Not enough values to search.')

def dbinsert(cur, sourceip, destip, sourceport, destport, rtt, wscaleavg, cwnd, retrans, iface, intervals, flownum):
    '''assembles a query and creates a corresponding row in the database'''
    query = '''INSERT INTO conns (
        sourceip,
        destip,
        sourceport,
        destport,
        flownum,
        iface,
        rttavg,
        wscaleavg,
        cwnd,
        retrans,
        intervals,
        created,
        modified)
    VALUES(
            \'{sip}\',
            \'{dip}\',
            {spo},
            {dpo},
            {fnm},
            \'{ifa}\',
            {rt},
            {wsc},
            {cnd},
            {retr},
            {intv},
            datetime(CURRENT_TIMESTAMP),
            datetime(CURRENT_TIMESTAMP))'''.format(
            sip=sourceip,
            dip=destip,
            spo=sourceport,
            dpo=destport,
            fnm=flownum,
            ifa=iface,
            rt=rtt,
            wsc=wscaleavg,
            cnd=cwnd,
            retr=retrans,
            intv=intervals)
    try:
        cur.execute(query)
    except Exception as e:
        raise DBError(e,'unable to insert {} into database'.format(query))
    return

def dbupdateconn(cur, sourceip, destip, sourceport, destport, rtt, wscaleavg, cwnd, retrans, iface, intervals, flownum):
    '''assembles a query and updates the corresponding row in the database'''
    query = '''UPDATE conns SET
    iface = \'{ifa}\',
    rttavg = {rt},
    wscaleavg = {wsc},
    cwnd = {cnd},
    retrans = {retr},
    intervals = {intv},
    modified = datetime(CURRENT_TIMESTAMP)
    WHERE
    sourceip = \'{sip}\' AND
    destip = \'{dip}\' AND
    sourceport = {spo} AND
    destport = {dpo} AND
    flownum = {fnm}'''.format(
        sip=sourceip,
        dip=destip,
        spo=sourceport,
        dpo=destport,
        fnm=flownum,
        ifa=iface,
        rt=rtt,
        wsc=wscaleavg,
        cnd=cwnd,
        retr=retrans,
        intv=intervals)
    try:
        cur.execute(query)
    except Exception as e:
        raise DBError(e,'could not update connection:{}'.format(query))
    return

def dbselectval(cur, sourceip, destip, sourceport, destport, selectfield):
    '''returns the \'latest\' particular value from the database'''
    query = '''SELECT {sval} FROM conns WHERE
    sourceip = \'{sip}\' AND
    destip = \'{dip}\' AND
    sourceport = {spo} AND
    destport = {dpo} ORDER BY flownum DESC LIMIT 1'''.format(
        sval=selectfield,
        sip=sourceip,
        dip=destip,
        spo=sourceport,
        dpo=destport)
    try:
        cur.execute(query)
        out = cur.fetchall()
    except Exception as e:
        raise DBError(e,'could not execute query {}'.format(query))
    if len(out)>0:
        return out[0][0]
    return -1

def dbupdateval(cur, sourceip, destip, sourceport, destport, updatefield, updateval):
    '''updates a particular value in the database'''
    if type(updateval) == str:
        updateval = '\''+updateval+'\''
    query = '''UPDATE conns SET {ufield}={uval} WHERE
        sourceip=\'{sip}\' AND
        sourceport={spo} AND
        destip=\'{dip}\' AND
        destport={dpo}'''.format(
            ufield=updatefield,
            uval=updateval,
            sip=str(sourceip),
            spo=sourceport,
            dip=str(destip),
            dpo=destport)
    try:
        cur.execute(query)
    except Exception as e:
        raise DBError(e,'could not execute query {}'.format(query))
    return

def dbinit():
    '''initializes the database and creates the table, if one doesn't exist already'''
    conn = sqlite3.connect('connections.db')
    c = conn.cursor()
    try:
        c.execute('''SELECT * FROM conns''')
    except sqlite3.OperationalError:
        print 'Table doesn\'t exist; Creating table...'
        c.execute('''CREATE TABLE conns (
            sourceip    text    NOT NULL,
            destip      text    NOT NULL,
            sourceport  int     NOT NULL,
            destport    int     NOT NULL,
            flownum     int     NOT NULL,
            iface       text,
            rttavg      real,
            wscaleavg   real,
            cwnd        int,
            retrans     int,
            intervals   int,
            created     datetime,
            modified    datetime,
            PRIMARY KEY (sourceip, sourceport, destip, destport, flownum));''')
    conn.commit()
    conn.close()

def throttleoutgoing(iface,ipaddr,speedclass):
    '''throttles an outgoing flow'''
    try:
        success = subprocess.check_call(['tc','filter','add',iface,'parent','1:','protocol','ip','prio','1','u32','match','ip','dst',ipaddr+'/32','flowid',speedclass[1]])
    except subprocess.CalledProcessError as e:
        raise TCError(e,)
    return success

def pollss():
    '''gets data from ss'''
    out = subprocess.check_output(['ss','-i','-t','-n'])
    out = re.sub('\A.+\n','',out)
    out = re.sub('\n\t','',out)
    out = out.splitlines()
    return out

def polltcp():
    '''gets data from /proc/net/tcp'''
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
    '''parses the data from /proc/net/tcp'''
    conn = sqlite3.connect('connections.db')
    c = conn.cursor()
    for connection in connections:
        connection = connection.strip()
        connection = connection.split()
        if connection[1] != '00000000:0000' and connection[2] != '00000000:0000':
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
            tempretr = int(dbselectval(c,sourceip,destip,sourceport,destport,'retrans'))
            retrans += tempretr
            dbupdateval(c,sourceip, destip, sourceport, destport, 'retrans',retrans)
    conn.commit()
    conn.close()

def doconns(interval):
    '''manages the periodic collection of ss and procfs data'''
    connections = pollss()
    loadconnections(connections)
    tcpconns = polltcp()
    parsetcp(tcpconns)
    threading.Timer(interval, doconns, [interval]).start()
    return

def checkfile(parser,fname):
    if not os.path.exists(fname):
        parser.error('File {} doesn\'t exist'.format(fname))
    else:
        return open(fname,'rw')

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)
    program_name = os.path.basename(sys.argv[0])
    program_version = 'v{}'.format(__version__)
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s {v} ({b})'.format(v=program_version, b=program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split('\n')[1]
    program_license = '''{}


USAGE
'''.format(program_shortdesc)

    try:
        # Setup argument parser
        parser = argparse.ArgumentParser(description=program_license, formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('interval',
            metavar='interval',
            type=int,
            action='store',
            help='Specify the monitoring interval in seconds. (min: 1, max: 60)')
        parser.add_argument('-f','--filename',
            dest='filename',
            metavar='filename',
            action='store',
            help='Specify the filename/location of your database.')
        parser.add_argument('-j','--json',
            action='store_true',
            help='use json rather than the default SQLite database.')
        parser.add_argument('-i','--interface',
            dest='interface',
            metavar='interface',
            action='store',
            help='Specify the name of the interface you wish to monitor/throttle.')
        parser.add_argument('-t','--timeout',
            dest='timeout',
            metavar='timeout',
            type=int,
            action='store',
            help='''Specify the amount of time that will pass in seconds before connections
            with the same ports and destination are considered new.''')
        parser.add_argument('-a','--affinitize',
            action='store_true',
            help='Affinitize and optimize the system.')
        parser.add_argument('-r','--throttle',
            action='store_true',
            help='Actively shape connections, rather than just collecting data.')
        parser.add_argument('-v','--verbose',
            action='store_true',
            help='Don\'t summarize connections in the database.')
        # Process arguments
        args = parser.parse_args()
    except KeyboardInterrupt:
        print 'Operation Cancelled\n'
        return 0
    except Exception, e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * ' '
        sys.stderr.write(program_name + ': ' + repr(e) + '\n')
        sys.stderr.write(indent + '  for help use --help'+'\n')
        return 2
    if not 0<args.interval<=60:
        raise CLIError(Exception,'minimum interval is 1 second, maximum is 60 seconds')
    interval = args.interval
    if args.throttle and not args.interface:
        raise CLIError(Exception,'Please supply an interface to throttle with the -i option.')

    if args.throttle:
        throttle = True
    if args.interface:
        interface = args.interface
    if args.filename:
        filename = args.filename
    if args.json:
        json = True
    if args.timeout:
        timeout = args.timeout
    if args.affinitize:
        affinitize = True
    if args.verbose:
        verbose = True
    dbinit()
    if args.affinitize:
        checkibalance()
        numcpus = pollcpu()
        print 'The number of cpus is:', numcpus
        irqlist = pollirq(interface)
        affinity = pollaffinity(irqlist)
        print affinity
        setaffinity(affinity,numcpus)
    if args.interface:
        linerate = getlinerate(args.interface)
    if args.throttle:
        setthrottles(interface)
    doconns(args.interval)

if __name__ == '__main__':
    if DEBUG:
        sys.argv.append('-h')
    if TESTRUN:
        import doctest
        doctest.testmod()
    sys.exit(main())
