import os
import sys
import subprocess
import socket
import time
import smtplib
import string

SMTP_HOST = "x.x.x.x"
process_name = "org.apache.cassandra.service.CassandraDaemon"
pid_file = "/opt/app/logs/cassandra/cassandra.pid"
CHECK_INTERVAL = 120

def get_hostIP():
    return socket.gethostbyname(socket.gethostname())


def check_port():
    host_ip = get_hostIP()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = s.connect_ex((host_ip, 9042))
    if result == 0:
        # print "Port 9042 is open"
        s.close()
        return "UP"
    else:
        # print "Port 9042 is down"
        return "DOWN"


def get_pid():
    """ Gets the PID of Cassandra. """
    if (os.path.exists(pid_file)):
        ps = subprocess.Popen("cat /opt/app/logs/cassandra/cassandra.pid", shell=True, stdout=subprocess.PIPE)
        pid = ps.stdout.read()
        return pid
    else:
        return -1


def check_pidfile():
    # Check for the existence pid file
    # print "1) Check 1 of 3: Checking PID file"
    if (os.path.exists(pid_file)):
        ps = subprocess.Popen("cat /opt/app/logs/cassandra/cassandra.pid", shell=True, stdout=subprocess.PIPE)
        pid = ps.stdout.read()
        # print "Cassandra PID:", pid
        return "UP"
    else:
        # print "Process seems to be DOWN"
        return "DOWN"


def check_proc(pid):
    # print "2) Check 2 of 3: Checking /proc file"

    if (os.path.exists("/proc/" + str(pid)) == True):
        # print "Cassandra Process is UP"
        return "UP"
    else:
        # print "Process is DOWN"
        return "DOWN"


def findCassandraProcess():
    # print "3) Check 3 of 3: Checking Java Process"
    ps = subprocess.Popen("ps -ef | grep java | grep " + process_name + "| grep -v 'grep "  +process_name + "'", shell=True, stdout=subprocess.PIPE)
    #print 'process_name:', process_name
    output = ps.stdout.read()
    ps.stdout.close()
    ps.wait()
    y = output.split()

    #print "Y = ",y
    if "org.apache.cassandra.service.CassandraDaemon" in y and "-Djvmid=cassandraNode" in y:
        # print "Cassandra is RUNNING"
        # print "C* java process:  UP"
        return "UP"
    else:
        # print "C* java process:  DOWN"
        return "DOWN"



def isCassandraUp():
    port_status = check_port()
    #print "Cassadra Port 9042: ", port_status


    pidfile_status = check_pidfile()
    #print "cassandra.pid file status: ", pidfile_status

    pid = get_pid()
    proc_status = check_proc(pid)
    #print "cassandra /proc file status: ", proc_status

    cassandra_java_process = findCassandraProcess()
    #print "C* java process: ", cassandra_java_process

    if pidfile_status == "UP" and cassandra_java_process == "UP" and proc_status == "UP" and port_status == "UP":
        return "RUNNING"
    else:
        # print "Cassandra is DOWN"
        return "DOWN"


def checkWriteAccess():
    # Check all the data directories that cassandra uses
    for i in range(1, 3):
        # print i
        # file_name="/data" + str(i) + "/data/" + str(i) + ".txt"
        print "Checking read/write access to file system"
        file_name = "/opt/app/dse-data/data/" + str(i) + ".txt"
        #print file_name
        try:
            f = open(file_name, 'w+')
            f.write(str(i))
            f.read()
            f.close()
            return "YES"
        except IOError:
            print "Could not read or write to file:", file_name
            print "Potential I/O Error: Please check"
            return "NO"



def startCassandra():
    # Check all the ways to ensure Cassandra is UP/DOWN
    cassandra_status = isCassandraUp()
    storage_status = checkWriteAccess()

    # Check if storage is good
    print "Is Storage good? ", storage_status

    MAX_START_RETRIES = 3
    i = 1

    if cassandra_status == "DOWN" and storage_status == "YES":
        while (i <= MAX_START_RETRIES):
            ## Start Cassandra ##
            print "Attempting to start Cassandra", str(i) + " of", str(MAX_START_RETRIES) + " times"
            subprocess.call(['/opt/app/cassandra/scripts/startCassandra.sh'])
            # SLEEP_TIME=180
            SLEEP_TIME = 30
            time.sleep(SLEEP_TIME)

            if i == MAX_START_RETRIES and isCassandraUp() == "DOWN":
                print "Exhausted MAX_START_RETRIES! Giving up"
                print "Sending Email Notification ...."
                hostname = socket.gethostname()
                SUBJECT = "Cassandra auto failed on " + str(hostname)
                text = "Cassandra auto restart failed on " + str(
                    hostname) + " due to MAX_START_RETRIES exhaustion. Please investigate!"
                sendEmailNotification(hostname, SUBJECT, text)
                sys.exit()

            if isCassandraUp() == "DOWN":
                i = i + 1
                continue
            else:
                print "Cassandra is started"
                hostname = socket.gethostname()
                SUBJECT = "Cassandra auto restarted on " + str(hostname)
                text = "Cassandra on " + str(hostname) + " was auto restarted when it was detected being DOWN."
                sendEmailNotification(hostname, SUBJECT, text)
                sys.exit()

    else:
        pass


def sendEmailNotification(host_name, SUBJECT, text):

    TO_EMAILS = ["id@email.com"]
    FROM = "id@email.com"
    BODY = string.join((
        "From: %s" % FROM,
        "To: %s" % ', '.join(TO_EMAILS),
        "Subject: %s" % SUBJECT,
        "",
        text
    ), "\r\n")
    server = smtplib.SMTP(SMTP_HOST)
    server.sendmail(FROM, TO_EMAILS, BODY)
    server.quit()

def main():
    while (True):
        cassandra_status = isCassandraUp()
        if cassandra_status == "RUNNING":
            print "Cassandra is UP!!!"
            print "No action required"
            print "Sleeping for", str(CHECK_INTERVAL) + " seconds"
            time.sleep(CHECK_INTERVAL)
            continue
        else:
            print "Cassandra is DOWN"
            print "Attempting to restart Cassandra"
            ### Try to restart Cassandra
            startCassandra()
            break


if __name__ == "__main__":
    main()


