import config, subprocess

class Process:
    def __init__(self, addr):
        self.addr = addr

    @staticmethod
    def lexec(cmd):
        p = subprocess.Popen(cmd, stdin = None, stdout = None, stderr = None, 
                         shell = True)
        p.communicate()
        return p

    @staticmethod
    def checkedRun(cmd):
        print cmd
        p = Process.lexec(cmd)
        if p.returncode != 0:
            raise Exception('failed')

    def _sshCmd(self, cmd):
        return "%s -t -t %s@%s \"%s\"" % (config.ssh_command(), config.USER, self.addr, cmd)

    def sync_rexec(self, cmd, env = None, ignerr = False):
        sshCmd = self._sshCmd(cmd)
        p = subprocess.Popen(sshCmd, stdin = None, stdout = subprocess.PIPE, 
               stderr = subprocess.PIPE, shell = True, env = env)
        p.wait()
        if p.returncode != 0 and not ignerr:
            raise Exception("Failed to execute: %s" % sshCmd)
        return p.stderr, p.stdout

    def async_rexec(self, cmd, stderrFile, stdoutFile, env = None):
        sshCmd = self._sshCmd(cmd)
        self.stderrFile = stderrFile
        self.stdoutFile = stdoutFile
        self.stdout = open(stdoutFile, "w")
        if stderrFile == stdoutFile:
            self.stderr = self.stdout
        else:
            self.stderr = open(stderrFile, "w")
        self.stdout2 = None
        self.stderr2 = None
        self.reopened = False
        print sshCmd
        self.p = subprocess.Popen(sshCmd, stdin = subprocess.PIPE, 
            stdout = self.stdout, stderr = self.stderr, shell = True, env = env)
    
    def _reopenOutput(self):
        if self.reopened:
            return
        self.reopened = True
        self.stdout.close()
        self.stderr.close()
        self.stdout2 = open(self.stdoutFile, "r")
        self.stderr2 = open(self.stdoutFile, "r")

    def poll(self):
        returncode = self.p.poll()
        if returncode != None:
            self._reopenOutput()
        return returncode

    def terminate(self):
        try:
            self.p.terminate()
            print "Waiting for process after terminate"
            self.p.wait()
            self._reopenOutput()
        except:
            pass

    def returncode(self):
        return self.p.returncode

    def err(self):
        self.stderr2.seek(0)
        return self.stderr2

    def out(self):
        self.stderr2.seek(0)
        return self.stdout2

    def scp(self, local, remote):
        self.lexec("%s %s %s@%s:%s" % (config.scp_command(), local, 
                                       config.USER, self.addr, remote))

    def scp_get(self, remote, local):
        self.lexec("%s %s@%s:%s %s" % (config.scp_command(), 
                                       config.USER, self.addr, remote, local))

