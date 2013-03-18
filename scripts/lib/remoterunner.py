import config
from process import Process
import sys, re, os

class RemoteRunner:
    def __init__(self, addr, cid, cmd = None, resultDir = None, ncore = 1):
        self.addr = addr
        self.cmd = cmd
        self.cid = cid
        self.proc = Process(self.addr)
        self.resultDir = resultDir
        self.ncore = ncore

    def poll(self):
        if hasattr(self.proc, "p"):
            return self.proc.poll()
        return 0

    def returncode(self):
        if hasattr(self.proc, "p"):
            return self.proc.returncode()
        return 0

    def err(self):
        return self.proc.err()

    def out(self):
        return self.proc.out()

    def setenv(self):
        self.proc.sync_rexec('''
                sudo sysctl net.core.wmem_max=1048576;
                sudo sysctl net.core.rmem_max=1048576;
                sudo sysctl net.core.wmem_default=1048576;
                sudo sysctl net.core.rmem_default=1048576;
               ''');

    def _async_rexec_script(self, cmd):
        self.proc.scp("./exp/data/gstore_commands.sh", "~/gstore_commands.sh")
        stderr = os.path.join(self.resultDir, self.addr + "_" + str(self.cid) + "_stderr")
        stdout = os.path.join(self.resultDir, self.addr + "_" + str(self.cid) + "_stdout")
        print "Issuing the following command to %s:\n  $ %s" % (self.addr, cmd)
        self.proc.async_rexec(cmd, stderr, stdout)

    def _sync_rexec_script(self, cmd):
        self.proc.scp("./exp/data/gstore_commands.sh", "~/gstore_commands.sh")
        err, out = self.proc.sync_rexec(cmd)
        print err.readlines()
        print out.readlines()

    def send_command(self, cmd):
        self._async_rexec_script("bash gstore_commands.sh %s" % cmd)

    def _run(self, wd, cmd = None, stderr = None, stdout = None, sync = False):
        if not wd:
            raise Exception('need a working directory')
        if not cmd:
            cmd = self.cmd
        if sync:
            self.proc.sync_rexec("cd %s; %s " % (wd, cmd))
        else:
            if not stderr:
                stderr = os.path.join(self.resultDir, self.addr + "_" + str(self.cid) + "_stderr")
            if not stdout:
                stdout = os.path.join(self.resultDir, self.addr + "_" + str(self.cid) + "_stdout")
            self.proc.async_rexec("cd %s; %s" % (wd, cmd), stderr, stdout)

    def term(self):
        self.proc.terminate()

