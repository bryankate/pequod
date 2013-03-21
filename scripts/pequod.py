import os, subprocess
from time import sleep

class PequodLoadRunner:
    @staticmethod
    def run(plotgroup, ld, opt, cluster, resultDir):
        cmd = ld['cmd']
        if opt.affinity:
            cmd = ['numactl', '-C', '0'] + cmd
        for i in range(opt.repeat):
            stdout = os.path.join(resultDir, "trail_%d.json" % i)
            if ld.has_key('server'):
                server = subprocess.Popen(ld['server'], shell = True)
                sleep(1)
            else:
                server = None
            print cmd
            p = subprocess.Popen(cmd, stdout = open(stdout, "w"), shell = True)
            p.communicate()
            if p.returncode != 0:
                raise Exception('failed to run %s' % cmd)
            if server:
                server.send_signal(9)
                server = None
