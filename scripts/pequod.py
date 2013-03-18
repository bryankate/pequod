import os, subprocess

class PequodLoadRunner:
    @staticmethod
    def run(plotgroup, ld, opt, cluster, resultDir):
        cmd = ld['cmd']
        if opt.affinity:
            cmd = ['numactl', '-C', '0'] + cmd
        for i in range(opt.repeat):
            stdout = os.path.join(resultDir, "trail_%d.json" % i)
            print cmd
            p = subprocess.Popen(cmd, stdout = open(stdout, "w"), shell = True)
            p.communicate()
            if p.returncode != 0:
                raise Exception('failed to run %s' % cmd)
