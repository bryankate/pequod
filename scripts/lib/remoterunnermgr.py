import time

def waitAll(runners, output):
    err = False
    try:
        for r in runners:
            print >> output, "waiting for", r.addr,
            while r.poll() == None:
                print >> output, ".",
                output.flush()
                time.sleep(1)
            print
            if r.returncode() != 0:
                err = True
                print >> output, "return code", r.returncode(), "from", 
                print >> output, r.addr, "cid", r.cid, ". See ", r.proc.stdoutFile,
                if r.proc.stderrFile != r.proc.stdoutFile:
                    print >> output, r.proc.stderrFile,
                print >> output
    except KeyboardInterrupt:
        for r in runners:
            if r.returncode() == None:
                r.term()
        err = True
        print >> output, "Interrupted"
    return err
