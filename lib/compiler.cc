#include "compiler.hh"
#include "string.hh"
#include <execinfo.h>
#include <iostream>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

void print_stacktrace() {
    String pid(getpid());
    char program[1024];
    ssize_t r = readlink("/proc/self/exe", program, sizeof(program));
    if(r > 0){
      program[r] = 0;
      int child_pid = fork();
      if (!child_pid) {
        dup2(2, 1);
        execlp("gdb", "gdb", program, pid.c_str(), "--batch", "-n",
               "-ex", "bt", NULL);
        abort();
      } else
        waitpid(child_pid, NULL, 0);
    }
}

void
fail_mandatory_assert(const char *file, int line, const char *assertion, const char *message)
{
    if (message)
	fprintf(stderr, "assertion \"%s\" [%s] failed: file \"%s\", line %d\n",
		message, assertion, file, line);
    else
	fprintf(stderr, "assertion \"%s\" failed: file \"%s\", line %d\n",
		assertion, file, line);
    print_stacktrace();
    abort();
}
