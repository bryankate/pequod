# Pequod #

This is the source release for Pequod, a fast, distributed key-value cache
with builtin support for materialized views. Pequod is a research prototype
and should not be used in any production environment. For background on 
Pequod's design, see the publication:

[Easy Freshness with Pequod Cache Joins](https://www.usenix.org/system/files/conference/nsdi14/nsdi14-paper-kate.pdf)


## Contents ##

* `PQDIR`            This directory.
* `PQDIR/src`        Pequod source.
* `PQDIR/lib`        Supporting source.
* `PQDIR/app`        Experiment application source.
* `PQDIR/scripts`    Testing scripts.
* `PQDIR/vis`        Debugging visualization.


## Building ##

Pequod builds on Linux and Mac OSX. To build:

    $ ./bootstrap.sh
    $ ./configure
    $ make

Pequod requires a C++11 compatible compiler, and the Apple-supplied compiler might
not be suitable for building on OSX. To use an alternate compiler (such as one 
installed with `homebrew`), specify the `CXX` variable at configuration time:

    $ ./configure CXX='g++-4.8 -std=gnu++11'

For better deubgging with `gdb`, disable compiler optimizations:

    $ make NO_OPT=1

For performance measurements, you should disable debugging features:

    $ ./configure --disable-tamer-debug
    $ make NDEBUG=1

Pequod looks for alternate malloc libraries (jemalloc and tcmalloc) and will 
use one if found. 

Integration with other tools, like Postgres, memcached and Redis, is determined 
automatically at configuration time depending on the presence of client 
libraries on the host system.

See `./configure --help` for more configure options.


## Testing ##

After compilation, test the build by running unit tests:

    $ ./obj/pqserver --tests

To run an individual test, name it explicitly:

    $ ./obj/pqserver test_simple

Running an application in a single process (such as twitternew):

    $ ./obj/pqserver --twitternew

will produce JSON output similar to:

<pre>
{
  "log": {
    "mem_max_rss_mb": [
      [6,129]
    ],
    "utime_us": [
      [6,1018973]
    ],
    "stime_us": [
      [6,53985]
    ],
    "cpu_pct": [
      [6,0]
    ]
  },
  "server_logs": null,
  "server_stats": [
    {
      "store_size": 720901,
      "source_ranges_size": 8484,
      "join_ranges_size": 1,
      "valid_ranges_size": 3483,
      "server_max_rss_mb": 155,
      "server_user_time": 0.539933,
      "server_system_time": 0.011884,
      "server_wall_time": 0.551801,
      "server_wall_time_insert": 0.132986,
      "server_wall_time_validate": 0.107239999999,
      "server_wall_time_evict": 0,
      "server_wall_time_other": 0.311575000002,
      "tables": [
        {"name":"p","ninsert":1230,"store_size":1230,"source_ranges_size":5001,"nvalidate":1800,"nsubtables":5000},
        {"name":"s","ninsert":563575,"store_size":563305,"source_ranges_size":3483,"nvalidate":3483,"nsubtables":5000},
        {"name":"t","nmodify":156892,"nmodify_nohint":3483,"store_size":156366,"sink_ranges_size":3483,"nsubtables":3483}
      ]
    }
  ],
  "nposts": 1230,
  "nbackposts": 0,
  "nsubscribes": 13173,
  "nchecks": 79075,
  "nfull": 9766,
  "nposts_read": 145572,
  "nactive": 3483,
  "nlogouts": 6522,
  "user_time": 0.524327,
  "system_time": 0.011857,
  "wall_time": 0.536175
}
</pre>


## Network testing ##

Pequod can be started as a server, listening on a port for clients:

    $ ./obj/pqserver -kl=7777 &

and to connect a client to a server and run an application:

    $ ./obj/pqserver -c=7777 --twitternew

which will produce JSON results similar to the inline example above.

Helper scripts can execute larger deployments on a multiprocessor and on 
Amazon EC2. They are in the `scripts` directory. For example, the command:

    $ ./scripts/local.py -c 3 basic

will run a short Twitter benchmark using 3 Pequod cache servers and 1 client. 
The results of the experiment will be stored in `PQDIR/results` in a unique 
directory and linked to `PQDIR/last`. 

The scripts take as input the experiment definitions (see `PQDIR/scripts/exp`) 
using the `-e` parameter. The number of cache servers to run is given by `-c`, 
and if a two-tier deployment is desired, `-b` will designate the number of base 
servers. The number of clients used to execute the test workload is given with `-g`. 

The JSON output is directed to `output_app_0.json` if one client is used, and 
`aggregate_output_app.json` if more than one client is used. A log file 
(prefixed with `fart_`) is generated for each Pequod server and client in the 
experiment.

The above command should print something like:

<pre>
Running experiment in test 'basic'.
./obj/pqserver -H=results/exp_2014_04_07-19_24_12/hosts.txt -B=1 -P=twitternew-text -kl=7000
./obj/pqserver -H=results/exp_2014_04_07-19_24_12/hosts.txt -B=1 -P=twitternew-text -kl=7001
./obj/pqserver -H=results/exp_2014_04_07-19_24_12/hosts.txt -B=1 -P=twitternew-text -kl=7002
./obj/pqserver -H=results/exp_2014_04_07-19_24_12/hosts.txt -B=1 -P=twitternew-text -kl=7003
Initializing cache servers.
./obj/pqserver --twitternew --verbose --no-binary --initialize --no-populate --no-execute -H=results/exp_2014_04_07-19_24_12/hosts.txt -B=1
Populating backend.
./obj/pqserver --twitternew --verbose --no-binary --no-initialize --no-execute --popduration=0 --nusers=1000 -H=results/exp_2014_04_07-19_24_12/hosts.txt -B=1
Starting app clients.
./obj/pqserver --twitternew --verbose --no-binary --no-initialize --no-populate --nusers=1000 --duration=100000 --fetch -H=results/exp_2014_04_07-19_24_12/hosts.txt -B=1
Done experiment. Results are stored at results/exp_2014_04_07-19_24_12/basic/
</pre>

For other script options, refer to the python code.


## 3rd Party Benchmarks ##

The 3rd party benchmark tool `memtier_benchmark` is included as a submodule. It
is modified to include Pequod as a protocol option. It can be built after 
Pequod is built (it relies on Pequod files to be generated first):

    $ make memtier

The benchmark can be executed manually against a running Pequod server, but it
is probably easier to use the `PQDIR/scripts/memtier.py` script. See the Python code 
for details.


## Support ##

This is research code, and you use it at your own risk. Requests for support by email may be ignored.


## License ##

Pequod is released under the BSD license. See the `PQDIR/LICENSE` file for details.