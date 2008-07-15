To build everything, make sure ant (http://ant.apache.org/) 
is in your PATH, then type:

ant release

To run live tests:

1) set up a running fedora instance with RI enabled
2)  define FEDORA_HOME env variable or system property fedora.home
3) if username is different from 'fedoraAdmin', define system property fedora.username
4) if password is different from 'fedoraAdmin', define system property fedora.password
5) run ant liveTests

It should automatically determine if you're running Mulgara or MPTStore, as well as automatically determine
the correct port to use, whether or not it needs to manually flush triples (if syncUpdates = true in the fedora config, it won't), and any additional configuration required.
