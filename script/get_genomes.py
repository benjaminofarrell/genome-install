#!/usr/bin/python

'''
Batch download genomes and compile BLASTDB from NCBI

USAGE
    get_genomes.py [OPTIONS] <NCBI query>
    
OPTIONS
    -f, --filter=FILE
        Skip species referenced in FILE
    -l, --log=FILE
        Write a log of compiled genomes.
'''

import getopt, sys, fileinput, os
from contextlib import contextmanager
from os import path
from urllib2 import URLError
from genome_install.blastdb import download_and_make
from genome_install.edirect import get_latest_assemblies


__doc__ = __doc__.format(path.split(sys.argv[0])[1])

class Options(dict):

    def __init__(self, argv):
        
        # set default
        self.set_default()
        
        # handle options with getopt
        try:
            opts, args = getopt.getopt(argv[1:], "f:l:", 
                ['filter=', 'log=', 'help'])
        except getopt.GetoptError, e:
            sys.stderr.write(str(e) + '\n\n' + __doc__)
            sys.exit(1)

        for o, a in opts:
            if o == '--help':
                sys.stdout.write(__doc__)
                sys.exit(0)
            elif o in ("-f", "--filter"):
                with open(a) as f:
                    self['filter'] = { line.strip() for line in f }                
            elif o in ("-l", "--log"):
                self['log'] = a

        self.args = args
    
    def set_default(self):
    
        # default parameter value
        self['filter'] = set()
        self['log'] = None
       
@contextmanager
def devnull():
    try: yield open(os.devnull, "w")
    finally: pass

def openlog(fname):
    if fname is None: return devnull()
    return open(fname, "w")
    
def main(argv=sys.argv):
    
    # read options and remove options strings from argv (avoid option names and
    # arguments to be handled as file names by fileinput.input().
    options = Options(argv)
    sys.argv[1:] = options.args
    
    # organize the main job...
    for query in options.args:
        repository = get_latest_assemblies(query)
        for species in sorted(repository.keys()):
            
            # skip species in the filter
            if species in options['filter']: continue
            
            # only download GenBank assemblies if no RefSeq is available
            assemblies = repository[species]
            if "RefSeq" in [ x.type for x in assemblies ]:
                assemblies = [ x for x in assemblies 
                               if x.type == "RefSeq" ]
            
            # download assemblies
            for assembly in assemblies:
                dbname = assembly.name.split(".")[0]
                
                # give three chances in case of time out
                counter = 0
                while counter < 3:
                    try:
                        r = download_and_make(
                            # url
                            assembly.filepath, 
                            
                            # makeblastdb options
                            dbtype="nucl",
                            parse_seqids=True,
                            out=dbname,
                            title=dbname,
                            
                            # stream option
                            decompress=True
                            )
                        break
                    except URLError, e:
                        if "[Errno 110]" in str(e):
                            counter += 1
                            continue
                        raise
                if r != 0:
                    sys.stderr.write("makeblastdb exited with an error\n")
                    sys.exit(r)
                sys.stdout.write("{}\t{}\n".format(species, assembly.filepath))
        
    # return 0 if everything succeeded
    return 0

# does not execute main if the script is imported as a module
if __name__ == '__main__': sys.exit(main())

