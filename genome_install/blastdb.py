#!/usr/bin/python

'''
API for BLAST database compling
'''

import urllib2, zlib, sys, socket
from contextlib import closing
from subprocess import Popen, PIPE
from functools import partial
from select import select
from platform import system


class MakeBlastDB(object):
    '''
    Compile BLAST databases from stream input.
    '''

    def __init__(self, input, **kwargs):
        
        # set program name given the platform
        self.executable = "makeblastdb"
        if system() == "Windows": self.executable += ".exe"
        
        # set defaults
        self.set_defaults()
        
        # set input
        self.input = input
        
        # handle options
        self.set_options(**kwargs)
    
    def set_options(self, **kwargs):
        '''
        Read and set parameter values.
        '''
        
        for k in kwargs:
        
            # makeblastdb options
            if k == "input":
                self._in = kwargs[k]
            elif k == "out":
                self.out = kwargs[k]
            elif k == "title":
                self.title = kwargs[k]
            elif k == "dbtype":
                self.dbtype = kwargs[k]
            elif k == "parse_seqids":
                self.parse_seqids = kwargs[k]
                
            # stream option
            elif k == "decompress":
                self.decompress = kwargs[k]
            elif k == "chunk_size":
                self.chunk_size = kwargs[k]
                
            # unknown option
            else:
                raise ValueError("unhandeled option: {}".format(k))   
    
    def set_defaults(self):
        '''
        Set the default parameters for makeblastdb.
        '''
        self._in = "-"
        self.out = None
        self.title = None
        self.dbtype = None
        self.parse_seqids = False
        self.proc = None
        self.decompress = False
        self.chunk_size = 16*1024

    def get_command_args(self):
        '''
        Returns a list with the command line's arguments
        '''
        
        args = [self.executable, "-in", self._in]
        if self.out is not None: args.extend(["-out", self.out])
        if self.title is not None: args.extend(["-title", self.title])  
        if self.dbtype is not None: args.extend(["-dbtype", self.dbtype])
        if self.parse_seqids: args.append("-parse_seqids")
        return args
    
    def open_process(self):
        '''
        Open a new process using subprocess.Popen. The process stdin is
        PIPE, whereas stdout and stderr are left as default.
        '''
        
        self.proc = Popen(self.get_command_args(), stdin=PIPE)
        
    def communicate(self):
        '''
        Read data from stream, close the process and return returncode.
        A decompress argument can be set True in case the input is 
        compressed. Note that you have to close the stream outside this
        function.
        '''
        
        # set up the read method and read the first chunk
        if self.decompress:
            _read = partial(StreamGunZip(self.input).decompress,
                                 self.chunk_size)
        else:
            _read = partial(self.input.read, self.chunk_size)
        data = _read()
        
        # stop if the input does not provide any data
        if not data:
            sys.stderr.write("MakeBlastDB: null input\n")
            self.proc.stdin.close()
            return self.proc.wait()
        
        # write to stdin whenever it is ready
        while data:
            rlist, wlist, xlist = select([], [self.proc.stdin], [])
            if self.proc.stdin in wlist:
                self.proc.stdin.write(data)
                
                # flush the buffers
                self.proc.stdin.flush()
                sys.stdout.flush()
                sys.stderr.flush()
                try:
                    data = _read()
                except StopIteration:
                    break
                    
        # finally, close and return process' returncode
        self.proc.stdin.close()
        return self.proc.wait()
    
    def make(self, **kwargs):
        '''
        Compile BLAST database.
        '''
            
        # override options
        self.set_options(**kwargs)
        
        # open process
        self.open_process()
        
        # run, return the returncode
        return self.communicate()
        
class StreamGunZip(object):
    '''
    A stream that decompress a open file.
    '''
    
    def __init__(self, compressed_f, chunk_size=1024):
        '''
        Init with a readable stream object. You can also define a
        chunk_size for the next method.
        '''
        
        self.d = zlib.decompressobj(zlib.MAX_WBITS|32).decompress
        self.stream = compressed_f
        self.chunk_size = chunk_size 
    
    def __iter__(self):
        return self
    
    def decompress(self, chunk_size=None):
        '''
        Decompress the following chunk of data from the stream, or all 
        the remaining data at once.
        '''
        
        if chunk_size is None:
            return self.d(self.stream.read())
        return self.d(self.stream.read(chunk_size))
    
    def next(self):
        '''
        Decompress the following chunk of data from the stream.
        '''
        
        data = self.decompress(self.chunk_size)
        if not data:
            raise StopIteration
        return data
        
def make(input, **kwargs):
    '''
    Call makeblastdb as a subprocess, return the returncode.
    
    make(input, db=None, dbtype='nucl', parse_seqids=False)
    '''
    
    # set up the BLAST database compiler
    compiler = MakeBlastDB(input, **kwargs)
    
    # make the BLAST database and return the returncode
    return compiler.make()
    
def download_and_make(url, **kwargs):
    '''
    Download sequences and compile a BLAST database with makeblastdb.
    
    download_and_make(url, db=None, dbtype='nucl', parse_seqids=True)
    '''    
    
    stream = urllib2.urlopen(url)
    stream.fp.fp._sock.shutdown(socket.SHUT_WR) # allow to close urlopen
    r = make(stream, **kwargs)    
    stream.close()
    
    # raise an error if the returncode > 0
    if r > 0:
        sys.stderr.write("makeblastdb exited with error [{}]\n".format(r))
    return r
