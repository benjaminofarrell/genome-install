#!/usr/bin/python

'''
Fetch latest genome assembly URLs for a given query using NCBI's E-direct
'''

import sys, os
from subprocess import Popen, PIPE
from StringIO import StringIO

class AssemblyReference(object):
    '''
    Store information about an assembly.
    '''
    
    def __init__(self, species, assembly_ftppath):
        self.species = species
        self.name = os.path.split(assembly_ftppath)[1]
        self.filename = self.name + "_genomic.fna.gz"
        self.filepath = os.path.join(assembly_ftppath, self.filename)
        self.version = self.name.split("_", 1)[1]
        self.type = "GenBank" if self.name.startswith("GCA") else "RefSeq"
        
    def __repr__(self):
        return "AssemblyReference: {} ({})".format(self.name, self.species)

class AssemblyRepository(dict):
    '''
    Store a collection of AssemblyReference's instances; initialize on 
    the output of an E-direct search.
    '''
    
    def __init__(self, f):
        for line in f:
            line = line.strip().split("\t")
            species = line[0]
            for assembly_ftppath in line[1:]:
                a = AssemblyReference(species, assembly_ftppath)
                try:
                    if a.filepath not in [ x.filepath for x in self[species] ]:
                        self[species].append(a)
                except KeyError:
                    self[species] = [a]
    
def get_genome_fname(urldir):
    fname = os.path.split(urldir)[-1]

def get_latest_assemblies(query, quiet=False):
    '''
    Return an assembly repository from an E-direct query.
    '''
    
    # esearch
    esearch = Popen(args=["esearch", "-db", "assembly", "-query", query],
        stdout=PIPE)
    
    # efetch
    efetch = Popen(args=["efetch", "-format", "docsum"],
        stdin=esearch.stdout,
        stdout=PIPE)
    esearch.stdout.close()
    
    # xtract
    xtract = Popen(args=["xtract", "-pattern", "DocumentSummary", 
                         "-element", "SpeciesName", "FtpPath_RefSeq",
                         "FtpPath_GenBank"],
        stdin=efetch.stdout,
        stdout=PIPE)
    efetch.stdout.close()
    
    # retrieve results
    o, e = xtract.communicate()
    
    # get a list of retrieved species names
    all_species = { line.split("\t")[0].strip() for line in StringIO(o) }
    
    # references all assemblies
    rep = AssemblyRepository(StringIO(o))
    
    # inform of the species with no URLs
    if not quiet:
        non_referenced_urls = sorted(all_species - set(rep.keys()))
        if non_referenced_urls:
            sys.stderr.write(
                "Assembly referenced but URL missing for the following" +
                " species:\n")
            for species in non_referenced_urls:
                sys.stderr.write("  - '{}'\n".format(species))
        
    # set up the output and inform of the species with more than one URLs
    return rep

