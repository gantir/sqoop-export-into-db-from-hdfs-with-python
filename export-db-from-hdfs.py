#Import HDFS Connexion client
from hdfs import InsecureClient
import posixpath as psp
import argparse
import pprint
import re


#APP options and variable

pp = pprint.PrettyPrinter(indent=4)
regex = r"(\/[0-9\/]+)"
parser = argparse.ArgumentParser()
PORT=50070
HOST='namenode'
CONNECTION_STRING='http://{}:{}'.format(HOST,PORT)
VERBOSE=False
tables = set()
#Get command line arg
def arg_parser():
	
#	parser.add_argument('-r','--run',help='run app with option Example : python script_name endpoint ')
	parser.add_argument('endpoint',help='Endpoint necessary for run program')
	parser.add_argument('--verbose', help='Show command output',action='store_true')
	args = parser.parse_args()
	if args.verbose:
		VERBOSE = True
	return args
#Define connection
def connection():
	return InsecureClient(CONNECTION_STRING)

def show_folder_into_endpoint_root():
	
	return [psp.join(dpath, fname) for dpath, _, fnames in client.walk(args.endpoint) for fname in fnames]
	pp.pprint(fpaths)

def get_table_name(fpaths):
	for path in fpaths:
		last = re.split(regex,path)[-1]
		table_name = last.split('/')[0]
		tables.add(table_name)
		

if __name__ == '__main__':
	args   = arg_parser()
	client = connection()
	fpaths = show_folder_into_endpoint_root()
	get_table_name(fpaths)
	pp.pprint(tables)






