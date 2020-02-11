#Import HDFS Connexion client
from hdfs import InsecureClient
import posixpath as psp
import argparse
import pprint
import re
import os

#APP options and variable

pp = pprint.PrettyPrinter(indent=4)
regex = r"(\/[0-9\/]+)"
parser = argparse.ArgumentParser()
PORT=50070
HOST='namenode'
CONNECTION_STRING='http://{}:{}'.format(HOST,PORT)
VERBOSE=False
tables = set()
#contain table and all path
datatables = dict()
#contain unique path to directory
unique_path = set()
#Get command line arg
def arg_parser():
	
#	parser.add_argument('-r','--run',help='run app with option Example : python script_name endpoint ')
	parser.add_argument('endpoint',help='Endpoint necessary for run program')
	parser.add_argument('--datbasename',help='Destination database name')
	parser.add_argument('--hostname',help='Destination database hostname')
	parser.add_argument('--username',help='Destination database username')
	parser.add_argument('--password',help='Destination database password')
	parser.add_argument('--driver',help='Export database driver for connection',default='mysql')
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
		dpaths = list()
		for p in fpaths:
			last = re.split(regex,p)[-1]
			if table_name in last:
				dpaths.append(p)
		if table_name not in datatables:
			datatables.update({table_name: dpaths})
		

def sqoop_export_into_db():
	for table in datatables:
		for t in datatables[table]:
			res = re.split(regex,t)
			p = res[0]+res[1]
			path = os.path.join(p,table)
			unique_path.add(path)
			


if __name__ == '__main__':
	args   = arg_parser()
	client = connection()
	fpaths = show_folder_into_endpoint_root()
	get_table_name(fpaths)
	#pp.pprint(datatables)
	sqoop_export_into_db()
	pp.pprint(unique_path)





