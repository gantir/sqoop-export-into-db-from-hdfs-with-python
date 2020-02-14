#Import HDFS Connexion client
from hdfs import InsecureClient
import posixpath as psp
import argparse
import pprint
import re
import os
import sys
#APP options and variable
sqoop = "sqoop-export --connect {} --table {} --username {} --password {} --export-dir {} "
pp = pprint.PrettyPrinter(indent=4)
regex = r"(\/[0-9\/]+)"
parser = argparse.ArgumentParser()
PORT=50070
HOST='namenode'
CONNECTION_STRING='http://{}:{}'.format(HOST,PORT)
VERBOSE=False
#Contain all tables and recent path
tb_recent_path = []
tb_last_path = []
last_tble_int = set()
#Contains all tables
tables = set()
#contain table and all path
datatables = dict()
#contain unique path to directory
unique_path = set()

def extract_tmps(target):
	return re.sub(r'\D','',target)


#Get command line arg
def arg_parser():
	
#	parser.add_argument('-r','--run',help='run app with option Example : python script_name endpoint ')
	parser.add_argument('-e','--endpoint',help='Endpoint necessary for run program',required=True)
	parser.add_argument('-d','--databasename',help='Destination database name')
	parser.add_argument('-hn','--hostname',help='Destination database hostname')
	parser.add_argument('-u','--username',help='Destination database username')
	parser.add_argument('-p','--password',help='Destination database password')
	parser.add_argument('-dr','--driver',help='Export database driver for connection',default='mysql')
	parser.add_argument('--verbose', help='Show command output',action='store_true')
	args = parser.parse_args()
	if args.verbose:
		VERBOSE = True
	return args
#Define connection
def connection():
	return InsecureClient(CONNECTION_STRING)

def show_folder_into_endpoint_root(client,args):
	
	return [psp.join(dpath, fname) for dpath, _, fnames in client.walk(args.endpoint) for fname in fnames]

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
		

def sqoop_export_path():
	for table in datatables:
		for t in datatables[table]:
			res = re.split(regex,t)
			p = res[0]+res[1]
			path = os.path.join(p,table)
			unique_path.add(path)
			

def command_builder(args,tbname,export_dir):
	conn_string = ""
	if args.driver == 'mysql':
		conn_string = 'jdbc:mysql://{}/{}'.format(args.hostname,args.databasename)
	elif args.driver == 'postgres':
		conn_string = 'jdbc:postgres://{}/{}'.format(args.hostname,args.databasename)
	else:
		pp.pprint("Driver unkown")
		sys.exit(0)

	
	#sqoop = "sqoo-export --connect {} --table {} --username {} --password {} --export-dir {} "
	if args.username is None :
		pp.pprint("Destination databasename username is required")
		sys.exit(0)
	if args.databasename is None:
		pp.pprint("Destination databasename is required")
		sys.exit(0)
	if args.password is None:
		pp.pprint("Destination databasename password is required")
		sys.exit(0)
	if args.hostname is None :
		pp.pprint("Destination databasename hostname or address is required")
		sys.exit(0)
	cmd = sqoop.format(conn_string,tbname,args.username,args.password,export_dir)
	#os.system(cmd)


def export_into_db(args):
	for p in unique_path:
		tmps = re.sub(r'\D','',p)
		if int(tmps) > max(last_tble_int):
			tbname = re.split(regex,p)[-1]
			command_builder(args,tbname,p)



def writer_last_segment_path_into_file(fpaths):
	have_new_path = False
	new_path = set()
	for path in fpaths:
		tmps = re.sub(r'\D','',path)
		if int(tmps) > max(last_tble_int):
			new_path.add(path + "\r\n")
			have_new_path = True
	
	if have_new_path :
		print("Have new segment")
		f = open('last_segment_path','w')
		for p in new_path:
			f.write(p)
		f.close()


def get_recent_segment():
	for d in datatables:
		data = dict()
		content = []
		for path in datatables[d]:
			rs 	= re.split(regex,path)
			tmstamp = re.sub(r'\D','',rs[1])
			content.append(int(tmstamp))
			data.update({int(tmstamp):path})
			rs = re.split(regex,data[max(content)])
			pf = rs[0] + rs[1] + d
		tb_recent_path.append(pf)
	
def read_last_path():
	data = open('last_segment_path','r')
	for d in data:
		tmps = re.sub(r'\D','',d)
		last_tble_int.add(int(tmps))
		tb_last_path.append(d)		




def main():
	args    	= arg_parser()
	client  	= connection()
	fpaths  	= show_folder_into_endpoint_root(client,args)
	read_last_path()
	get_table_name(fpaths)
	sqoop_export_path()
	get_recent_segment()
	writer_last_segment_path_into_file(tb_recent_path)
	export_into_db(args)
	



if __name__ == '__main__':
	main()













