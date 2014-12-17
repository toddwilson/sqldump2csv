import re
import csv
import argparse
import gzip

RE_INSERT_STATEMENT = re.compile("^INSERT INTO [\'`](?P<table>[a-zA-Z_]+)[\'`] VALUES \((?P<valuestr>.*)\);")
RE_SPLIT_CSV = re.compile(r''',(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''')
DELIMITER = '\t'


class Writer(object):

	extensions = {
		',': '.csv',
		'\t': '.tsv'
	}

	def __init__(self, delimiter, compress, path):
		self.files = {}
		self.writers = {}
		self.delimiter = delimiter
		self.compression = compress
		self.path = path

		if self.path != '':
			if self.path[-1] != '/':
				self.path = self.path + '/'

	def open(self, table):
		if self.compression is True:
			self.files[table] = gzip.open("%s%s%s.gz" % (self.path, table, self.extensions.get(self.delimiter, '')), 'w')
		else:
			self.files[table] = open("%s%s%s" % (self.path, table, self.extensions.get(self.delimiter, '')), 'w')
		self.writers[table] = csv.writer(self.files[table], delimiter=self.delimiter, quoting=csv.QUOTE_MINIMAL)

	def write(self, table, data):
		if table not in self.files:
			self.open(table)

		self.writers[table].writerow(data)

	def close(self):
		for f in self.files.values():
			print(f.name)
			f.close()


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Convert MySQL Dump to CSV')
	parser.add_argument('-d', '--delimiter', required=False, help='Delimter to use. Default is tab.')
	parser.add_argument('-c', '--compress', required=False, action='store_true', help='Write output using gzip')
	parser.add_argument('sqlfile', nargs='?', help='File to load')
	parser.add_argument('output', nargs='?', help='Location of output files')

	args = parser.parse_args()

	if args.sqlfile is None:
		exit('Error: No input SQL file specified')

	path = ''
	if args.output is not None:
		path = args.output

	delimiter = DELIMITER
	if args.delimiter is not None:
		delimiter = args.delimiter

	f = open(args.sqlfile, 'r')

	writer = Writer(delimiter, args.compress, path)

	# this is messy and I just need to write a better regular expression
	def strip_quotes(c):
		if c[0] == "'" and c[-1] == "'":
			return c[1:-1]
		else:
			return c

	for line in f.readlines():

		if line.startswith(('--', '/*')):
			continue

		match = RE_INSERT_STATEMENT.match(line)

		if match is None:
			continue

		data = map(strip_quotes, RE_SPLIT_CSV.split(match.group('valuestr')))

		writer.write(match.group('table'), data)


	writer.close()