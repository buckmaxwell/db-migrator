import psycopg2
import sqlite3

pgconn = psycopg2.connect('postgresql://%s:%s@atena-db-dev.clfg28nzaugf.us-east-1.rds.amazonaws.com:5432/ais_srm_dev' % ('aetnaDeveloper', 'Safety2015Risk*'))
slconn = sqlite3.connect('application_database.db')

pcur = pgconn.cursor()
scur = slconn.cursor()


#parts
scur.execute('SELECT * FROM parts')
for row in scur.fetchall():
	print row
#subarts
#standards
#violations
#violations2
#violations3
#violations4
