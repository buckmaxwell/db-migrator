import psycopg2
import sqlite3

pgconn = psycopg2.connect('postgresql://%s:%s@atena-db-dev.clfg28nzaugf.us-east-1.rds.amazonaws.com:5432/ais_srm_dev' % ('aetnaDeveloper', 'Safety2015Risk*'))
slconn = sqlite3.connect('application_database.db')

pcur = pgconn.cursor()
scur = slconn.cursor()

print "BEGINNING MIGRATION"

'''
#parts
scur.execute('SELECT number, title FROM parts')
for row in scur.fetchall():
	pcur.execute('INSERT INTO dbv_parts (number, title, update_number, active) VALUES (%s, %s, %s, %s)', 
		(row[0], row[1], 1, True) )
	pgconn.commit()

print "SUCCESSFULLY INSERTED PARTS"


#subarts
scur.execute('SELECT letter, part_number, title FROM subparts')
for row in scur.fetchall():
	pcur.execute('INSERT INTO dbv_subparts (letter, part_number, title, update_number, active) VALUES (%s, %s, %s, %s, %s)',
		(row[0], row[1], row[2], 1, True))
	pgconn.commit()

print "SUCCESSFULLY INSERTED SUBPARTS"


#standards
scur.execute('SELECT number,title,subpart_letter,part_number FROM standards')
for row in scur.fetchall():
	pcur.execute('INSERT INTO dbv_standards (number, title, subpart_letter, part_number, update_number, active) VALUES (%s, %s, %s, %s, %s, %s)',
		(row[0], row[1], row[2], row[3], 1, True))
	pgconn.commit()

print "SUCCESSFULLY INSERTED STANDARDS"

'''
#violations
scur.execute('SELECT number,description,standard_number FROM violations')
for row in scur.fetchall():
	pcur.execute('INSERT INTO dbv_violations (number, description, standard_number, update_number, active) VALUES (%s, %s, %s, %s, %s)', 
		(row[0], row[1], row[2], 1, True))
	pgconn.commit()

print "SUCCESSFULLY INSERTED VIOLATIONS"

#violations2
scur.execute('SELECT number,description,violation_number FROM violations_level_2')
for row in scur.fetchall():
	try:
		pcur.execute('INSERT INTO dbv_violation_2s (number, description, violation_number, update_number, active) VALUES (%s, %s, %s, %s, %s)',
			(row[0], row[1], row[2], 1, True))
		pgconn.commit()
	except Exception as e:
		print "ERROR1: " + str(e)

print "SUCCESSFULLY INSERTED VIOLATIONS2"


#violations3
scur.execute('SELECT number,description,violations_level_2_number FROM violations_level_3')
for row in scur.fetchall():
	try:
		pcur.execute('INSERT INTO dbv_violation_3s (number, description, violation2_number, update_number, active) VALUES (%s, %s, %s, %s, %s)',
			(row[0], row[1], row[2], 1, True))
		pgconn.commit()
	except Exception as e:
		print "ERROR2: " + str(e)

print "SUCCESSFULLY INSERTED VIOLATIONS3"



#violations4
scur.execute('SELECT number,description,violations_level_3_number FROM violations_level_4')
for row in scur.fetchall():
	pcur.execute('INSERT INTO dbv_violation_4s (number, description, violation3_number, update_number, active) VALUES (%s, %s, %s, %s, %s)',
		(row[0], row[1], row[2], 1, True))
	pgconn.commit()

print "SUCCESSFULLY INSERTED VIOLATIONS4"




















