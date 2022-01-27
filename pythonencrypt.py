import psycopg2

def dbConnection():
    host = "backup-server-restore.postgres.database.azure.com"
    dbname = "USBCrm"
    user = "usb@backup-server-restore"
    password = "postgres220-"
    sslmode = "prefer"
    database_uri ="host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    connection = psycopg2.connect(database_uri)

    return connection


con = dbConnection()
cursor = con.cursor()

sql = 'select service_number from usb.payments limit 1;'
res = cursor.execute(sql)
print(res.fetchone())