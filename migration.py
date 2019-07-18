#!/usr/bin/env python
import MySQLdb


def init_connection():
    con = MySQLdb.connect(host="109.236.84.201",
                          user="devel",
                          passwd="Aida4tej",
                          db="mp_megapersonals")
    return con


def get_cursor(con):
    cur = con.cursor()
    return cur


def convert_to_single_transaction(con, cur, verification_status_id):

    sql_select_all = """SELECT user_id FROM verification_transactions"""
    sql_get_user_records = """ SELECT * FROM verification_transactions WHERE user_id =  %s order by request_time desc"""
    sql_delete_records_by_id = """DELETE FROM verification_transactions WHERE id =  %s""";
    sql_select_failed_transactions = """SELECT * FROM verification_transactions WHERE user_id =  %s and verification_status_id = 2"""
    sql_update_failed_transaction = """UPDATE verification_transactions SET failed_attempts = %s WHERE id = %s"""

    cur.execute(sql_select_all)
    users = list(dict.fromkeys(cur.fetchall()))

    for user_id in users:
        cur.execute(sql_get_user_records, (user_id,))
        user_records = list(cur.fetchall())

        if user_records[0][12] == verification_status_id:
            print "LAST VERIFICATION_STATUS_ID : " + str(verification_status_id)
            print "USER ID : " + str(user_id)
            # print user_records
            cur.execute(sql_select_failed_transactions, (user_id,))
            count_failed_attempts = len(list(cur.fetchall()))
            if count_failed_attempts > 3:
                count_failed_attempts = 3

            print "Failed attempts : " + str(count_failed_attempts) + " for user : " + str(user_id)
            print "Update last transaction with the ID : " + str(user_records[0][0])
            cur.execute(sql_update_failed_transaction, (count_failed_attempts, user_records[0][0],))
            del user_records[0]
            con.commit()

            for user_record in user_records:
                print "Delete with the ID : " + str(user_record[0])
                cur.execute(sql_delete_records_by_id, (user_record[0],))
                con.commit()


if __name__ == "__main__":
    sql_unique_index_user_id = """CREATE UNIQUE INDEX idx_verification_transaction_user_id ON verification_transactions(user_id)"""

    connection = init_connection()
    cursor = get_cursor(connection)

    statuses_id = [1,2,3,4]
    for status_id in statuses_id:
        convert_to_single_transaction(connection, cursor, status_id)

    cursor.execute(sql_unique_index_user_id)
    connection.commit()
    print "Unique index with the name: 'idx_verification_transaction_user_id' on user_id has been created"

    connection.close()
