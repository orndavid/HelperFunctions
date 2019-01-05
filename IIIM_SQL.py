# Filename: IIIM_SQL.py
# Created: Sat Jan  5 07:19:49 2019 (+0000)
# Last-Updated: Sat Jan  5 07:20:42 2019 (+0000)
# Author: David
# Description:
#! /usr/bin/env python3
##############################################################################
#     File Name           :     iiim_sql.py
#     Created By          :     david
#     Email               :     david@iiim.is
#     Creation Date       :     [2018-08-15 13:16]
#     Last Modified       :     [2018-09-04 14:40]
#     Description         :     A custom connector with a sqllite database
#     Version             :     0.1
##############################################################################
import pickle
import numpy as np
import io
import sqlite3


class IIIM_SQL(object):
    """
    A custom sql connections object
    """
    def __init__(self, db_file):
        """
        Initialize a connector with a db_file name defined by user
        """
        self.db = db_file

        # Definition of array storage and tuple storage
        def adapt_array(arr):
            """
            http://stackoverflow.com/a/31312102/190597 (SoulNibbler)
            """
            out = io.BytesIO()
            np.save(out, arr)
            out.seek(0)
            return sqlite3.Binary(out.read())

        def convert_array(text):
            out = io.BytesIO(text)
            out.seek(0)
            return np.load(out)

        def adapt_tuple(tuple):
            return pickle.dumps(tuple)
        sqlite3.register_adapter(tuple, adapt_tuple)
        sqlite3.register_converter("tuple", pickle.loads)

        # Collation definition
        def collate_tuple(st1, st2):
            return cmp(pickle.loads(st1), pickle.loads(st2))

        # TODO : Add functionality to save/load a pickle binary object

        # We can add these types to the connector
        # Convert np.array to TEXT when inserting
        sqlite3.register_adapter(np.ndarray, adapt_array)
        # Convert TEXT to np.array when selecting
        sqlite3.register_converter("array", convert_array)

        # Binary push of blob object
        self.conn = sqlite3.connect(self.db,
                                    detect_types=sqlite3.PARSE_DECLTYPES)

        self.conn.create_collation("cmtuple", collate_tuple)

    def __del__(self):
        """
        Destructor trying to close
        """
        self.conn.close()

    def execute(self, command):
        """
        Execute a command and commit it
        """
        cursor = self.conn.cursor()
        cursor.execute(command)
        self.commit()

    def commit(self):
        """
        Commit changes on db
        """
        self.conn.commit()

    def cursor(self, EMPTY=False, *args):
        """
        Execute a statement on the database
        """
        cursor = self.conn.cursor()
        if not EMPTY:
            cursor = cursor.execute(args[0])
            return cursor
        else:
            return cursor

    def tables(self, show=True):
        """
        List all available tables
        """
        cursor = self.read("""SELECT name FROM sqlite_master
                              WHERE type='table';""")

        tables = cursor.fetchall()
        tabs = []
        for tab in tables:
            if show:
                print("\t{}".format(tab))
            tabs.append(tab[0])
        return tabs

    def columns(self, table, show=True):
        """
        Get a list of the columns in a table
        """
        cols = ""
        eval_text = """SELECT * FROM {}""".format(table)
        cursor = self.read(eval_text)
        columns = cursor.description
        n = len(columns)
        ret_cols = []
        for idx, col in enumerate(columns):
            ret_cols.append(col[0])
            if idx == n-1:
                cols += "{}".format(col[0])
            else:
                cols += "{}, ".format(col[0])
        if show:
            print(cols)
        return ret_cols

    def create_table(self, command):
        """
        Take in a sqllite command to create a table, if one doesn't allready
        exist
        """
        self.execute(command)
        self.commit()

    def drop(self, table_name):
        """
        Drop a table by name
        """
        output = "DROP TABLE IF EXISTS {}".format(table_name)
        self.execute(output)
        self.commit()

    def schema(self, table):
        """
        Try to output the schema for the table
        """
        cols = ""
        eval_text = """PRAGMA table_info('{}');""".format(table)
        cursor = self.execute(eval_text)
        data = cursor.fetchall()
        n = len(data)
        for idx, col in enumerate(data):
            if idx < n - 1:
                cols += col[1] + " : " + col[2] + " | "
            else:
                cols += col[1] + " : " + col[2]
        print(cols)

    def insert(self, table, values):
        val_str = "("
        for idx in range(len(values)):
            if idx == len(values) - 1:
                val_str += "?)"
            else:
                val_str += "?, "

        eval_text = """
                    INSERT INTO {}
                    VALUES {}
                    """.format(table, val_str)

        cursor = self.conn.cursor()
        cursor.execute(eval_text, values)
        self.commit()

    def read(self, search_query):
        """
        Send a written search query to the database, return a cursor
        """
        cursor = self.conn.cursor()
        return cursor.execute(search_query)


def show_db():
    """
    An example of how to list tables and available columns in a table
    """
    obj = IIIM_SQL("iiimSqlTester.db")
    obj.tables()  # Prints the ouput
    print("\n\n")
    tables = obj.tables(show=False)  # Gets a list of the tables without print
    # Note that obj.tables always returns, it's just a question of putting
    # show=False to not print the output again, or at all.
    for table in tables:
        print("\n{}".format(table))
        col_names = obj.columns(table)  # Print all columns for each table
        for col in col_names:
            print("{}".format(col))


def create_db():
    """
    An example of how to create a new table database
    """
    obj = IIIM_SQL("iiimSqlTester.db")
    obj.drop("newTable")
    obj.create_table("""
                     CREATE TABLE IF NOT EXISTS newTable(
                        id integer PRIMARY KEY,
                        name text,
                        value array,
                        second tuple collate cmtuple
                    )""")
    for k in range(100):
        obj.insert("newTable", (k,
                                "This is a test",
                                np.zeros((2, 2)),
                                (np.random.rand(), np.random.rand()))
                   )


def search():
    obj = IIIM_SQL("iiimSqlTester.db")
    query = "SELECT * FROM newTable"
    data = obj.read(query)
    for value in data.fetchall():
        print(value)

    query = """
            SELECT id, value, second FROM newTable WHERE id < 10
            """
    data = obj.read(query)

    for value in data.fetchall():
        print(value)


def auto_incr_make():
    """
    A test designed for autoincrement, here we create a table and populate
    with a few valuesk
    """
    obj = IIIM_SQL("iiimSqlTester.db")
    obj.drop("autoincr")
    obj.create_table("""
                     CREATE TABLE IF NOT EXISTS autoincr(
                        id integer PRIMARY KEY AUTOINCREMENT,
                        value array
                    )""")
    obj.commit()
    for k in range(100):
        obj.insert("autoincr", (None, np.random.rand(100, 2)))


def ins_auto_inc():
    obj = IIIM_SQL("iiimSqlTester.db")
    obj.insert("autoincr", (None, np.random.rand(50, 3)))


def augment_table():
    """
    How to add column to a table
    """
    obj = IIIM_SQL("iiimSqlTester.db")
    obj.execute("""
                ALTER TABLE autoincr ADD COLUMN n INTEGER
                """)
    obj.execute("""
                ALTER TABLE autoincr ADD COLUMN m INTEGER
                """)
    obj.commit()


def add_values():
    """
    How to populate the field in the table with numbers
    """
    obj = IIIM_SQL("iiimSqlTester.db")
    # Go through the entire table
    data = obj.read("""
             SELECT id, value FROM autoincr
             """)
    data = data.fetchall()

    for id, val in data:
        n, m = val.shape
        # Here we insert back into id, n,m values
        update_n = """
        UPDATE autoincr SET n={} WHERE id={}
        """.format(n, id)
        update_m = """
        UPDATE autoincr SET m={} WHERE id={}
        """.format(m, id)
        obj.execute(update_n)
        obj.execute(update_m)


if __name__ == "__main__":
    show_db()
    create_db()
    search()
    auto_incr_make()
    ins_auto_inc()
    augment_table()
    add_values()
    print("Don't run this function as main.... :(")
