from DDL_queries import *
import logging


class CreateLoad:
    """
    This class contains objects that perform drop, create, insert and copy operations on RedShift tables.

    Parameters
    -----------
    cur: obj
        cursor for redshift database connection.

    conn: obj
        connection for redshift database.

    aws_iam_role: str
        AWS iam_role

    schema: str
        schema in redshift database

    drop_tables: list
        list of tables to drop from schema in database

    create_tables: list
        list of tables to create in database

    s3_files: list
        list of s3 files in s3 bucket

    """

    def __init__(self,
                 cur,
                 conn,
                 aws_iam_role,
                 schema,
                 drop_tables=None,
                 create_tables=None,
                 s3_files=None):

        self.cur = cur
        self.conn = conn
        self.aws_iam_role = aws_iam_role
        self.schema = schema
        self.drop_tables = drop_tables
        self.create_tables = create_tables
        self.s3_files = s3_files

    def drop_dbtables(self):
        """
        Executes query to drop table in redshift given a schema and table name.
        """
        for table in self.drop_tables:
            self.cur.execute(drop_table.format(self.schema, table))
            self.conn.commit()
            logging.info(f"{table} successfully dropped.")

    def create_dbtables(self):
        """
        Executes query to create table(s) in redshift.
        """
        for query in self.create_tables:
            self.cur.execute(query)
            self.conn.commit()

    def load_table_copy(self, redshift_table):
        """
        Executes copy query to load table in redshift from s3 bucket
        """
        for file in self.s3_files:
            self.cur.execute(copy_staging_data.format(self.schema, redshift_table, file, self.aws_iam_role))
            self.conn.commit()




