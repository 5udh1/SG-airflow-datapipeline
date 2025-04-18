import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class FactsCalculatorOperator(BaseOperator):
    """
    Custom Operator to calculate facts (max, min, avg) on a table and
    store the results in a new table in a Redshift cluster.
    """

    facts_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    SELECT
        {groupby_column},
        MAX({fact_column}) AS max_{fact_column},
        MIN({fact_column}) AS min_{fact_column},
        AVG({fact_column}) AS average_{fact_column}
    FROM {origin_table}
    GROUP BY {groupby_column};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 origin_table="",
                 destination_table="",
                 fact_column="",
                 groupby_column="",
                 *args, **kwargs):
        super(FactsCalculatorOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.origin_table = origin_table
        self.destination_table = destination_table
        self.fact_column = fact_column
        self.groupby_column = groupby_column

    def execute(self, context):
        self.log.info("Starting FactsCalculatorOperator execution")
        # Fetch Redshift connection using PostgresHook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Format the SQL template with the provided parameters
        formatted_sql = self.facts_sql_template.format(
            origin_table=self.origin_table,
            destination_table=self.destination_table,
            fact_column=self.fact_column,
            groupby_column=self.groupby_column
        )
        self.log.info(f"Executing SQL on Redshift:\n{formatted_sql}")
        redshift_hook.run(formatted_sql)
        self.log.info(f"Completed. Table '{self.destination_table}' created successfully.")