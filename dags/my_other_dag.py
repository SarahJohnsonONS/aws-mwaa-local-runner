import textwrap
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Instantiate the DAG
with DAG(
    "my_other_dag",
    # Default args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["example@airflow.org"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="DAG without decorator",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 30),
    catchup=False,
    tags=["example"],
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(task_id="print_date", bash_command="date")
    t2 = BashOperator(
        task_id="sleep", depends_on_past=False, bash_command="sleep 5", retries=3
    )
    t1.doc_md = textwrap.dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
    """
    )

    dag.doc_md = (
        __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    )
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this

    # Jinja templating
    templated_command = textwrap.dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )
    # Can also pass files to bash_command argument e.g. templated_command.sh
    t3 = BashOperator(
        task_id="templated", depends_on_past=False, bash_command=templated_command
    )

    # # Different ways of defining dependencies
    # # This means that t2 will depend on t1 running successfully to run.
    # t1.set_downstream(t2)
    # # Equivalent to:
    # t2.set_upstream(t1)

    # # The bit shift operator can also be used to chain operations:
    # # Downstream:
    # t1 >> t2
    # # Upstream:
    # t2 << t1

    # # Chaining multiple dependencies:
    # t1 >> t2 >> t3

    # # A list of tasks can also be set as dependencies.
    # # All equivalent:
    # t1.set_downstream([t2, t3])
    # [t2, t3] << t1
    t1 >> [t2, t3]
