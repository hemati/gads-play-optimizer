"""Run a one-off export of data."""

from airflow.dag import DagBag

if __name__ == "__main__":
    dagbag = DagBag("airflow")
    dag = dagbag.get_dag("daily_gads_play_sync")
    dag.clear()
    dag.run()
