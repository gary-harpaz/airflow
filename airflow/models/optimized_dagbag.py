import os

from airflow.configuration import conf
from airflow import settings, LoggingMixin
from airflow import models
from airflow.utils.db import create_session
from datetime import datetime


class OptimizedDagBag(LoggingMixin):
    def __init__(
            self,
            dag_folder=None,
            executor=None,
            include_examples=conf.getboolean('core', 'LOAD_EXAMPLES'),
            safe_mode=conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE')):
        self.dagbag = models.DagBag(dag_folder=dag_folder, collect_dags_on_init=False, executor=executor,
                                    include_examples=include_examples, safe_mode=safe_mode)
        self.file_last_changes_dict = {}
        self.file_last_changes_dict.setdefault(None)

    def init_dag_file_by_id(self):
        with create_session() as session:
            DM = models.DagModel
            orm_dags = session.query(DM).with_entities(DM.dag_id, DM.fileloc).all()
            for orm_dag in orm_dags:
                self.dag_file_by_id[orm_dag.dag_id] = orm_dag.fileloc

    def _get_cached_dag(self, dag_id):
        dag = self.dagbag.dags.get(dag_id)
        # check if cached dag should be refreshed using file last modified
        file_last_changed_on_disk = None
        if dag is not None:
            file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(dag.fileloc))
            cached_file_last_changed_on_disk = self.file_last_changes_dict.get(dag.fileloc)
            if cached_file_last_changed_on_disk is None:
                self.file_last_changes_dict[dag.fileloc] = file_last_changed_on_disk
                return dag, file_last_changed_on_disk
            if cached_file_last_changed_on_disk == file_last_changed_on_disk:
                return dag, file_last_changed_on_disk
            # file has changed cached version is invalid
            dag = None
        return dag, file_last_changed_on_disk

    def _process_dag_file(self,dag_id, file_last_changed_on_disk):
        from airflow.models.dag import DagModel
        orm_dag = DagModel.get_current(dag_id)
        if orm_dag is None:
            return None
        if file_last_changed_on_disk is None:
            file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(orm_dag.fileloc))
        cached_file_last_changed_on_disk = self.file_last_changes_dict.get(orm_dag.fileloc)
        if cached_file_last_changed_on_disk is None or file_last_changed_on_disk > cached_file_last_changed_on_disk:
            self.dagbag.process_file(filepath=orm_dag.fileloc, only_if_updated=False, safe_mode=self.dagbag.safe_mode)
            self.file_last_changes_dict[orm_dag.fileloc] = file_last_changed_on_disk
        dag = self.dagbag.dags.get(dag_id)
        return dag

    def get_dag(self, dag_id):
        try:
            dag, file_last_changed_on_disk = self._get_cached_dag(dag_id)
            if dag is not None:
                return dag
            dag = self._process_dag_file(dag_id, file_last_changed_on_disk)
        except FileNotFoundError as e:
            self.log.error("fastdagbag, while fetching " + dag_id + " encountered file not found error " + str(e))
        return dag
