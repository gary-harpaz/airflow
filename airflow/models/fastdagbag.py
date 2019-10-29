import os

from airflow.configuration import conf
from airflow import settings, LoggingMixin
from airflow import models
from airflow.utils.db import create_session
from datetime import datetime


class FastDagBag(LoggingMixin):
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

    def get_dag(self, dag_id):
        try:
            result = self.dagbag.dags.get(dag_id)
            file_last_changed_on_disk = None
            cached_file_last_changed_on_disk = None
            if result is not None:
                file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(result.fileloc))
                cached_file_last_changed_on_disk = self.file_last_changes_dict.get(result.fileloc)
                if cached_file_last_changed_on_disk is None:
                    self.file_last_changes_dict[result.fileloc] = file_last_changed_on_disk
                    return result
                if cached_file_last_changed_on_disk == file_last_changed_on_disk:
                    return result
            from airflow.models.dag import DagModel
            orm_dag = DagModel.get_current(dag_id)
            if orm_dag is None:
                return None
            if file_last_changed_on_disk is None:
                file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(orm_dag.fileloc))
            if cached_file_last_changed_on_disk is None:
                cached_file_last_changed_on_disk = self.file_last_changes_dict.get(orm_dag.fileloc)
            if cached_file_last_changed_on_disk is None or file_last_changed_on_disk > cached_file_last_changed_on_disk:
                self.dagbag.process_file(filepath=orm_dag.fileloc, only_if_updated=False, safe_mode=self.dagbag.safe_mode)
                self.file_last_changes_dict[orm_dag.fileloc] = file_last_changed_on_disk
            result = self.dagbag.dags.get(dag_id)
        except FileNotFoundError as e:
            self.log.error("fastdagbag, while fetching " + dag_id + " encountered file not found error " + str(e))
            result = None
        return result
