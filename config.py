from pydantic import BaseModel, Field, model_validator
from typing import Annotated, Literal, Union, Optional


class DatabaseBase(BaseModel):
    database: str
    user: str
    password: str


class MssqlDatabaseConfig(DatabaseBase):
    type: Literal["mssql"]
    server: str
    trust_server_certificate: bool = False


class FirebirdDatabaseConfig(DatabaseBase):
    type: Literal["firebird"]
    host: str
    port: int
    charset: str = "UTF8"


DatabaseConfig = Annotated[Union[MssqlDatabaseConfig, FirebirdDatabaseConfig], Field(discriminator="type")]


class FrappeAuthConfig(BaseModel):
    api_key: str
    api_secret: str


class FrappeConfig(FrappeAuthConfig):
    limit_page_length: int = 20
    url: str  # without trailing slash


class TaskFrappeBase(BaseModel):
    modified_field: Literal["modified"] = "modified"
    id_field: Literal["name"] = "name"
    # All Date fields that will be converted to datetime after get (due to json)
    datetime_fields: list[str] = []
    # All fields that will get parsed to int (due to frappe int fields have default 0, so int are sometimes stored in 'Data' Fields (string))
    int_fields: list[str] = []

    def model_post_init(self, __context):
        if self.modified_field not in self.datetime_fields:
            self.datetime_fields.append(self.modified_field)


class TaskFrappeBidirectional(TaskFrappeBase):
    fk_id_field: str


class TaskDbBase(BaseModel):
    modified_field: str
    fallback_modified_field: Optional[str] = None


class TaskDbBidirectional(TaskDbBase):
    fk_id_field: str
    id_field: str


class TaskBase(BaseModel):
    doc_type: str
    db_name: str
    mapping: dict[str, str]
    key_fields: list[str]
    frappe: Optional[TaskFrappeBase] = None
    db: Optional[TaskDbBase] = None
    table_name: Optional[str] = None
    query: Optional[str] = None
    query_with_timestamp: Optional[str] = None
    create_new: bool = True
    use_last_sync_date: bool = True

    @model_validator(mode="after")
    def check_key_fields_in_mapping(self) -> "TaskBase":
        # Wir nutzen hier die in der TaskMapping definierten Felder
        mapping_keys = self.mapping.keys()
        missing_keys = [key for key in self.key_fields if key not in mapping_keys]
        if missing_keys:
            raise ValueError(f"Die folgenden key_fields fehlen im mapping: {missing_keys}")
        return self

    @model_validator(mode="after")
    def check_required_fields(self) -> "TaskBase":
        if self.use_last_sync_date:
            if self.frappe is None:
                raise ValueError(
                    "Die Frappe-Konfiguration 'frappe' muss angegeben werden, wenn 'use_last_sync_date' True ist."
                )
            if self.db is None:
                raise ValueError("Die DB-Konfiguration 'db' muss angegeben werden, wenn 'use_last_sync_date' True ist.")
            if self.query is not None and self.query_with_timestamp is None:
                raise ValueError(
                    "'query_with_timestamp' muss angegeben werden, wenn 'use_last_sync_date' True ist und 'query' genutzt wird."
                )
        return self


class BidirectionalTaskConfig(TaskBase):
    direction: Literal["bidirectional"]
    table_name: str
    frappe: TaskFrappeBidirectional
    db: TaskDbBidirectional
    delete: bool = True
    datetime_comparison_accuracy_milliseconds: int = 100


class DbToFrappeTaskConfig(TaskBase):
    direction: Literal["db_to_frappe"]
    process_all: bool = True

    @model_validator(mode="after")
    def validate_table_or_query(self) -> "DbToFrappeTaskConfig":
        if self.table_name is None and self.query is None:
            raise ValueError("Entweder 'table_name' oder 'query' muss angegeben werden.")
        return self


class FrappeToDbTaskConfig(TaskBase):
    direction: Literal["frappe_to_db"]
    table_name: str


TaskConfig = Annotated[
    Union[BidirectionalTaskConfig, DbToFrappeTaskConfig, FrappeToDbTaskConfig], Field(discriminator="direction")
]


class Config(BaseModel):
    databases: dict[str, DatabaseConfig]
    frappe: FrappeConfig
    tasks: dict[str, TaskConfig]
    dry_run: bool = False
    timestamp_file: str = "timestamps.yaml"
    timestamp_buffer_seconds: int = 15
